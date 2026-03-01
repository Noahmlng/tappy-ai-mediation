#!/usr/bin/env node
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { parseArgs, printJson, withDbPool } from './common.js'
import {
  cleanText,
  splitCsv,
  timestampTag,
  toPositiveInteger,
  writeJson,
} from './audit-common.js'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const PROJECT_ROOT = path.resolve(__dirname, '..', '..')
const OUTPUT_ROOT = path.join(PROJECT_ROOT, 'output', 'inventory-audit')

const DEFAULT_LIMIT = 8000
const DEFAULT_TIMEOUT_MS = 10000
const DEFAULT_CONCURRENCY = 20
const IMAGE_EXT_RE = /\.(png|jpe?g|webp|gif|svg|avif|bmp|ico)(\?|#|$)/i

function looksLikeImageUrl(url = '') {
  const text = cleanText(url)
  if (!text) return false
  return /^https?:\/\//i.test(text) && IMAGE_EXT_RE.test(text)
}

function categoryKeyFromRow(row = {}) {
  const metadata = row?.metadata && typeof row.metadata === 'object' ? row.metadata : {}
  const l1 = cleanText(metadata.vertical_l1 || metadata.verticalL1)
  const l2 = cleanText(metadata.vertical_l2 || metadata.verticalL2 || metadata.category)
  if (l1 && l2) return `${l1}::${l2}`
  if (l2) return l2
  if (l1) return l1
  return '(unknown)'
}

function brandKeyFromRow(row = {}) {
  const metadata = row?.metadata && typeof row.metadata === 'object' ? row.metadata : {}
  return cleanText(
    metadata.brand_id
    || metadata.brandId
    || metadata.merchant
    || metadata.merchantName
    || row.offer_id,
  )
}

function isPriceMissing(row = {}) {
  const metadata = row?.metadata && typeof row.metadata === 'object' ? row.metadata : {}
  const explicit = metadata.price_missing
  if (typeof explicit === 'boolean') return explicit
  const price = Number(metadata.price)
  return !(Number.isFinite(price) && price > 0)
}

function normalizeContentType(value = '') {
  return cleanText(value).toLowerCase().split(';')[0]
}

function isImageContentType(value = '') {
  return normalizeContentType(value).startsWith('image/')
}

function classifyFetchError(error) {
  const message = cleanText(error?.message || String(error || '')).toLowerCase()
  const name = cleanText(error?.name || '').toLowerCase()
  if (name.includes('abort') || message.includes('abort') || message.includes('timeout')) return 'timeout'
  if (
    message.includes('enotfound')
    || message.includes('getaddrinfo')
    || message.includes('dns')
    || message.includes('eai_again')
  ) return 'dns'
  return 'request_error'
}

async function probeImageUrl(url = '', timeoutMs = DEFAULT_TIMEOUT_MS) {
  const target = cleanText(url)
  if (!target) return { ok: false, status_class: 'missing_image_url' }
  try {
    const run = async (method) => {
      const response = await fetch(target, {
        method,
        redirect: 'follow',
        signal: AbortSignal.timeout(Math.max(1000, timeoutMs)),
        headers: { 'user-agent': 'inventory-coverage-categories/1.0' },
      })
      const contentType = normalizeContentType(response.headers.get('content-type') || '')
      return {
        status: Number(response.status) || 0,
        valid:
          response.status >= 200
          && response.status < 400
          && (isImageContentType(contentType) || looksLikeImageUrl(response.url || target)),
      }
    }
    let result = await run('HEAD')
    if ([403, 405].includes(result.status)) result = await run('GET')
    return {
      ok: result.valid === true,
      status_class:
        result.status >= 200 && result.status < 300
          ? '2xx'
          : result.status >= 300 && result.status < 400
            ? '3xx'
            : result.status >= 400 && result.status < 500
              ? '4xx'
              : result.status >= 500 && result.status < 600
                ? '5xx'
                : 'other_http',
    }
  } catch (error) {
    return { ok: false, status_class: classifyFetchError(error) }
  }
}

function createLimiter(maxConcurrency = DEFAULT_CONCURRENCY) {
  const concurrency = Math.max(1, toPositiveInteger(maxConcurrency, DEFAULT_CONCURRENCY))
  let active = 0
  const queue = []

  async function runNext() {
    if (active >= concurrency) return
    const task = queue.shift()
    if (!task) return
    active += 1
    try {
      const value = await task.fn()
      task.resolve(value)
    } catch (error) {
      task.reject(error)
    } finally {
      active -= 1
      void runNext()
    }
  }

  return async function limit(fn) {
    return await new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject })
      void runNext()
    })
  }
}

async function queryRows(args = {}) {
  const limit = toPositiveInteger(args.limit, DEFAULT_LIMIT)
  const networks = splitCsv(args.networks || 'house').map((item) => cleanText(item).toLowerCase()).filter(Boolean)
  const categories = splitCsv(args.categories).map((item) => cleanText(item).toLowerCase()).filter(Boolean)

  return await withDbPool(async (pool) => {
    const result = await pool.query(
      `
      SELECT
        offer_id,
        network,
        title,
        target_url,
        image_url,
        metadata
      FROM offer_inventory_norm
      WHERE availability = 'active'
        AND ($1::text[] IS NULL OR lower(network) = ANY($1::text[]))
      ORDER BY updated_at DESC
      LIMIT $2
      `,
      [networks.length > 0 ? networks : null, limit],
    )
    const rows = Array.isArray(result.rows) ? result.rows : []
    return rows.filter((row) => {
      if (categories.length <= 0) return true
      return categories.includes(categoryKeyFromRow(row).toLowerCase())
    })
  })
}

async function computeCategoryCoverage(rows = [], options = {}) {
  const probeImages = options.probeImages === true
  const imageTimeoutMs = toPositiveInteger(options.imageTimeoutMs, DEFAULT_TIMEOUT_MS)
  const limiter = createLimiter(options.concurrency)
  const imageCache = new Map()

  const buckets = new Map()
  const tasks = rows.map((row) => limiter(async () => {
    const category = categoryKeyFromRow(row)
    const brand = brandKeyFromRow(row)
    const imageUrl = cleanText(row.image_url)
    let imageValid = looksLikeImageUrl(imageUrl)
    if (probeImages && imageUrl) {
      if (!imageCache.has(imageUrl)) {
        imageCache.set(imageUrl, await probeImageUrl(imageUrl, imageTimeoutMs))
      }
      imageValid = imageCache.get(imageUrl)?.ok === true
    }
    const priceMissing = isPriceMissing(row)

    if (!buckets.has(category)) {
      buckets.set(category, {
        category_key: category,
        offer_count: 0,
        brand_set: new Set(),
        price_missing_count: 0,
        image_valid_count: 0,
      })
    }
    const bucket = buckets.get(category)
    bucket.offer_count += 1
    if (brand) bucket.brand_set.add(brand)
    if (priceMissing) bucket.price_missing_count += 1
    if (imageValid) bucket.image_valid_count += 1
  }))

  await Promise.all(tasks)

  return [...buckets.values()]
    .map((row) => ({
      category_key: row.category_key,
      brand_count: row.brand_set.size,
      product_offer_count: row.offer_count,
      price_missing_count: row.price_missing_count,
      price_missing_ratio: Number((row.price_missing_count / Math.max(1, row.offer_count)).toFixed(4)),
      image_valid_count: row.image_valid_count,
      image_valid_ratio: Number((row.image_valid_count / Math.max(1, row.offer_count)).toFixed(4)),
    }))
    .sort((a, b) => b.product_offer_count - a.product_offer_count || a.category_key.localeCompare(b.category_key))
}

export async function runCoverageCategories(rawArgs = {}) {
  const args = rawArgs && typeof rawArgs === 'object' ? rawArgs : {}
  const probeImages = String(args['probe-images'] || '').toLowerCase() === 'true'
  const imageTimeoutMs = toPositiveInteger(args['image-timeout-ms'], DEFAULT_TIMEOUT_MS)
  const concurrency = toPositiveInteger(args.concurrency, DEFAULT_CONCURRENCY)
  const minBrands = toPositiveInteger(args['min-brands'], 20)
  const minOffers = toPositiveInteger(args['min-offers'], 60)
  const maxPriceMissingRatio = Number(args['max-price-missing-ratio'] || 1)
  const requiredImageValidRatio = Number(args['required-image-valid-ratio'] || 1)
  const tag = timestampTag()
  const outputFile = path.resolve(
    PROJECT_ROOT,
    cleanText(args['output-file']) || path.join(OUTPUT_ROOT, `coverage-categories-${tag}.json`),
  )

  const rows = await queryRows(args)
  const byCategory = await computeCategoryCoverage(rows, {
    probeImages,
    imageTimeoutMs,
    concurrency,
  })
  const failed = byCategory.filter((row) => (
    row.brand_count < minBrands
    || row.product_offer_count < minOffers
    || row.price_missing_ratio > maxPriceMissingRatio
    || row.image_valid_ratio < requiredImageValidRatio
  ))

  const report = {
    generated_at: new Date().toISOString(),
    inputs: {
      rows: rows.length,
      networks: splitCsv(args.networks || 'house'),
      categories: splitCsv(args.categories),
      probe_images: probeImages,
      image_timeout_ms: imageTimeoutMs,
      concurrency,
      acceptance: {
        min_brands: minBrands,
        min_offers: minOffers,
        max_price_missing_ratio: maxPriceMissingRatio,
        required_image_valid_ratio: requiredImageValidRatio,
      },
    },
    summary: {
      total_categories: byCategory.length,
      failed_categories: failed.length,
      all_passed: failed.length === 0,
    },
    by_category: byCategory,
    failed_categories: failed,
    output_file: path.relative(PROJECT_ROOT, outputFile),
  }

  await writeJson(outputFile, report)
  await writeJson(path.join(OUTPUT_ROOT, 'latest-coverage-categories.json'), {
    generated_at: new Date().toISOString(),
    report_json: path.relative(PROJECT_ROOT, outputFile),
  })
  return report
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const report = await runCoverageCategories(args)
  printJson({
    ok: true,
    totalCategories: report.summary.total_categories,
    failedCategories: report.summary.failed_categories,
    allPassed: report.summary.all_passed,
    outputFile: report.output_file,
  })
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error('[inventory-coverage-categories] failed:', error?.message || error)
    process.exit(1)
  })
}

export const __coverageCategoriesInternal = Object.freeze({
  categoryKeyFromRow,
  brandKeyFromRow,
  isPriceMissing,
  looksLikeImageUrl,
})
