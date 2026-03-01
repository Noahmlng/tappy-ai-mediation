#!/usr/bin/env node
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  CURATED_ROOT,
  parseArgs,
  toInteger,
  timestampTag,
  readJsonl,
  writeJson,
  writeJsonl,
  ensureDir,
  cleanText,
  hashId,
  normalizeUrl,
} from './lib/common.js'
import { VERTICAL_TAXONOMY } from './lib/vertical-taxonomy.js'

const OFFERS_ROOT = path.resolve(process.cwd(), 'data/house-ads/offers')
const OFFER_JOBS_DIR = path.join(OFFERS_ROOT, 'raw', 'offer-jobs')
const DEFAULT_BRANDS_FILE = path.join(CURATED_ROOT, 'brands.jsonl')
const DEFAULT_LOCALE = 'en-US'
const __filename = fileURLToPath(import.meta.url)

function slugify(value) {
  return cleanText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+/, '')
    .replace(/-+$/, '')
}

function dedupe(values = []) {
  const seen = new Set()
  const output = []
  for (const value of values) {
    const text = cleanText(value)
    if (!text || seen.has(text.toLowerCase())) continue
    seen.add(text.toLowerCase())
    output.push(text)
  }
  return output
}

function categoryKey(row) {
  return `${cleanText(row.vertical_l1) || 'unknown'}::${cleanText(row.vertical_l2) || 'unknown'}`
}

function taxonomyKeywordMap() {
  const map = new Map()
  for (const item of VERTICAL_TAXONOMY) {
    map.set(`${item.vertical_l1}::${item.vertical_l2}`, item.keywords || [])
  }
  return map
}

function inferPriceBand(verticalL1 = '', verticalL2 = '') {
  const key = `${verticalL1}::${verticalL2}`.toLowerCase()
  if (key.includes('smartphone') || key.includes('laptop')) return { min: 299, max: 2499, currency: 'USD' }
  if (key.includes('footwear') || key.includes('apparel')) return { min: 29, max: 299, currency: 'USD' }
  if (key.includes('cosmetics') || key.includes('skincare')) return { min: 12, max: 189, currency: 'USD' }
  if (key.includes('pet_supplies')) return { min: 15, max: 259, currency: 'USD' }
  if (key.includes('kitchen_appliance')) return { min: 39, max: 899, currency: 'USD' }
  return { min: 19, max: 399, currency: 'USD' }
}

function buildCrawlTargets(domain, keywords = []) {
  const host = cleanText(domain).toLowerCase()
  if (!host) return []
  const base = `https://${host}`
  const keywordPaths = keywords
    .slice(0, 2)
    .map((keyword) => slugify(keyword))
    .filter(Boolean)
    .map((keyword) => `/collections/${keyword}`)
  return dedupe([
    `${base}/`,
    `${base}/shop`,
    `${base}/products`,
    `${base}/collections`,
    `${base}/offers`,
    `${base}/deals`,
    `${base}/sale`,
    `${base}/promotions`,
    ...keywordPaths.map((item) => `${base}${item}`),
  ])
}

function resolveCanonicalLandingUrl(brand = {}, domain = '') {
  const evidence = brand?.evidence && typeof brand.evidence === 'object' ? brand.evidence : {}
  const candidates = [
    cleanText(evidence.homepage_url),
    cleanText(evidence.redirect_final_url),
    cleanText(brand.homepage_url),
    cleanText(brand.canonical_url),
    domain ? `https://${domain}` : '',
  ]

  for (const candidate of candidates) {
    const normalized = normalizeUrl(candidate)
    if (!normalized) continue
    try {
      const parsed = new URL(normalized)
      if (!['http:', 'https:'].includes(parsed.protocol)) continue
      parsed.pathname = '/'
      parsed.search = ''
      parsed.hash = ''
      return parsed.toString()
    } catch {
      // ignore invalid candidate
    }
  }

  return domain ? `https://${domain}/` : ''
}

function dedupeBrands(brands = []) {
  const byBrandId = new Map()
  for (const row of brands) {
    const brandId = cleanText(row.brand_id)
    if (!brandId) continue
    const existing = byBrandId.get(brandId)
    if (!existing) {
      byBrandId.set(brandId, row)
      continue
    }
    if (Number(row.source_confidence || 0) > Number(existing.source_confidence || 0)) {
      byBrandId.set(brandId, row)
    }
  }
  return [...byBrandId.values()]
}

function buildBuckets(brands = []) {
  const map = new Map()
  for (const row of brands) {
    const key = categoryKey(row)
    if (!map.has(key)) map.set(key, [])
    map.get(key).push(row)
  }
  const buckets = [...map.entries()]
    .map(([key, rows]) => ({
      key,
      rows: rows.sort((a, b) => {
        const aScore = Number(a.source_confidence || 0)
        const bScore = Number(b.source_confidence || 0)
        if (aScore !== bScore) return bScore - aScore
        return cleanText(a.brand_id).localeCompare(cleanText(b.brand_id))
      }),
      idx: 0,
    }))
    .sort((a, b) => a.key.localeCompare(b.key))
  return buckets
}

function roundRobinBalanced(buckets = [], limit = Number.MAX_SAFE_INTEGER) {
  const ordered = []
  let remaining = buckets.reduce((sum, bucket) => sum + bucket.rows.length, 0)
  let round = 0
  while (remaining > 0 && ordered.length < limit) {
    round += 1
    let pickedInRound = 0
    for (const bucket of buckets) {
      if (ordered.length >= limit) break
      if (bucket.idx >= bucket.rows.length) continue
      const brand = bucket.rows[bucket.idx]
      bucket.idx += 1
      remaining -= 1
      pickedInRound += 1
      ordered.push({
        brand,
        category_key: bucket.key,
        category_slot: bucket.idx,
        round,
      })
    }
    if (pickedInRound === 0) break
  }
  return ordered
}

function buildJobs(orderedRows, keywordByCategory) {
  return orderedRows.map((item, index) => {
    const brand = item.brand
    const verticalL1 = cleanText(brand.vertical_l1) || 'unknown'
    const verticalL2 = cleanText(brand.vertical_l2) || 'unknown'
    const domain = cleanText(brand.official_domain).toLowerCase()
    const market = cleanText(brand.market) || 'US'
    const keywords = keywordByCategory.get(item.category_key) || []
    const priceBand = inferPriceBand(verticalL1, verticalL2)
    const crawlTargets = buildCrawlTargets(domain, keywords)
    const canonicalLandingUrl = resolveCanonicalLandingUrl(brand, domain)
    const sourceConfidence = Number(brand.source_confidence || 0)
    const priorityTier = sourceConfidence >= 0.85 ? 'high' : sourceConfidence >= 0.72 ? 'medium' : 'normal'
    const jobId = `offer_job_${hashId(`${brand.brand_id}|${domain}|${index + 1}`, 12)}`

    return {
      job_id: jobId,
      job_type: 'offer_discovery',
      status: 'pending',
      queue_index: index + 1,
      round: item.round,
      category_key: item.category_key,
      category_slot: item.category_slot,
      priority_tier: priorityTier,
      brand_id: cleanText(brand.brand_id),
      brand_name: cleanText(brand.brand_name),
      official_domain: domain,
      canonical_landing_url: canonicalLandingUrl,
      vertical_l1: verticalL1,
      vertical_l2: verticalL2,
      market,
      locale_hint: DEFAULT_LOCALE,
      source_confidence: Number(sourceConfidence.toFixed(4)),
      crawl_targets: crawlTargets,
      stages: [
        {
          stage: 'crawl_real_offers',
          required: true,
          max_pages: 8,
        },
        {
          stage: 'synthesize_offer_fallback',
          required: true,
          min_link_offers: 2,
          min_product_offers: 6,
        },
      ],
      synthetic_hints: {
        keyword_seed: keywords,
        price_band: priceBand,
      },
      created_at: new Date().toISOString(),
    }
  })
}

function categoryDistribution(rows = []) {
  const dist = new Map()
  for (const row of rows) {
    const key = row.category_key
    dist.set(key, (dist.get(key) || 0) + 1)
  }
  return Object.fromEntries([...dist.entries()].sort((a, b) => b[1] - a[1]))
}

function prefixBalance(rows = [], categoryCount = 1) {
  const prefixSizes = [categoryCount, categoryCount * 3, categoryCount * 5]
  const out = []
  for (const size of prefixSizes) {
    const actualSize = Math.min(size, rows.length)
    const set = new Set(rows.slice(0, actualSize).map((row) => row.category_key))
    out.push({
      prefix_size: actualSize,
      unique_categories: set.size,
      expected_max_categories: categoryCount,
      coverage_ratio: Number((set.size / Math.max(1, categoryCount)).toFixed(4)),
    })
  }
  return out
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const tag = timestampTag()
  const inputFile = path.resolve(process.cwd(), cleanText(args['brands-file']) || DEFAULT_BRANDS_FILE)
  const maxJobs = toInteger(args['max-jobs'], 0)

  const brands = await readJsonl(inputFile)
  const dedupedBrands = dedupeBrands(brands)
  const buckets = buildBuckets(dedupedBrands)
  const keywordByCategory = taxonomyKeywordMap()
  const ordered = roundRobinBalanced(
    buckets,
    maxJobs > 0 ? Math.max(1, maxJobs) : Number.MAX_SAFE_INTEGER,
  )
  const jobs = buildJobs(ordered, keywordByCategory)

  await ensureDir(OFFER_JOBS_DIR)
  const jsonlPath = path.join(OFFER_JOBS_DIR, `offer-jobs-${tag}.jsonl`)
  const summaryPath = path.join(OFFER_JOBS_DIR, `offer-jobs-${tag}.summary.json`)
  const latestPath = path.join(OFFER_JOBS_DIR, 'latest-offer-jobs.json')

  const categories = [...new Set(jobs.map((job) => job.category_key))]
  const categoryDist = categoryDistribution(jobs)
  const categoryCounts = Object.values(categoryDist)
  const summary = {
    generatedAt: new Date().toISOString(),
    input: path.relative(process.cwd(), inputFile),
    inputBrands: brands.length,
    dedupedBrands: dedupedBrands.length,
    outputJobs: jobs.length,
    categoryCount: categories.length,
    categoryMinJobs: categoryCounts.length ? Math.min(...categoryCounts) : 0,
    categoryMaxJobs: categoryCounts.length ? Math.max(...categoryCounts) : 0,
    prefixBalance: prefixBalance(jobs, categories.length),
    categoryDistribution: categoryDist,
    output: path.relative(process.cwd(), jsonlPath),
  }

  await writeJsonl(jsonlPath, jobs)
  await writeJson(summaryPath, summary)
  await writeJson(latestPath, {
    generatedAt: new Date().toISOString(),
    latestJsonl: path.relative(process.cwd(), jsonlPath),
    latestSummary: path.relative(process.cwd(), summaryPath),
  })

  console.log(
    JSON.stringify(
      {
        ok: true,
        inputBrands: brands.length,
        dedupedBrands: dedupedBrands.length,
        outputJobs: jobs.length,
        categories: categories.length,
        jobsFile: path.relative(process.cwd(), jsonlPath),
        summaryFile: path.relative(process.cwd(), summaryPath),
      },
      null,
      2,
    ),
  )
}

if (path.resolve(process.argv[1] || '') === __filename) {
  main().catch((error) => {
    console.error('[build-offer-jobs] failed:', error?.message || error)
    process.exit(1)
  })
}

export const __buildOfferJobsInternal = Object.freeze({
  resolveCanonicalLandingUrl,
})
