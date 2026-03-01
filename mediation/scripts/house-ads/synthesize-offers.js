#!/usr/bin/env node
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  parseArgs,
  toInteger,
  readJson,
  readJsonl,
  writeJson,
  writeJsonl,
  ensureDir,
  cleanText,
  hashId,
  timestampTag,
} from './lib/common.js'
import { sourcePolicyForCategory } from './lib/category-policy.js'

const OFFERS_ROOT = path.resolve(process.cwd(), 'data/house-ads/offers')
const OFFER_JOBS_DIR = path.join(OFFERS_ROOT, 'raw', 'offer-jobs')
const OFFERS_CURATED_DIR = path.join(OFFERS_ROOT, 'curated')
const __filename = fileURLToPath(import.meta.url)

const VALID_AVAILABILITY = ['in_stock', 'limited', 'preorder', 'unknown']
const TITLE_SUFFIX = ['Essential Pick', 'Top Rated', 'New Arrival', 'Limited Deal', 'Best Seller', 'Popular Choice']

function toNumber(value, fallback = 0) {
  const num = Number(value)
  return Number.isFinite(num) ? num : fallback
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function slugify(value = '') {
  return cleanText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+/, '')
    .replace(/-+$/, '')
}

function sampleByHash(seed, choices = []) {
  if (!choices.length) return ''
  const token = hashId(seed, 8)
  const n = parseInt(token, 16)
  const idx = Number.isFinite(n) ? n % choices.length : 0
  return choices[idx]
}

function rand01(seed) {
  const token = hashId(seed, 8)
  const n = parseInt(token, 16)
  if (!Number.isFinite(n)) return 0.5
  return n / 0xffffffff
}

function pickLatestMeta(baseDir, name) {
  return path.join(baseDir, name)
}

async function loadJobs(args) {
  const explicit = cleanText(args['jobs-file'])
  if (explicit) return readJsonl(path.resolve(process.cwd(), explicit))
  const latestMeta = await readJson(pickLatestMeta(OFFER_JOBS_DIR, 'latest-offer-jobs.json'), null)
  if (!latestMeta?.latestJsonl) throw new Error('No latest offer jobs metadata found. Run build-offer-jobs first.')
  return readJsonl(path.resolve(process.cwd(), latestMeta.latestJsonl))
}

async function loadRealOffers(args) {
  const explicit = cleanText(args['real-offers-file'])
  if (explicit) return readJsonl(path.resolve(process.cwd(), explicit))
  const latestMeta = await readJson(pickLatestMeta(OFFERS_CURATED_DIR, 'latest-offers-real.json'), null)
  if (!latestMeta?.latestJsonl) return []
  return readJsonl(path.resolve(process.cwd(), latestMeta.latestJsonl))
}

function inferFallbackPriceBand(job) {
  const l2 = cleanText(job.vertical_l2).toLowerCase()
  if (l2.includes('smartphone')) return { min: 259, max: 1899, currency: 'USD' }
  if (l2.includes('laptop')) return { min: 399, max: 2999, currency: 'USD' }
  if (l2.includes('footwear')) return { min: 39, max: 299, currency: 'USD' }
  if (l2.includes('apparel')) return { min: 29, max: 249, currency: 'USD' }
  if (l2.includes('cosmetics') || l2.includes('skincare')) return { min: 12, max: 189, currency: 'USD' }
  if (l2.includes('pet_supplies')) return { min: 15, max: 259, currency: 'USD' }
  if (l2.includes('kitchen_appliance')) return { min: 49, max: 899, currency: 'USD' }
  if (l2.includes('hotel_booking') || l2.includes('travel')) return { min: 59, max: 699, currency: 'USD' }
  return { min: 19, max: 399, currency: 'USD' }
}

function normalizePriceBand(job) {
  const raw = job?.synthetic_hints?.price_band || {}
  const fallback = inferFallbackPriceBand(job)
  const min = clamp(toNumber(raw.min, fallback.min), 1, 100000)
  const maxCandidate = toNumber(raw.max, fallback.max)
  const max = clamp(maxCandidate >= min ? maxCandidate : min + 10, min + 1, 200000)
  const currencyRaw = cleanText(raw.currency || fallback.currency).toUpperCase()
  const currency = /^[A-Z]{3}$/.test(currencyRaw) ? currencyRaw : 'USD'
  return { min, max, currency }
}

function targetProductCount(job, existingProductCount, options) {
  const stage = (Array.isArray(job.stages) ? job.stages : []).find((item) => item?.stage === 'synthesize_offer_fallback')
  const stageMin = toInteger(stage?.min_product_offers, 0)
  const desired = Math.max(1, options.minProductOffers, stageMin)

  if (options.fillMode === 'all') return desired
  if (options.fillMode === 'topup') return Math.max(0, desired - existingProductCount)
  // fillMode === 'missing' (default)
  return existingProductCount > 0 ? 0 : desired
}

function campaignId(job) {
  const key = `${job.brand_id}|${job.vertical_l2}|product`
  return `campaign_${slugify(job.vertical_l2 || 'product')}_${hashId(key, 10)}`
}

function buildTags(job, keyword) {
  const raw = [
    cleanText(job.vertical_l1),
    cleanText(job.vertical_l2),
    cleanText(keyword),
    'product',
    'recommendation',
    'synthetic',
  ]
  const out = []
  const seen = new Set()
  for (const item of raw) {
    const text = cleanText(item).toLowerCase()
    if (!text) continue
    if (seen.has(text)) continue
    seen.add(text)
    out.push(text.slice(0, 40))
  }
  return out.length > 0 ? out : ['product', 'synthetic']
}

function buildProductTitle(brandName, keyword, seed) {
  const suffix = sampleByHash(`suffix|${seed}`, TITLE_SUFFIX) || 'Featured'
  const keywordPart = cleanText(keyword) || 'Product'
  return cleanText(`${brandName} ${keywordPart} ${suffix}`).slice(0, 120)
}

function buildDescription(brandName, keyword) {
  return cleanText(`High-intent ${keyword || 'product'} recommendation from ${brandName}, ready for sponsored placement.`).slice(0, 240)
}

function buildSnippet(brandName, keyword) {
  return cleanText(`${brandName} ${keyword || 'product'} option with strong category relevance and direct shopping intent.`).slice(0, 240)
}

function normalizeHttpUrl(value = '') {
  const raw = cleanText(value)
  if (!raw) return ''
  try {
    const parsed = new URL(raw)
    if (!['http:', 'https:'].includes(parsed.protocol)) return ''
    parsed.search = ''
    parsed.hash = ''
    return parsed.toString()
  } catch {
    return ''
  }
}

function resolveSyntheticTargetUrl(job = {}) {
  const canonical = normalizeHttpUrl(cleanText(job.canonical_landing_url))
  if (canonical) return canonical
  const domain = cleanText(job.official_domain).toLowerCase()
  return domain ? `https://${domain}/` : ''
}

function buildSyntheticImageUrl(job = {}, index = 0) {
  const domain = cleanText(job.official_domain).toLowerCase()
  const brand = cleanText(job.brand_id).toLowerCase()
  const seedBase = cleanText(domain || brand || `brand-${index + 1}`)
  const seed = encodeURIComponent(`${seedBase}-${index + 1}`)
  return `https://picsum.photos/seed/${seed}/640/360`
}

function buildSyntheticOffer(job, index, priceBand) {
  const brandId = cleanText(job.brand_id)
  const brandName = cleanText(job.brand_name) || brandId
  const market = cleanText(job.market) || 'US'
  const keywords = Array.isArray(job?.synthetic_hints?.keyword_seed) ? job.synthetic_hints.keyword_seed : []
  const keyword = cleanText(keywords[index % Math.max(1, keywords.length)] || job.vertical_l2 || 'product')
  const seed = `${job.job_id}|${brandId}|${index}`

  const p = rand01(`price|${seed}`)
  const priceRaw = priceBand.min + (priceBand.max - priceBand.min) * p
  const price = Number(priceRaw.toFixed(2))
  const discountBase = 0.08 + rand01(`discount|${seed}`) * 0.32
  const originalPrice = Number((price / (1 - discountBase)).toFixed(2))
  const discountPct = Number((((originalPrice - price) / originalPrice) * 100).toFixed(2))

  const availability = sampleByHash(`availability|${seed}`, VALID_AVAILABILITY) || 'in_stock'
  const targetUrl = resolveSyntheticTargetUrl(job)
  const imageUrl = buildSyntheticImageUrl(job, index)
  const cmpId = campaignId(job)
  const title = buildProductTitle(brandName, keyword, seed)
  const offerKey = `${cmpId}|${targetUrl}|${title}|${index}`

  return {
    offer_id: `offer_${hashId(offerKey, 12)}`,
    campaign_id: cmpId,
    brand_id: brandId,
    offer_type: 'product',
    vertical_l1: cleanText(job.vertical_l1),
    vertical_l2: cleanText(job.vertical_l2),
    market,
    title,
    snippet: buildSnippet(brandName, keyword),
    description: buildDescription(brandName, keyword),
    target_url: targetUrl,
    image_url: imageUrl,
    status: 'active',
    language: 'en',
    disclosure: 'Sponsored',
    source_type: 'synthetic',
    confidence_score: Number((0.62 + rand01(`conf|${seed}`) * 0.18).toFixed(4)),
    freshness_ttl_hours: 72,
    last_verified_at: new Date().toISOString(),
    product_id: `syn_prd_${hashId(`${brandId}|${targetUrl}|${index}`, 10)}`,
    merchant: brandName,
    price,
    original_price: originalPrice,
    currency: priceBand.currency,
    discount_pct: discountPct,
    availability,
    tags: buildTags(job, keyword),
  }
}

function validateProductOffer(offer) {
  const errors = []
  const required = [
    'offer_id',
    'campaign_id',
    'brand_id',
    'offer_type',
    'vertical_l1',
    'vertical_l2',
    'market',
    'title',
    'snippet',
    'target_url',
    'status',
    'language',
    'disclosure',
    'source_type',
    'confidence_score',
    'freshness_ttl_hours',
    'last_verified_at',
    'product_id',
    'merchant',
    'price',
    'currency',
    'availability',
    'tags',
  ]
  for (const field of required) {
    if (!(field in offer)) errors.push(`missing_${field}`)
    else if (typeof offer[field] === 'string' && !cleanText(offer[field])) errors.push(`empty_${field}`)
  }
  if (offer.offer_type !== 'product') errors.push('invalid_offer_type')
  if (!['active', 'paused', 'archived'].includes(offer.status)) errors.push('invalid_status')
  if (offer.source_type !== 'synthetic') errors.push('invalid_source_type')
  if (!/^https?:\/\//i.test(String(offer.target_url || ''))) errors.push('invalid_target_url')
  if (!/^https?:\/\//i.test(String(offer.image_url || ''))) errors.push('invalid_image_url')
  if (!(typeof offer.price === 'number' && offer.price > 0)) errors.push('invalid_price')
  if (!(typeof offer.original_price === 'number' && offer.original_price >= offer.price)) errors.push('invalid_original_price')
  if (!/^[A-Z]{3}$/.test(String(offer.currency || ''))) errors.push('invalid_currency')
  if (!VALID_AVAILABILITY.includes(offer.availability)) errors.push('invalid_availability')
  if (!Array.isArray(offer.tags) || offer.tags.length === 0) errors.push('invalid_tags')
  if (!(typeof offer.confidence_score === 'number' && offer.confidence_score >= 0 && offer.confidence_score <= 1)) {
    errors.push('invalid_confidence_score')
  }
  if (!(Number.isInteger(offer.freshness_ttl_hours) && offer.freshness_ttl_hours >= 1 && offer.freshness_ttl_hours <= 720)) {
    errors.push('invalid_freshness_ttl_hours')
  }
  return errors
}

function dedupeOffers(rows = []) {
  const map = new Map()
  for (const row of rows) {
    const key = [
      cleanText(row.brand_id),
      cleanText(row.offer_type),
      cleanText(row.target_url).toLowerCase(),
      cleanText(row.title).toLowerCase(),
    ].join('|')
    const existing = map.get(key)
    if (!existing || row.confidence_score > existing.confidence_score) map.set(key, row)
  }
  return [...map.values()]
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const tag = timestampTag()
  const maxBrands = toInteger(args['max-brands'], 0)
  const minProductOffers = toInteger(args['min-product-offers'], 6)
  const fillModeRaw = cleanText(args['fill-mode'] || 'missing').toLowerCase()
  const fillMode = ['missing', 'topup', 'all'].includes(fillModeRaw) ? fillModeRaw : 'missing'
  const categoryAllowlist = cleanText(args['category-allowlist'])
    .split(',')
    .map((item) => cleanText(item))
    .filter(Boolean)

  const jobs = await loadJobs(args)
  const realOffers = await loadRealOffers(args)
  const allowlistSet = new Set(categoryAllowlist.map((item) => item.toLowerCase()))
  const filteredJobs = allowlistSet.size === 0
    ? jobs
    : jobs.filter((job) => allowlistSet.has(`${cleanText(job.vertical_l1)}::${cleanText(job.vertical_l2)}`.toLowerCase()))
  const scopedJobs = maxBrands > 0 ? filteredJobs.slice(0, Math.max(1, maxBrands)) : filteredJobs

  const realProductCountByBrand = new Map()
  for (const offer of realOffers) {
    if (cleanText(offer.offer_type) !== 'product') continue
    const brandId = cleanText(offer.brand_id)
    realProductCountByBrand.set(brandId, (realProductCountByBrand.get(brandId) || 0) + 1)
  }

  const synthetic = []
  const skippedBrands = []
  const rejected = []

  for (const job of scopedJobs) {
    const brandId = cleanText(job.brand_id)
    const sourcePolicy = sourcePolicyForCategory(job.vertical_l1, job.vertical_l2, 'default')
    if (sourcePolicy === 'real_only') {
      skippedBrands.push({
        brand_id: brandId,
        reason: 'category_real_only',
        category_key: `${cleanText(job.vertical_l1)}::${cleanText(job.vertical_l2)}`,
      })
      continue
    }
    const existingProduct = realProductCountByBrand.get(brandId) || 0
    const targetCount = targetProductCount(job, existingProduct, {
      fillMode,
      minProductOffers,
    })
    if (targetCount <= 0) {
      skippedBrands.push({
        brand_id: brandId,
        reason: 'already_has_real_product_offers',
        existing_product_offers: existingProduct,
      })
      continue
    }

    const priceBand = normalizePriceBand(job)
    const startIndex = fillMode === 'topup' ? Math.max(0, existingProduct) : 0
    for (let i = 0; i < targetCount; i += 1) {
      const offer = buildSyntheticOffer(job, startIndex + i, priceBand)
      const errors = validateProductOffer(offer)
      if (errors.length > 0) {
        rejected.push({
          brand_id: brandId,
          offer_id: offer.offer_id,
          reason: errors.join('|'),
        })
        continue
      }
      synthetic.push(offer)
    }
  }

  const deduped = dedupeOffers(synthetic)
  await ensureDir(OFFERS_CURATED_DIR)
  const outputPath = path.join(OFFERS_CURATED_DIR, `offers-synthetic-${tag}.jsonl`)
  const summaryPath = path.join(OFFERS_CURATED_DIR, `offers-synthetic-${tag}.summary.json`)
  const rejectedPath = path.join(OFFERS_CURATED_DIR, `offers-synthetic-${tag}.rejected.json`)
  const latestMetaPath = path.join(OFFERS_CURATED_DIR, 'latest-offers-synthetic.json')

  const perBrand = new Map()
  const perCategory = new Map()
  for (const offer of deduped) {
    perBrand.set(offer.brand_id, (perBrand.get(offer.brand_id) || 0) + 1)
    const ckey = `${offer.vertical_l1}::${offer.vertical_l2}`
    perCategory.set(ckey, (perCategory.get(ckey) || 0) + 1)
  }
  const perBrandCounts = [...perBrand.values()]
  const summary = {
    generatedAt: new Date().toISOString(),
    fillMode,
    minProductOffers,
    categoryAllowlist,
    inputJobs: scopedJobs.length,
    realOffers: realOffers.length,
    brandsWithRealProductOffers: realProductCountByBrand.size,
    syntheticOffersGenerated: synthetic.length,
    syntheticOffersDeduped: deduped.length,
    syntheticBrandsCovered: perBrand.size,
    perBrandMinOffers: perBrandCounts.length ? Math.min(...perBrandCounts) : 0,
    perBrandMaxOffers: perBrandCounts.length ? Math.max(...perBrandCounts) : 0,
    perBrandAvgOffers: perBrandCounts.length
      ? Number((perBrandCounts.reduce((sum, n) => sum + n, 0) / perBrandCounts.length).toFixed(4))
      : 0,
    rejectedCount: rejected.length,
    skippedBrandCount: skippedBrands.length,
    targetUrlStrategy: 'canonical_landing_url',
    imageUrlStrategy: 'picsum_seed_image',
    categoryDistribution: Object.fromEntries([...perCategory.entries()].sort((a, b) => b[1] - a[1])),
    output: path.relative(process.cwd(), outputPath),
  }

  await writeJsonl(outputPath, deduped)
  await writeJson(summaryPath, summary)
  await writeJson(rejectedPath, rejected)
  await writeJson(latestMetaPath, {
    generatedAt: new Date().toISOString(),
    latestJsonl: path.relative(process.cwd(), outputPath),
    latestSummary: path.relative(process.cwd(), summaryPath),
    latestRejected: path.relative(process.cwd(), rejectedPath),
  })

  console.log(
    JSON.stringify(
      {
        ok: true,
        fillMode,
        inputJobs: scopedJobs.length,
        syntheticOffers: deduped.length,
        syntheticBrandsCovered: perBrand.size,
        rejectedCount: rejected.length,
        outputFile: path.relative(process.cwd(), outputPath),
      },
      null,
      2,
    ),
  )
}

if (path.resolve(process.argv[1] || '') === __filename) {
  main().catch((error) => {
    console.error('[synthesize-offers] failed:', error?.message || error)
    process.exit(1)
  })
}

export const __synthesizeOffersInternal = Object.freeze({
  resolveSyntheticTargetUrl,
  buildSyntheticImageUrl,
  buildSyntheticOffer,
})
