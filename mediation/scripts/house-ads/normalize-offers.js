#!/usr/bin/env node
import path from 'node:path'
import {
  parseArgs,
  toInteger,
  readJson,
  readJsonl,
  writeJson,
  writeJsonl,
  ensureDir,
  cleanText,
  normalizeUrl,
  hashId,
  timestampTag,
} from './lib/common.js'

const OFFERS_ROOT = path.resolve(process.cwd(), 'data/house-ads/offers')
const OFFER_SIGNALS_DIR = path.join(OFFERS_ROOT, 'raw', 'offer-signals')
const OFFERS_CURATED_DIR = path.join(OFFERS_ROOT, 'curated')

const VALID_OFFER_TYPES = new Set(['link', 'product'])
const VALID_STATUS = new Set(['active', 'paused', 'archived'])
const VALID_SOURCE_TYPE = new Set(['real', 'partner', 'synthetic'])
const VALID_AVAILABILITY = new Set(['in_stock', 'limited', 'preorder', 'out_of_stock', 'unknown'])

function clamp01(value) {
  const num = Number(value)
  if (!Number.isFinite(num)) return 0
  if (num < 0) return 0
  if (num > 1) return 1
  return Number(num.toFixed(4))
}

function asInt(value, fallback) {
  const num = Number(value)
  if (!Number.isFinite(num)) return fallback
  return Math.floor(num)
}

function safeUrl(urlText) {
  const normalized = normalizeUrl(urlText || '')
  if (!normalized || !/^https?:\/\//i.test(normalized)) return ''
  try {
    const parsed = new URL(normalized)
    return parsed.toString()
  } catch {
    return ''
  }
}

function normalizeLanguage(value = '') {
  const text = cleanText(value || '').toLowerCase()
  if (!text) return 'en'
  return text.slice(0, 10)
}

function normalizeCurrency(value = '') {
  const text = cleanText(value || '').toUpperCase()
  if (/^[A-Z]{3}$/.test(text)) return text
  if (text.includes('$') || text.includes('USD')) return 'USD'
  if (text.includes('EUR') || text.includes('€')) return 'EUR'
  if (text.includes('GBP') || text.includes('£')) return 'GBP'
  if (text.includes('JPY') || text.includes('¥')) return 'JPY'
  return 'USD'
}

function normalizeAvailability(value = '') {
  const text = cleanText(value || '').toLowerCase()
  if (VALID_AVAILABILITY.has(text)) return text
  if (text.includes('in stock')) return 'in_stock'
  if (text.includes('limited')) return 'limited'
  if (text.includes('preorder') || text.includes('pre-order')) return 'preorder'
  if (text.includes('out of stock') || text.includes('sold out')) return 'out_of_stock'
  return 'unknown'
}

function slugify(value = '') {
  return cleanText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+/, '')
    .replace(/-+$/, '')
}

function normalizeTags(candidate, signal) {
  const tags = Array.isArray(candidate?.tags) ? candidate.tags : []
  const fromCandidate = tags.map((item) => cleanText(item)).filter(Boolean)
  const fallback = [cleanText(signal.vertical_l1), cleanText(signal.vertical_l2)].filter(Boolean)
  const merged = [...fromCandidate, ...fallback]
  const seen = new Set()
  const out = []
  for (const item of merged) {
    const key = item.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    out.push(item.slice(0, 40))
  }
  return out.length > 0 ? out : ['general']
}

function campaignId(brandId, verticalL2, offerType) {
  const stable = `${brandId}|${verticalL2}|${offerType}`
  return `campaign_${slugify(verticalL2 || offerType || 'offer')}_${hashId(stable, 10)}`
}

function offerId(campaignIdValue, targetUrl, title, idx) {
  return `offer_${hashId(`${campaignIdValue}|${targetUrl}|${title}|${idx}`, 12)}`
}

function toPositiveNumber(value) {
  const num = Number(value)
  if (!Number.isFinite(num) || num <= 0) return null
  return Number(num.toFixed(2))
}

function normalizeLinkOffer(signal, candidate, idx) {
  const targetUrl = safeUrl(candidate.target_url || candidate.source_url || '')
  if (!targetUrl) return null
  const brandId = cleanText(signal.brand_id)
  const verticalL2 = cleanText(signal.vertical_l2)
  const campaign = campaignId(brandId, verticalL2, 'link')
  const title = cleanText(candidate.title || '').slice(0, 120)
  const description = cleanText(candidate.description || `Explore ${title}`).slice(0, 240)
  if (!title || !description) return null

  const imageUrl = safeUrl(candidate.image_url || '')
  const output = {
    offer_id: offerId(campaign, targetUrl, title, idx),
    campaign_id: campaign,
    brand_id: brandId,
    offer_type: 'link',
    vertical_l1: cleanText(signal.vertical_l1),
    vertical_l2: verticalL2,
    market: cleanText(signal.market) || 'US',
    title,
    description,
    target_url: targetUrl,
    cta_text: cleanText(candidate.cta_text || 'Shop Now').slice(0, 40),
    status: 'active',
    language: normalizeLanguage(candidate.language || 'en'),
    disclosure: cleanText(candidate.disclosure || 'Sponsored').slice(0, 120),
    source_type: 'real',
    confidence_score: clamp01(candidate.confidence),
    freshness_ttl_hours: 48,
    last_verified_at: cleanText(signal.updated_at) || new Date().toISOString(),
    tags: normalizeTags(candidate, signal),
    extraction_method: cleanText(candidate.extraction_method || ''),
    source_url: safeUrl(candidate.source_url || ''),
  }
  if (imageUrl) output.image_url = imageUrl
  return output
}

function normalizeProductOffer(signal, candidate, idx) {
  const targetUrl = safeUrl(candidate.target_url || candidate.source_url || '')
  if (!targetUrl) return null
  const brandId = cleanText(signal.brand_id)
  const verticalL2 = cleanText(signal.vertical_l2)
  const campaign = campaignId(brandId, verticalL2, 'product')
  const title = cleanText(candidate.title || '').slice(0, 120)
  const snippet = cleanText(candidate.snippet || candidate.description || '').slice(0, 240)
  const price = toPositiveNumber(candidate.price)
  if (!title || !snippet) return null

  const originalPrice = toPositiveNumber(candidate.original_price)
  const imageUrl = safeUrl(candidate.image_url || '')
  const output = {
    offer_id: offerId(campaign, targetUrl, title, idx),
    campaign_id: campaign,
    brand_id: brandId,
    offer_type: 'product',
    vertical_l1: cleanText(signal.vertical_l1),
    vertical_l2: verticalL2,
    market: cleanText(signal.market) || 'US',
    title,
    snippet,
    target_url: targetUrl,
    status: 'active',
    language: normalizeLanguage(candidate.language || 'en'),
    disclosure: cleanText(candidate.disclosure || 'Sponsored').slice(0, 120),
    source_type: 'real',
    confidence_score: clamp01(candidate.confidence),
    freshness_ttl_hours: 48,
    last_verified_at: cleanText(signal.updated_at) || new Date().toISOString(),
    product_id: cleanText(candidate.product_id || `prd_${hashId(`${brandId}|${targetUrl}|${title}`, 10)}`),
    merchant: cleanText(candidate.merchant || signal.brand_name || brandId),
    currency: normalizeCurrency(candidate.currency),
    availability: normalizeAvailability(candidate.availability),
    tags: normalizeTags(candidate, signal),
    extraction_method: cleanText(candidate.extraction_method || ''),
    source_url: safeUrl(candidate.source_url || ''),
  }
  if (imageUrl) output.image_url = imageUrl

  if (price !== null) {
    output.price = price
    if (originalPrice && originalPrice >= price) {
      output.original_price = originalPrice
      output.discount_pct = Number((((originalPrice - price) / originalPrice) * 100).toFixed(2))
    }
  }
  return output
}

function validateOffer(offer) {
  const errors = []
  const nonEmpty = (v) => typeof v === 'string' && cleanText(v).length > 0

  const requiredBase = [
    'offer_id',
    'campaign_id',
    'brand_id',
    'offer_type',
    'vertical_l1',
    'vertical_l2',
    'market',
    'title',
    'target_url',
    'status',
    'source_type',
    'confidence_score',
    'freshness_ttl_hours',
    'last_verified_at',
  ]
  for (const field of requiredBase) {
    if (!(field in offer)) errors.push(`missing_${field}`)
    else if (typeof offer[field] === 'string' && !nonEmpty(offer[field])) errors.push(`empty_${field}`)
  }

  if (!VALID_OFFER_TYPES.has(offer.offer_type)) errors.push('invalid_offer_type')
  if (!VALID_STATUS.has(offer.status)) errors.push('invalid_status')
  if (!VALID_SOURCE_TYPE.has(offer.source_type)) errors.push('invalid_source_type')
  if (!/^https?:\/\//i.test(String(offer.target_url || ''))) errors.push('invalid_target_url')
  if (!(typeof offer.confidence_score === 'number' && offer.confidence_score >= 0 && offer.confidence_score <= 1)) {
    errors.push('invalid_confidence_score')
  }
  if (!(Number.isInteger(offer.freshness_ttl_hours) && offer.freshness_ttl_hours >= 1 && offer.freshness_ttl_hours <= 720)) {
    errors.push('invalid_freshness_ttl_hours')
  }
  if (!Array.isArray(offer.tags) || offer.tags.length === 0) errors.push('invalid_tags')

  if (offer.offer_type === 'link') {
    for (const field of ['description', 'cta_text', 'language', 'disclosure']) {
      if (!nonEmpty(offer[field])) errors.push(`missing_or_empty_${field}`)
    }
  }
  if (offer.offer_type === 'product') {
    for (const field of ['snippet', 'product_id', 'merchant', 'currency', 'availability', 'language', 'disclosure']) {
      if (!nonEmpty(offer[field])) errors.push(`missing_or_empty_${field}`)
    }
    if (
      'price' in offer
      && !(typeof offer.price === 'number' && offer.price > 0)
    ) {
      errors.push('invalid_price')
    }
    if (!VALID_AVAILABILITY.has(offer.availability)) errors.push('invalid_availability')
    if (!/^[A-Z]{3}$/.test(String(offer.currency || ''))) errors.push('invalid_currency')
  }

  return errors
}

function removeRuntimeOnlyFields(offer) {
  const { extraction_method, source_url, ...rest } = offer
  return rest
}

async function loadSignals(args) {
  const explicit = cleanText(args['signals-file'])
  if (explicit) return readJsonl(path.resolve(process.cwd(), explicit))
  const latestMetaPath = path.join(OFFER_SIGNALS_DIR, 'latest-offer-signals.json')
  const latestMeta = await readJson(latestMetaPath, null)
  if (!latestMeta?.latestJsonl) {
    throw new Error('No latest offer signals metadata found. Run crawl-offers first.')
  }
  return readJsonl(path.resolve(process.cwd(), latestMeta.latestJsonl))
}

function normalizeSignalsToOffers(signals, options) {
  const output = []
  const rejected = []

  for (const signal of signals) {
    const candidates = Array.isArray(signal.candidates) ? signal.candidates : []
    if (candidates.length === 0) continue
    let localIndex = 0
    for (const candidate of candidates) {
      localIndex += 1
      let normalized = null
      const offerType = cleanText(candidate.offer_type || '').toLowerCase()

      if (offerType === 'product') {
        normalized = normalizeProductOffer(signal, candidate, localIndex)
        if (!normalized && options.fallbackProductToLink) {
          normalized = normalizeLinkOffer(signal, candidate, localIndex)
        }
      } else {
        normalized = normalizeLinkOffer(signal, candidate, localIndex)
      }
      if (!normalized) {
        rejected.push({
          brand_id: cleanText(signal.brand_id),
          offer_type: offerType || 'unknown',
          reason: 'normalize_failed',
          candidate_title: cleanText(candidate.title || ''),
          candidate_url: cleanText(candidate.target_url || ''),
        })
        continue
      }

      const errors = validateOffer(normalized)
      if (errors.length > 0) {
        rejected.push({
          brand_id: cleanText(signal.brand_id),
          offer_type: normalized.offer_type,
          reason: errors.join('|'),
          candidate_title: cleanText(candidate.title || ''),
          candidate_url: cleanText(candidate.target_url || ''),
        })
        continue
      }

      output.push(removeRuntimeOnlyFields(normalized))
    }
  }

  return { output, rejected }
}

function dedupeOffers(offers = []) {
  const map = new Map()
  for (const offer of offers) {
    const key = [
      cleanText(offer.campaign_id),
      cleanText(offer.offer_type),
      cleanText(offer.target_url).toLowerCase(),
      cleanText(offer.title).toLowerCase(),
    ].join('|')
    const existing = map.get(key)
    if (!existing || offer.confidence_score > existing.confidence_score) {
      map.set(key, offer)
    }
  }
  return [...map.values()]
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const tag = timestampTag()
  const maxSignals = toInteger(args['max-signals'], 0)
  const fallbackProductToLink = String(args['fallback-product-to-link'] || 'true').toLowerCase() !== 'false'

  const signals = await loadSignals(args)
  const scopedSignals = maxSignals > 0 ? signals.slice(0, Math.max(1, maxSignals)) : signals

  const { output, rejected } = normalizeSignalsToOffers(scopedSignals, { fallbackProductToLink })
  const deduped = dedupeOffers(output)

  await ensureDir(OFFERS_CURATED_DIR)
  const outputPath = path.join(OFFERS_CURATED_DIR, `offers-real-${tag}.jsonl`)
  const summaryPath = path.join(OFFERS_CURATED_DIR, `offers-real-${tag}.summary.json`)
  const rejectedPath = path.join(OFFERS_CURATED_DIR, `offers-real-${tag}.rejected.json`)
  const latestMetaPath = path.join(OFFERS_CURATED_DIR, 'latest-offers-real.json')

  const byType = {
    link: deduped.filter((row) => row.offer_type === 'link').length,
    product: deduped.filter((row) => row.offer_type === 'product').length,
  }
  const uniqueCampaigns = new Set(deduped.map((row) => row.campaign_id)).size
  const uniqueBrands = new Set(deduped.map((row) => row.brand_id)).size
  const summary = {
    generatedAt: new Date().toISOString(),
    inputSignals: scopedSignals.length,
    normalizedOffers: output.length,
    dedupedOffers: deduped.length,
    rejectedCandidates: rejected.length,
    uniqueBrands,
    uniqueCampaigns,
    byType,
    output: path.relative(process.cwd(), outputPath),
    rejectedOutput: path.relative(process.cwd(), rejectedPath),
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
        inputSignals: scopedSignals.length,
        normalizedOffers: output.length,
        dedupedOffers: deduped.length,
        rejectedCandidates: rejected.length,
        uniqueBrands,
        uniqueCampaigns,
        byType,
        outputFile: path.relative(process.cwd(), outputPath),
      },
      null,
      2,
    ),
  )
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error('[normalize-offers] failed:', error?.message || error)
    process.exit(1)
  })
}

export const __normalizeOffersInternal = Object.freeze({
  normalizeProductOffer,
  validateOffer,
})
