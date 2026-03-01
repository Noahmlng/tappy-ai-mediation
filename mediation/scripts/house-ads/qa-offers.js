#!/usr/bin/env node
import fs from 'node:fs/promises'
import path from 'node:path'
import {
  CURATED_ROOT,
  parseArgs,
  toInteger,
  toBoolean,
  cleanText,
  readJson,
  readJsonl,
  writeJson,
  writeJsonl,
  ensureDir,
  registrableDomain,
  timestampTag,
} from './lib/common.js'
import {
  categoryKey,
  sourcePolicyForCategory,
  isDeferredSensitiveCategory,
  hasBlockedKeyword,
} from './lib/category-policy.js'

const OFFERS_ROOT = path.resolve(process.cwd(), 'data/house-ads/offers')
const OFFERS_CURATED_DIR = path.join(OFFERS_ROOT, 'curated')
const OFFERS_REPORT_DIR = path.join(OFFERS_ROOT, 'reports')

const VALID_OFFER_TYPES = new Set(['link', 'product'])
const VALID_STATUS = new Set(['active', 'paused', 'archived'])
const VALID_SOURCE_TYPE = new Set(['real', 'partner', 'synthetic'])
const VALID_AVAILABILITY = new Set(['in_stock', 'limited', 'preorder', 'out_of_stock', 'unknown'])

const INSTITUTIONAL_TLDS = new Set(['gov', 'edu', 'mil', 'int'])
const SHORTLINK_DOMAINS = new Set([
  't.co',
  't.me',
  'wa.me',
  'm.me',
  'g.co',
  'g.page',
  'a.co',
  'bit.ly',
  'tinyurl.com',
  'lnkd.in',
  'ow.ly',
  'is.gd',
])
const HOSTING_HINTS = [
  'github.io',
  'pages.dev',
  'netlify.app',
  'vercel.app',
  'blogspot.',
  'wordpress.com',
  'wixsite.com',
  'appspot.com',
  'cloudfront.net',
  'amazonaws.com',
  'googleusercontent.com',
]
const RISKY_TERMS = [
  'torrent',
  'crack',
  'pirated',
  'adult',
  'porn',
  'casino',
  'betting',
  'loan shark',
  'counterfeit',
]
const IMAGE_EXT_RE = /\.(png|jpe?g|webp|gif|svg|avif|bmp|ico)(\?|#|$)/i

function toNumber(value, fallback = 0) {
  const num = Number(value)
  return Number.isFinite(num) ? num : fallback
}

function safeUrl(urlText) {
  const text = cleanText(urlText || '')
  if (!text) return ''
  try {
    const parsed = new URL(text)
    if (!['http:', 'https:'].includes(parsed.protocol)) return ''
    return parsed.toString()
  } catch {
    return ''
  }
}

function classifyTargetDomain(targetUrl = '') {
  const safe = safeUrl(targetUrl)
  if (!safe) return { domain: '', blocked: true, reason: 'invalid_target_url' }
  const parsed = new URL(safe)
  const host = registrableDomain(parsed.hostname)
  if (!host) return { domain: '', blocked: true, reason: 'invalid_target_domain' }
  if (SHORTLINK_DOMAINS.has(host)) return { domain: host, blocked: true, reason: 'shortlink_domain' }
  if (HOSTING_HINTS.some((hint) => host.includes(hint))) return { domain: host, blocked: true, reason: 'infra_hosting_domain' }
  const parts = host.split('.')
  const tld = parts[parts.length - 1] || ''
  if (INSTITUTIONAL_TLDS.has(tld)) return { domain: host, blocked: true, reason: 'institution_domain' }
  return { domain: host, blocked: false, reason: '' }
}

function dedupeKey(offer) {
  return [
    cleanText(offer.brand_id).toLowerCase(),
    cleanText(offer.target_url).toLowerCase(),
    cleanText(offer.title).toLowerCase(),
  ].join('|')
}

function sourcePriority(value) {
  const v = cleanText(value).toLowerCase()
  if (v === 'real') return 3
  if (v === 'partner') return 2
  if (v === 'synthetic') return 1
  return 0
}

function pickBetterOffer(current, incoming) {
  const currentScore = toNumber(current?.confidence_score, 0)
  const incomingScore = toNumber(incoming?.confidence_score, 0)
  if (incomingScore > currentScore) return incoming
  if (incomingScore < currentScore) return current
  const currentPriority = sourcePriority(current?.source_type)
  const incomingPriority = sourcePriority(incoming?.source_type)
  if (incomingPriority > currentPriority) return incoming
  return current
}

function hasRiskyTerm(value = '') {
  const text = cleanText(value).toLowerCase()
  if (!text) return false
  return RISKY_TERMS.some((term) => text.includes(term))
}

function isNonEmptyString(value) {
  return typeof value === 'string' && cleanText(value).length > 0
}

function normalizeImageContentType(value = '') {
  return cleanText(value).toLowerCase().split(';')[0]
}

function isImageContentType(value = '') {
  return normalizeImageContentType(value).startsWith('image/')
}

function looksLikeImageUrl(url = '') {
  return IMAGE_EXT_RE.test(cleanText(url))
}

function classifyFetchError(error) {
  const message = cleanText(error?.message || String(error || '')).toLowerCase()
  const name = cleanText(error?.name || '').toLowerCase()
  if (name.includes('abort') || message.includes('abort') || message.includes('timeout')) return 'timeout'
  if (message.includes('enotfound') || message.includes('getaddrinfo') || message.includes('dns')) return 'dns'
  return 'request_error'
}

async function probeImageUrl(url, timeoutMs = 10000) {
  const target = safeUrl(url)
  if (!target) {
    return {
      ok: false,
      status: 0,
      valid_image: false,
      status_class: 'invalid_image_url',
      content_type: '',
      error_class: 'invalid_image_url',
      error_message: 'invalid_image_url',
    }
  }

  const run = async (method) => {
    const response = await fetch(target, {
      method,
      redirect: 'follow',
      signal: AbortSignal.timeout(Math.max(1000, timeoutMs)),
      headers: {
        'user-agent': 'house-ads-qa-offers/1.0',
      },
    })
    const contentType = normalizeImageContentType(response.headers.get('content-type') || '')
    return {
      status: Number(response.status) || 0,
      content_type: contentType,
      valid_image:
        response.status >= 200
        && response.status < 400
        && (isImageContentType(contentType) || looksLikeImageUrl(response.url || target)),
      status_class:
        response.status >= 200 && response.status < 300
          ? '2xx'
          : response.status >= 300 && response.status < 400
            ? '3xx'
            : response.status >= 400 && response.status < 500
              ? '4xx'
              : response.status >= 500 && response.status < 600
                ? '5xx'
                : 'other_http',
    }
  }

  try {
    let result = await run('HEAD')
    if ([403, 405].includes(result.status)) result = await run('GET')
    return {
      ok: result.valid_image,
      status: result.status,
      valid_image: result.valid_image,
      status_class: result.status_class,
      content_type: result.content_type,
      error_class: '',
      error_message: '',
    }
  } catch (error) {
    const errorClass = classifyFetchError(error)
    return {
      ok: false,
      status: 0,
      valid_image: false,
      status_class: errorClass,
      content_type: '',
      error_class: errorClass,
      error_message: cleanText(error?.message || String(error)),
    }
  }
}

function ensurePriceMissingTag(offer = {}) {
  if (cleanText(offer.offer_type).toLowerCase() !== 'product') return offer
  if (typeof offer.price === 'number' && offer.price > 0) return offer
  const tags = Array.isArray(offer.tags) ? [...offer.tags] : []
  const exists = tags.some((tag) => cleanText(tag).toLowerCase() === 'price_missing')
  if (!exists) tags.push('price_missing')
  return {
    ...offer,
    tags,
  }
}

function validateOffer(offer, brandMap, options, imageProbe = null) {
  const reasons = []

  const offerType = cleanText(offer.offer_type).toLowerCase()
  const status = cleanText(offer.status).toLowerCase()
  const sourceType = cleanText(offer.source_type).toLowerCase()
  const brandId = cleanText(offer.brand_id)
  const title = cleanText(offer.title)
  const targetUrl = safeUrl(offer.target_url)

  if (!brandId) reasons.push('missing_brand_id')
  if (!brandMap.has(brandId)) reasons.push('brand_not_in_approved_pool')
  if (!VALID_OFFER_TYPES.has(offerType)) reasons.push('invalid_offer_type')
  if (!VALID_STATUS.has(status)) reasons.push('invalid_status')
  if (!VALID_SOURCE_TYPE.has(sourceType)) reasons.push('invalid_source_type')
  if (!targetUrl) reasons.push('invalid_target_url')
  if (!title) reasons.push('missing_title')
  if (hasRiskyTerm(`${title} ${cleanText(offer.description)} ${cleanText(offer.snippet)}`)) {
    reasons.push('contains_risky_term')
  }
  if (options.sensitiveBlock && hasBlockedKeyword(
    `${title} ${cleanText(offer.description)} ${cleanText(offer.snippet)} ${JSON.stringify(offer.tags || [])}`,
  )) {
    reasons.push('blocked_keyword_sensitive')
  }
  if (options.sensitiveBlock && isDeferredSensitiveCategory(offer.vertical_l1, offer.vertical_l2)) {
    reasons.push('category_deferred_sensitive')
  }

  if (status !== 'active') reasons.push('status_not_active')

  const confidence = toNumber(offer.confidence_score, -1)
  if (!(confidence >= 0 && confidence <= 1)) reasons.push('invalid_confidence_score')
  const threshold = sourceType === 'synthetic' ? options.minConfidenceSynthetic : options.minConfidenceReal
  if (confidence < threshold) reasons.push('confidence_below_threshold')

  const ttl = toInteger(offer.freshness_ttl_hours, -1)
  if (!(ttl >= 1 && ttl <= 720)) reasons.push('invalid_freshness_ttl_hours')

  const targetDomain = classifyTargetDomain(targetUrl)
  if (targetDomain.blocked) reasons.push(`target_domain_blocked:${targetDomain.reason}`)
  if (
    options.sourcePolicy === 'real_only'
    && sourcePolicyForCategory(offer.vertical_l1, offer.vertical_l2, 'default') === 'real_only'
    && sourceType !== 'real'
  ) {
    reasons.push('category_requires_real_source')
  }

  const brand = brandMap.get(brandId)
  const brandDomain = registrableDomain(brand?.official_domain || '')
  if (sourceType === 'synthetic' && brandDomain && targetDomain.domain && targetDomain.domain !== brandDomain) {
    reasons.push('synthetic_domain_mismatch')
  }

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
    if (!(field in offer)) reasons.push(`missing_${field}`)
  }

  if (offerType === 'link') {
    for (const field of ['description', 'cta_text', 'language', 'disclosure']) {
      if (!isNonEmptyString(offer[field])) reasons.push(`missing_or_empty_${field}`)
    }
  } else if (offerType === 'product') {
    for (const field of ['snippet', 'product_id', 'merchant', 'currency', 'availability', 'language', 'disclosure']) {
      if (!isNonEmptyString(offer[field])) reasons.push(`missing_or_empty_${field}`)
    }
    if ('price' in offer && !(typeof offer.price === 'number' && offer.price > 0)) reasons.push('invalid_price')
    if ('original_price' in offer && !(typeof offer.original_price === 'number' && offer.original_price >= offer.price)) {
      reasons.push('invalid_original_price')
    }
    if (!/^[A-Z]{3}$/.test(cleanText(offer.currency || ''))) reasons.push('invalid_currency')
    if (!VALID_AVAILABILITY.has(cleanText(offer.availability || ''))) reasons.push('invalid_availability')
    if (!Array.isArray(offer.tags) || offer.tags.length === 0) reasons.push('invalid_tags')
  }

  if (options.imageHardGate) {
    const imageUrl = safeUrl(offer.image_url)
    if (!imageUrl) {
      reasons.push('missing_or_invalid_image_url')
    } else if (!imageProbe || imageProbe.valid_image !== true) {
      const reason = cleanText(imageProbe?.status_class) || cleanText(imageProbe?.error_class) || 'image_probe_failed'
      reasons.push(`invalid_image_url:${reason}`)
    }
  }

  return [...new Set(reasons)]
}

function toCsv(rows = []) {
  if (!rows.length) return ''
  const headers = Object.keys(rows[0])
  const esc = (value) => {
    const text = String(value ?? '')
    if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`
    return text
  }
  const lines = [headers.join(',')]
  for (const row of rows) {
    lines.push(headers.map((h) => esc(row[h])).join(','))
  }
  return `${lines.join('\n')}\n`
}

async function writeCsv(filePath, rows = []) {
  await ensureDir(path.dirname(filePath))
  await fs.writeFile(filePath, toCsv(rows), 'utf8')
}

function reasonDistribution(rows = []) {
  const map = {}
  for (const row of rows) {
    const reasons = cleanText(row.reasons || '').split('|').filter(Boolean)
    for (const reason of reasons) {
      map[reason] = (map[reason] || 0) + 1
    }
  }
  return Object.fromEntries(Object.entries(map).sort((a, b) => b[1] - a[1]))
}

async function resolveOffersFile(args) {
  const explicit = cleanText(args['offers-file'])
  if (explicit) return path.resolve(process.cwd(), explicit)
  const latestMetaPath = path.join(OFFERS_CURATED_DIR, 'latest-offers-merged.json')
  const latestMeta = await readJson(latestMetaPath, null)
  if (!latestMeta?.latestJsonl) throw new Error('No latest merged offers found. Run merge-offers first.')
  return path.resolve(process.cwd(), latestMeta.latestJsonl)
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const tag = timestampTag()
  const minConfidenceReal = Number(args['min-confidence-real'] || 0.55)
  const minConfidenceSynthetic = Number(args['min-confidence-synthetic'] || 0.6)
  const maxOffers = toInteger(args['max-offers'], 0)
  const sourcePolicy = cleanText(args['source-policy'] || 'default').toLowerCase()
  const imageHardGate = toBoolean(args['image-hard-gate'], false)
  const sensitiveBlock = toBoolean(args['sensitive-block'], false)
  const imageTimeoutMs = toInteger(args['image-timeout-ms'], 10000)

  const offersFile = await resolveOffersFile(args)
  const brandsFile = path.resolve(process.cwd(), cleanText(args['brands-file']) || path.join(CURATED_ROOT, 'brands.jsonl'))
  const outputOffersFile = path.resolve(
    process.cwd(),
    cleanText(args['output-file']) || path.join(OFFERS_CURATED_DIR, 'offers.jsonl'),
  )

  const [offersRaw, brands] = await Promise.all([readJsonl(offersFile), readJsonl(brandsFile)])
  const offers = maxOffers > 0 ? offersRaw.slice(0, Math.max(1, maxOffers)) : offersRaw
  const brandMap = new Map(brands.map((brand) => [cleanText(brand.brand_id), brand]))

  const dedupeMap = new Map()
  const duplicateRejected = []

  for (const offer of offers) {
    const key = dedupeKey(offer)
    if (!key || key === '||') {
      duplicateRejected.push({
        offer_id: cleanText(offer.offer_id),
        brand_id: cleanText(offer.brand_id),
        offer_type: cleanText(offer.offer_type),
        title: cleanText(offer.title),
        target_url: cleanText(offer.target_url),
        source_type: cleanText(offer.source_type),
        confidence_score: toNumber(offer.confidence_score, 0),
        status: cleanText(offer.status),
        reasons: 'invalid_dedupe_key',
      })
      continue
    }
    const existing = dedupeMap.get(key)
    if (!existing) {
      dedupeMap.set(key, offer)
      continue
    }
    const chosen = pickBetterOffer(existing, offer)
    const dropped = chosen === existing ? offer : existing
    dedupeMap.set(key, chosen)
    duplicateRejected.push({
      offer_id: cleanText(dropped.offer_id),
      brand_id: cleanText(dropped.brand_id),
      offer_type: cleanText(dropped.offer_type),
      title: cleanText(dropped.title),
      target_url: cleanText(dropped.target_url),
      source_type: cleanText(dropped.source_type),
      confidence_score: toNumber(dropped.confidence_score, 0),
      status: cleanText(dropped.status),
      reasons: 'duplicate_lower_confidence',
    })
  }

  const accepted = []
  const rejected = [...duplicateRejected]
  const imageProbeCache = new Map()

  for (const rawOffer of dedupeMap.values()) {
    const offer = ensurePriceMissingTag(rawOffer)
    let imageProbe = null
    if (imageHardGate && cleanText(offer.image_url)) {
      const cacheKey = cleanText(offer.image_url)
      if (!imageProbeCache.has(cacheKey)) {
        imageProbeCache.set(cacheKey, await probeImageUrl(cacheKey, imageTimeoutMs))
      }
      imageProbe = imageProbeCache.get(cacheKey) || null
    }

    const reasons = validateOffer(offer, brandMap, {
      minConfidenceReal,
      minConfidenceSynthetic,
      sourcePolicy,
      imageHardGate,
      sensitiveBlock,
    }, imageProbe)
    if (reasons.length > 0) {
      rejected.push({
        offer_id: cleanText(offer.offer_id),
        brand_id: cleanText(offer.brand_id),
        offer_type: cleanText(offer.offer_type),
        title: cleanText(offer.title),
        target_url: cleanText(offer.target_url),
        source_type: cleanText(offer.source_type),
        confidence_score: toNumber(offer.confidence_score, 0),
        status: cleanText(offer.status),
        reasons: reasons.join('|'),
      })
      continue
    }
    accepted.push(offer)
  }

  const rejectedCsvPath = path.join(OFFERS_REPORT_DIR, `offers-rejected-${tag}.csv`)
  const rejectedLatestCsvPath = path.join(OFFERS_REPORT_DIR, 'offers-rejected-latest.csv')
  const summaryPath = path.join(OFFERS_REPORT_DIR, `offers-quality-summary-${tag}.json`)
  const summaryLatestPath = path.join(OFFERS_REPORT_DIR, 'offers-quality-summary-latest.json')

  const summary = {
    generated_at: new Date().toISOString(),
    input_file: path.relative(process.cwd(), offersFile),
    brands_file: path.relative(process.cwd(), brandsFile),
    thresholds: {
      min_confidence_real: minConfidenceReal,
      min_confidence_synthetic: minConfidenceSynthetic,
      source_policy: sourcePolicy,
      image_hard_gate: imageHardGate,
      sensitive_block: sensitiveBlock,
      image_timeout_ms: imageTimeoutMs,
    },
    counts: {
      input_offers: offers.length,
      deduped_offers: dedupeMap.size,
      accepted_offers: accepted.length,
      rejected_offers: rejected.length,
      duplicate_rejected: duplicateRejected.length,
    },
    accepted_by_type: {
      link: accepted.filter((item) => cleanText(item.offer_type) === 'link').length,
      product: accepted.filter((item) => cleanText(item.offer_type) === 'product').length,
    },
    accepted_by_source: {
      real: accepted.filter((item) => cleanText(item.source_type) === 'real').length,
      partner: accepted.filter((item) => cleanText(item.source_type) === 'partner').length,
      synthetic: accepted.filter((item) => cleanText(item.source_type) === 'synthetic').length,
    },
    accepted_product_price_missing: accepted.filter((item) =>
      cleanText(item.offer_type) === 'product'
      && !(typeof item.price === 'number' && item.price > 0)
      && Array.isArray(item.tags)
      && item.tags.some((tag) => cleanText(tag).toLowerCase() === 'price_missing')).length,
    accepted_unique_brands: new Set(accepted.map((item) => cleanText(item.brand_id))).size,
    rejected_reason_distribution: reasonDistribution(rejected),
    output_files: {
      accepted_offers_jsonl: path.relative(process.cwd(), outputOffersFile),
      rejected_csv: path.relative(process.cwd(), rejectedCsvPath),
      summary_json: path.relative(process.cwd(), summaryPath),
      latest_rejected_csv: path.relative(process.cwd(), rejectedLatestCsvPath),
      latest_summary_json: path.relative(process.cwd(), summaryLatestPath),
    },
  }

  await ensureDir(OFFERS_CURATED_DIR)
  await ensureDir(OFFERS_REPORT_DIR)
  await writeJsonl(outputOffersFile, accepted)
  await writeCsv(rejectedCsvPath, rejected)
  await writeJson(summaryPath, summary)
  await fs.copyFile(rejectedCsvPath, rejectedLatestCsvPath)
  await fs.copyFile(summaryPath, summaryLatestPath)

  console.log(
    JSON.stringify(
      {
        ok: true,
        inputOffers: offers.length,
        acceptedOffers: accepted.length,
        rejectedOffers: rejected.length,
        acceptedOutput: path.relative(process.cwd(), outputOffersFile),
        rejectedOutput: path.relative(process.cwd(), rejectedCsvPath),
        summaryOutput: path.relative(process.cwd(), summaryPath),
      },
      null,
      2,
    ),
  )
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error('[qa-offers] failed:', error?.message || error)
    process.exit(1)
  })
}

export const __qaOffersInternal = Object.freeze({
  validateOffer,
  ensurePriceMissingTag,
  probeImageUrl,
})
