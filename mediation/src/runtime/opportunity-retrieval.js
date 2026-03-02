import { buildQueryEmbedding, vectorToSqlLiteral } from './embedding.js'
import { normalizeUnifiedOffers } from '../offers/index.js'
import { queryBm25Candidates } from './bm25-index.js'

const DEFAULT_LEXICAL_TOP_K = 120
const DEFAULT_VECTOR_TOP_K = 120
const DEFAULT_FINAL_TOP_K = 40
const DEFAULT_RRF_K = 60
const DEFAULT_LOCALE_MATCH_MODE = 'locale_or_base'
const DEFAULT_INTENT_MIN_LEXICAL_SCORE = 0.02
const DEFAULT_HOUSE_LOWINFO_FILTER_ENABLED = true
const HOUSE_LOWINFO_TEMPLATE_PHRASE = 'option with strong category relevance and direct shopping intent'
const DEFAULT_HYBRID_STRATEGY = 'rrf_then_linear'
const DEFAULT_HYBRID_SPARSE_WEIGHT = 0.8
const DEFAULT_HYBRID_DENSE_WEIGHT = 0.2
const DEFAULT_BM25_REFRESH_INTERVAL_MS = 10 * 60 * 1000
const DEFAULT_BM25_COLD_START_WAIT_MS = 120
const DEFAULT_BRAND_MISS_PENALTY = 0.08
const DEFAULT_HOUSE_SHARE_CAP = 0.6
const DEFAULT_TOPIC_COVERAGE_THRESHOLD = 0.05
const DEFAULT_HOUSE_BRAND_MISS_MIN_PENALTY = 0.18
const DEFAULT_HOUSE_BRAND_MISS_DYNAMIC_RATIO = 0.45
const DEFAULT_HOUSE_BRAND_MISS_MAX_PENALTY = 0.65
const DEFAULT_PARTNER_BRAND_MISS_PENALTY = 0.08
const BRAND_ENTITY_STOPWORDS = new Set([
  'the', 'and', 'for', 'with', 'from', 'that', 'this', 'those', 'these', 'have', 'will', 'your', 'about', 'which',
  'what', 'when', 'where', 'who', 'how', 'why', 'would', 'could', 'should', 'very', 'more', 'most',
  'best', 'better', 'recommend', 'recommendation', 'recommendations', 'compare', 'comparison', 'price', 'prices',
  'pricing', 'deal', 'deals', 'tool', 'tools', 'platform', 'platforms', 'software', 'service', 'services',
  'please', 'thanks',
  'category', 'categories', 'automated', 'easiest', 'easy', 'recommended', 'approach', 'approaches', 'method', 'methods',
  'breakdown', 'all', 'one',
  '推荐', '比较', '对比', '价格', '优惠', '什么', '怎么', '可以', '帮我', '一下',
])

function cleanText(value) {
  return String(value || '').trim().replace(/\s+/g, ' ')
}

function clipText(value, maxLength = 220) {
  const text = cleanText(value)
  if (!text) return ''
  return text.length <= maxLength ? text : `${text.slice(0, maxLength)}...`
}

function toPositiveInteger(value, fallback) {
  const n = Number(value)
  if (!Number.isFinite(n) || n <= 0) return fallback
  return Math.floor(n)
}

function toFiniteNumber(value, fallback = 0) {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  return n
}

function clamp01(value, fallback = 0) {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  if (n < 0) return 0
  if (n > 1) return 1
  return n
}

function round(value, precision = 6) {
  const n = Number(value)
  if (!Number.isFinite(n)) return 0
  return Number(n.toFixed(precision))
}

function parseBoolean(value, fallback = false) {
  const normalized = String(value ?? '').trim().toLowerCase()
  if (!normalized) return fallback
  if (['1', 'true', 'on', 'yes', 'enabled'].includes(normalized)) return true
  if (['0', 'false', 'off', 'no', 'disabled'].includes(normalized)) return false
  return fallback
}

function normalizeLocaleMatchMode(value) {
  const normalized = cleanText(value).toLowerCase()
  if (!normalized) return DEFAULT_LOCALE_MATCH_MODE
  if (normalized === 'exact') return 'exact'
  if (normalized === DEFAULT_LOCALE_MATCH_MODE) return DEFAULT_LOCALE_MATCH_MODE
  if (normalized === 'base_or_locale') return DEFAULT_LOCALE_MATCH_MODE
  return DEFAULT_LOCALE_MATCH_MODE
}

function resolveLanguageFilter(language = '', matchMode = DEFAULT_LOCALE_MATCH_MODE) {
  const requested = cleanText(language)
  const normalizedLocale = requested.toLowerCase().replace(/_/g, '-')
  if (!normalizedLocale) {
    return {
      requested,
      normalized: '',
      base: '',
      accepted: [],
    }
  }
  const base = normalizedLocale.split('-')[0] || normalizedLocale
  if (matchMode === 'exact') {
    return {
      requested,
      normalized: normalizedLocale,
      base,
      accepted: [normalizedLocale],
    }
  }
  const accepted = base && base !== normalizedLocale
    ? [normalizedLocale, base]
    : [normalizedLocale]
  return {
    requested,
    normalized: normalizedLocale,
    base,
    accepted: Array.from(new Set(accepted)),
  }
}

function normalizeNetworkFilters(value = []) {
  if (!Array.isArray(value)) return []
  return Array.from(new Set(value
    .map((item) => cleanText(item).toLowerCase())
    .filter((item) => item === 'partnerstack' || item === 'cj' || item === 'house')))
}

function normalizeFilters(filters = {}) {
  const input = filters && typeof filters === 'object' ? filters : {}
  return {
    networks: normalizeNetworkFilters(input.networks),
    market: cleanText(input.market).toUpperCase(),
    language: cleanText(input.language).replace(/_/g, '-'),
  }
}

function countCandidatesByNetwork(candidates = []) {
  const seed = { partnerstack: 0, cj: 0, house: 0 }
  for (const candidate of candidates) {
    const network = cleanText(candidate?.network).toLowerCase()
    if (!network) continue
    if (Object.prototype.hasOwnProperty.call(seed, network)) {
      seed[network] += 1
      continue
    }
    seed[network] = (seed[network] || 0) + 1
  }
  return seed
}

function matchesLanguageWithMode(language = '', languageFilter = {}) {
  const candidateLanguage = cleanText(language).toLowerCase().replace(/_/g, '-')
  const accepted = Array.isArray(languageFilter.accepted) ? languageFilter.accepted : []
  if (accepted.length <= 0) return true
  return accepted.includes(candidateLanguage)
}

function isHouseLowInfoCandidate(candidate = {}, threshold = DEFAULT_INTENT_MIN_LEXICAL_SCORE) {
  const network = cleanText(candidate?.network).toLowerCase()
  if (network !== 'house') return false
  const tags = Array.isArray(candidate?.tags) ? candidate.tags : []
  const hasSyntheticTag = tags.some((tag) => cleanText(tag).toLowerCase() === 'synthetic')
  if (!hasSyntheticTag) return false
  const description = cleanText(candidate?.description).toLowerCase()
  if (!description.includes(HOUSE_LOWINFO_TEMPLATE_PHRASE)) return false
  return toFiniteNumber(candidate?.lexicalScore, 0) < threshold
}

function applyHouseLowInfoFilter(candidates = [], policy = {}) {
  const enabled = parseBoolean(policy?.enabled, DEFAULT_HOUSE_LOWINFO_FILTER_ENABLED)
  const lexicalThreshold = clamp01(
    policy?.minLexicalScore,
    DEFAULT_INTENT_MIN_LEXICAL_SCORE,
  )
  const beforeCounts = countCandidatesByNetwork(candidates)
  if (!enabled || candidates.length <= 0) {
    return {
      candidates,
      filteredCount: 0,
      beforeCounts,
      afterCounts: beforeCounts,
      enabled,
      lexicalThreshold,
    }
  }

  let filteredCount = 0
  const filtered = candidates.filter((candidate) => {
    const shouldFilter = isHouseLowInfoCandidate(candidate, lexicalThreshold)
    if (shouldFilter) filteredCount += 1
    return !shouldFilter
  })
  return {
    candidates: filtered,
    filteredCount,
    beforeCounts,
    afterCounts: countCandidatesByNetwork(filtered),
    enabled,
    lexicalThreshold,
  }
}

function compareHybridCandidates(a = {}, b = {}) {
  if (b.fusedScore !== a.fusedScore) return b.fusedScore - a.fusedScore
  if (b.sparseScoreNormalized !== a.sparseScoreNormalized) return b.sparseScoreNormalized - a.sparseScoreNormalized
  if (b.denseScoreNormalized !== a.denseScoreNormalized) return b.denseScoreNormalized - a.denseScoreNormalized
  if (b.rrfScore !== a.rrfScore) return b.rrfScore - a.rrfScore
  if (b.vectorScore !== a.vectorScore) return b.vectorScore - a.vectorScore
  if (b.lexicalScore !== a.lexicalScore) return b.lexicalScore - a.lexicalScore
  return String(a.offerId || '').localeCompare(String(b.offerId || ''))
}

function normalizeBrandEntityToken(value = '') {
  const normalized = cleanText(value).toLowerCase()
  if (!normalized) return ''
  if (BRAND_ENTITY_STOPWORDS.has(normalized)) return ''
  if (/^\d+$/.test(normalized)) return ''
  if (normalized.length < 3 && !/[\u4e00-\u9fff]{2,}/.test(normalized)) return ''
  return normalized
}

function normalizeBrandEntityTokens(tokens = []) {
  if (!Array.isArray(tokens)) return []
  const dedupe = new Set()
  const normalized = []
  for (const token of tokens) {
    const next = normalizeBrandEntityToken(token)
    if (!next) continue
    if (dedupe.has(next)) continue
    dedupe.add(next)
    normalized.push(next)
  }
  return normalized
}

function normalizeBrandComparable(value = '') {
  return String(value || '').toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]+/g, '')
}

function extractCandidateBrandMatchSignals(candidate = {}) {
  const metadata = candidate?.metadata && typeof candidate.metadata === 'object' ? candidate.metadata : {}
  const tags = Array.isArray(candidate?.tags) ? candidate.tags : []
  const rows = [
    candidate?.title,
    candidate?.description,
    metadata.brand,
    metadata.brandName,
    metadata.brand_name,
    metadata.brandId,
    metadata.brand_id,
    metadata.merchant,
    metadata.merchantName,
    metadata.merchant_name,
    ...tags,
  ]
    .map((item) => String(item || '').trim())
    .filter(Boolean)
  const corpus = rows.join(' ')
  return {
    tokenSet: new Set(tokenize(corpus)),
    normalizedCorpus: normalizeBrandComparable(corpus),
  }
}

function computeHouseShare(candidates = [], finalTopK = DEFAULT_FINAL_TOP_K) {
  const topSize = Math.max(1, toPositiveInteger(finalTopK, DEFAULT_FINAL_TOP_K))
  const topCandidates = (Array.isArray(candidates) ? candidates : []).slice(0, topSize)
  if (topCandidates.length <= 0) return 0
  const houseCount = topCandidates.filter((item) => cleanText(item?.network).toLowerCase() === 'house').length
  return round(houseCount / topSize)
}

function applyBrandIntentProtection(candidates = [], policy = {}) {
  const list = Array.isArray(candidates) ? candidates : []
  const finalTopK = toPositiveInteger(policy.finalTopK, DEFAULT_FINAL_TOP_K)
  const brandEntityTokens = normalizeBrandEntityTokens(policy.brandEntityTokens)
  const brandIntentDetected = brandEntityTokens.length > 0
  const partnerBrandMissPenalty = clamp01(policy.brandMissPenalty, DEFAULT_PARTNER_BRAND_MISS_PENALTY)
  const houseShareCap = clamp01(policy.houseShareCap, DEFAULT_HOUSE_SHARE_CAP)
  const brandTokenSet = new Set(brandEntityTokens)
  const penaltiesApplied = []

  const withBrandHits = list.map((candidate) => {
    const candidateSignals = extractCandidateBrandMatchSignals(candidate)
    let brandEntityHitCount = 0
    for (const token of brandTokenSet) {
      const normalizedToken = normalizeBrandComparable(token)
      if (!normalizedToken) continue
      const directTokenMatched = candidateSignals.tokenSet.has(token)
      const normalizedMatched = candidateSignals.normalizedCorpus.includes(normalizedToken)
      if (directTokenMatched || normalizedMatched) brandEntityHitCount += 1
    }
    return {
      ...candidate,
      brandEntityHitCount,
      penaltyApplied: 0,
      eliminationReason: String(candidate?.eliminationReason || '').trim(),
    }
  })

  if (!brandIntentDetected) {
    return {
      candidates: [...withBrandHits].sort(compareHybridCandidates),
      brandIntentDetected,
      brandEntityTokens,
      penaltiesApplied,
      houseShareBeforeCap: computeHouseShare(withBrandHits, finalTopK),
      houseShareAfterCap: computeHouseShare(withBrandHits, finalTopK),
      brandIntentBlockedNoHit: false,
    }
  }

  const penalized = withBrandHits.map((candidate) => {
    const network = cleanText(candidate?.network).toLowerCase()
    if (candidate.brandEntityHitCount > 0) {
      return candidate
    }
    let penaltyApplied = 0
    let penaltyType = ''
    if (network === 'house') {
      const dynamicPenalty = Math.max(
        DEFAULT_HOUSE_BRAND_MISS_MIN_PENALTY,
        toFiniteNumber(candidate?.fusedScore, 0) * DEFAULT_HOUSE_BRAND_MISS_DYNAMIC_RATIO,
      )
      penaltyApplied = round(Math.min(DEFAULT_HOUSE_BRAND_MISS_MAX_PENALTY, dynamicPenalty))
      penaltyType = 'house_brand_miss_penalty'
    } else if (network === 'partnerstack' || network === 'cj') {
      penaltyApplied = round(Math.max(DEFAULT_PARTNER_BRAND_MISS_PENALTY, partnerBrandMissPenalty))
      penaltyType = `${network}_brand_miss_penalty`
    }
    if (penaltyApplied <= 0) return candidate
    const nextFusedScore = round(Math.max(0, toFiniteNumber(candidate?.fusedScore, 0) - penaltyApplied))
    penaltiesApplied.push({
      offerId: cleanText(candidate?.offerId),
      type: penaltyType || 'brand_miss_penalty',
      amount: penaltyApplied,
    })
    return {
      ...candidate,
      penaltyApplied,
      fusedScore: nextFusedScore,
    }
  })

  const sorted = [...penalized].sort(compareHybridCandidates)
  const houseShareBeforeCap = computeHouseShare(sorted, finalTopK)

  const hasPartnerstackBrandHit = sorted.some((candidate) => (
    cleanText(candidate?.network).toLowerCase() === 'partnerstack'
    && toPositiveInteger(candidate?.brandEntityHitCount, 0) > 0
  ))

  if (!hasPartnerstackBrandHit) {
    return {
      candidates: sorted,
      brandIntentDetected,
      brandEntityTokens,
      penaltiesApplied,
      houseShareBeforeCap,
      houseShareAfterCap: computeHouseShare(sorted, finalTopK),
      brandIntentBlockedNoHit: false,
    }
  }

  const maxHouseInTopK = Math.max(0, Math.floor(Math.max(1, finalTopK) * houseShareCap))
  let houseCount = 0
  const kept = []
  const deferredHouse = []

  for (const candidate of sorted) {
    const isHouse = cleanText(candidate?.network).toLowerCase() === 'house'
    if (isHouse && houseCount >= maxHouseInTopK) {
      deferredHouse.push({
        ...candidate,
        eliminationReason: candidate.eliminationReason || 'house_share_cap_demoted',
      })
      continue
    }
    if (isHouse) houseCount += 1
    kept.push(candidate)
  }

  const reshuffled = [...kept]
  return {
    candidates: reshuffled,
    brandIntentDetected,
    brandEntityTokens,
    penaltiesApplied,
    houseShareBeforeCap,
    houseShareAfterCap: computeHouseShare(reshuffled, finalTopK),
    brandIntentBlockedNoHit: false,
  }
}

function tokenize(value = '') {
  return cleanText(value)
    .toLowerCase()
    .split(/[^a-z0-9\u4e00-\u9fff]+/g)
    .filter((item) => item.length >= 2)
}

function extractUrlTokens(url = '') {
  const raw = cleanText(url)
  if (!raw) return []
  let parsed
  try {
    parsed = new URL(raw)
  } catch {
    return []
  }
  const hostTokens = String(parsed.hostname || '')
    .toLowerCase()
    .split(/[^a-z0-9]+/g)
    .filter((item) => item.length >= 2)
  const pathTokens = String(parsed.pathname || '')
    .toLowerCase()
    .split(/[^a-z0-9]+/g)
    .filter((item) => item.length >= 2)
  return Array.from(new Set([...hostTokens, ...pathTokens]))
}

function collectMetadataSegments(value, out = [], dedupe = new Set(), depth = 0, limit = 64) {
  if (out.length >= limit || depth > 2) return
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    const text = cleanText(value)
    if (!text) return
    const key = text.toLowerCase()
    if (dedupe.has(key)) return
    dedupe.add(key)
    out.push(text)
    return
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      if (out.length >= limit) break
      collectMetadataSegments(item, out, dedupe, depth + 1, limit)
    }
    return
  }
  if (!value || typeof value !== 'object') return
  for (const [key, nested] of Object.entries(value)) {
    if (out.length >= limit) break
    if (String(key || '').toLowerCase() === 'retrievaltext') continue
    collectMetadataSegments(nested, out, dedupe, depth + 1, limit)
  }
}

function buildCandidateRetrievalText(candidate = {}) {
  const metadata = candidate?.metadata && typeof candidate.metadata === 'object' ? candidate.metadata : {}
  if (cleanText(candidate?.retrievalText)) return cleanText(candidate.retrievalText)
  if (cleanText(metadata.retrievalText)) return cleanText(metadata.retrievalText)

  const tags = Array.isArray(candidate?.tags) ? candidate.tags : []
  const metadataDirect = [
    metadata.brand,
    metadata.brandName,
    metadata.brand_name,
    metadata.brandId,
    metadata.brand_id,
    metadata.merchant,
    metadata.merchantName,
    metadata.merchant_name,
    metadata.productName,
    metadata.product_name,
    metadata.category,
    metadata.verticalL1,
    metadata.vertical_l1,
    metadata.verticalL2,
    metadata.vertical_l2,
    metadata.useCase,
    metadata.use_case,
    metadata.solution,
  ]
  const nested = []
  collectMetadataSegments(metadata, nested, new Set(), 0, 64)
  const segments = [
    candidate?.title,
    candidate?.description,
    ...tags,
    ...metadataDirect,
    ...nested,
    ...extractUrlTokens(candidate?.targetUrl),
  ]
    .map((item) => cleanText(item))
    .filter(Boolean)
  return cleanText(Array.from(new Set(segments)).join(' '))
}

function computeQueryTokenMatches(candidate = {}, queryTokens = []) {
  if (!Array.isArray(queryTokens) || queryTokens.length <= 0) return []
  const candidateTokens = new Set(tokenizeTopic(buildCandidateRetrievalText(candidate)))
  return Array.from(new Set(queryTokens.filter((token) => candidateTokens.has(token))))
}

function tokenizeTopic(value = '') {
  return tokenize(value)
    .filter((token) => token.length >= 3)
    .filter((token) => !BRAND_ENTITY_STOPWORDS.has(token))
}

function overlapScore(queryTokens = [], candidateTokens = []) {
  if (!Array.isArray(queryTokens) || queryTokens.length === 0) return 0
  if (!Array.isArray(candidateTokens) || candidateTokens.length === 0) return 0
  const querySet = new Set(queryTokens)
  const candidateSet = new Set(candidateTokens)
  let hit = 0
  for (const token of querySet) {
    if (candidateSet.has(token)) hit += 1
  }
  return hit / querySet.size
}

function computeTopicCoverageScore(candidate = {}, queryTokens = []) {
  if (!Array.isArray(queryTokens) || queryTokens.length <= 0) return 0
  const candidateTokens = tokenizeTopic(buildCandidateRetrievalText(candidate))
  return round(overlapScore(queryTokens, candidateTokens))
}

function normalizeQuality(value) {
  const n = toFiniteNumber(value, 0)
  if (n <= 0) return 0
  if (n <= 1) return Math.min(1, n)
  return Math.min(1, n / 100)
}

function toFallbackCandidate(offer = {}, query = '') {
  const metadata = offer?.metadata && typeof offer.metadata === 'object' ? offer.metadata : {}
  const normalizedMetadata = { ...metadata }
  const merchantName = cleanText(offer.merchantName)
  if (!cleanText(normalizedMetadata.merchant) && merchantName) {
    normalizedMetadata.merchant = merchantName
  }
  if (!cleanText(normalizedMetadata.merchantName) && merchantName) {
    normalizedMetadata.merchantName = merchantName
  }
  const tags = Array.isArray(metadata.matchTags)
    ? metadata.matchTags
    : (Array.isArray(metadata.tags) ? metadata.tags : [])
  const title = cleanText(offer.title)
  const description = cleanText(offer.description)
  const corpus = buildCandidateRetrievalText({
    title,
    description,
    targetUrl: cleanText(offer.targetUrl || offer.trackingUrl),
    tags,
    metadata: normalizedMetadata,
  })
  const queryTokens = tokenize(query)
  const candidateTokens = tokenize(corpus)
  const lexicalScore = overlapScore(queryTokens, candidateTokens)
  const quality = normalizeQuality(offer.qualityScore)
  const bidHint = Math.max(0, toFiniteNumber(offer.bidValue, 0))
  const bidBoost = bidHint > 0 ? Math.min(0.2, bidHint / 100) : 0
  const vectorScore = Math.min(1, lexicalScore * 0.8 + quality * 0.2)
  const fusedScore = Math.min(1, lexicalScore * 0.55 + vectorScore * 0.35 + bidBoost * 0.1)

  return {
    offerId: cleanText(offer.offerId),
    network: cleanText(offer.sourceNetwork || metadata.sourceNetwork || ''),
    upstreamOfferId: cleanText(offer.sourceId),
    title,
    description,
    targetUrl: cleanText(offer.targetUrl || offer.trackingUrl),
    market: cleanText(offer.market || 'US'),
    language: cleanText(offer.locale || 'en-US'),
    availability: cleanText(offer.availability || 'active') || 'active',
    quality: toFiniteNumber(offer.qualityScore, 0),
    bidHint,
    policyWeight: toFiniteNumber(normalizedMetadata.policyWeight, 0),
    freshnessAt: cleanText(offer.updatedAt),
    tags,
    metadata: normalizedMetadata,
    updatedAt: cleanText(offer.updatedAt),
    retrievalText: corpus,
    lexicalScore: toFiniteNumber(lexicalScore, 0),
    vectorScore: toFiniteNumber(vectorScore, 0),
    fusedScore: toFiniteNumber(fusedScore, 0),
  }
}

function createFallbackCandidatesFromOffers(offers = [], input = {}) {
  const normalized = normalizeUnifiedOffers(offers)
  const query = cleanText(input.query)
  const filters = normalizeFilters(input.filters)
  const languageFilter = resolveLanguageFilter(
    filters.language,
    normalizeLocaleMatchMode(input.languageMatchMode),
  )

  const candidates = normalized
    .map((offer) => toFallbackCandidate(offer, query))
    .filter((item) => item.offerId && item.title && item.targetUrl)
    .filter((item) => cleanText(item.availability || 'active').toLowerCase() === 'active')
    .filter((item) => {
      if (filters.networks.length > 0 && !filters.networks.includes(cleanText(item.network).toLowerCase())) return false
      if (filters.market && cleanText(item.market).toUpperCase() !== filters.market) return false
      if (!matchesLanguageWithMode(item.language, languageFilter)) return false
      return true
    })
    .sort((a, b) => {
      if (b.fusedScore !== a.fusedScore) return b.fusedScore - a.fusedScore
      if (b.lexicalScore !== a.lexicalScore) return b.lexicalScore - a.lexicalScore
      if (b.bidHint !== a.bidHint) return b.bidHint - a.bidHint
      return a.offerId.localeCompare(b.offerId)
    })
    .map((candidate, index) => ({
      ...candidate,
      lexicalRank: index + 1,
      vectorRank: index + 1,
    }))

  return candidates
}

async function fetchBm25Candidates(pool, query, filters = {}, topK = DEFAULT_LEXICAL_TOP_K, policy = {}) {
  const trimmedQuery = cleanText(query)
  if (!trimmedQuery) return []

  const normalizedFilters = normalizeFilters(filters)
  const languageFilter = resolveLanguageFilter(
    normalizedFilters.language,
    normalizeLocaleMatchMode(policy.languageMatchMode),
  )
  try {
    return await queryBm25Candidates({
      pool,
      query: trimmedQuery,
      filters: normalizedFilters,
      acceptedLanguages: languageFilter.accepted,
      topK: toPositiveInteger(topK, DEFAULT_LEXICAL_TOP_K),
      refreshIntervalMs: toPositiveInteger(policy.bm25RefreshIntervalMs, DEFAULT_BM25_REFRESH_INTERVAL_MS),
      coldStartWaitMs: toPositiveInteger(policy.bm25ColdStartWaitMs, DEFAULT_BM25_COLD_START_WAIT_MS),
      k1: 1.2,
      b: 0.75,
    })
  } catch {
    return []
  }
}

async function fetchVectorCandidates(pool, query, filters = {}, topK = DEFAULT_VECTOR_TOP_K, policy = {}) {
  const trimmedQuery = cleanText(query)
  if (!trimmedQuery) return []

  const embedding = buildQueryEmbedding(trimmedQuery)
  const normalizedFilters = normalizeFilters(filters)
  const languageFilter = resolveLanguageFilter(
    normalizedFilters.language,
    normalizeLocaleMatchMode(policy.languageMatchMode),
  )

  const sql = `
    SELECT
      n.offer_id,
      n.network,
      n.upstream_offer_id,
      n.title,
      n.description,
      n.target_url,
      n.market,
      n.language,
      n.availability,
      n.quality,
      n.bid_hint,
      n.policy_weight,
      n.freshness_at,
      n.tags,
      n.metadata,
      n.updated_at,
      1 - (e.embedding <=> $1::vector) AS vector_score
    FROM offer_inventory_embeddings e
    INNER JOIN offer_inventory_norm n ON n.offer_id = e.offer_id
    WHERE n.availability = 'active'
      AND ($2::text[] IS NULL OR n.network = ANY($2::text[]))
      AND ($3::text IS NULL OR upper(n.market) = upper($3::text))
      AND ($4::text[] IS NULL OR lower(n.language) = ANY($4::text[]))
    ORDER BY e.embedding <=> $1::vector ASC
    LIMIT $5
  `

  const result = await pool.query(sql, [
    vectorToSqlLiteral(embedding.vector),
    normalizedFilters.networks.length > 0 ? normalizedFilters.networks : null,
    normalizedFilters.market || null,
    languageFilter.accepted.length > 0 ? languageFilter.accepted : null,
    toPositiveInteger(topK, DEFAULT_VECTOR_TOP_K),
  ])

  return Array.isArray(result.rows) ? result.rows : []
}

function mergeCandidate(base = {}, override = {}) {
  const merged = {
    offerId: cleanText(override.offer_id || base.offerId),
    network: cleanText(override.network || base.network),
    upstreamOfferId: cleanText(override.upstream_offer_id || base.upstreamOfferId),
    title: cleanText(override.title || base.title),
    description: cleanText(override.description || base.description),
    targetUrl: cleanText(override.target_url || base.targetUrl),
    market: cleanText(override.market || base.market),
    language: cleanText(override.language || base.language),
    availability: cleanText(override.availability || base.availability),
    quality: toFiniteNumber(override.quality ?? base.quality, 0),
    bidHint: toFiniteNumber(override.bid_hint ?? base.bidHint, 0),
    policyWeight: toFiniteNumber(override.policy_weight ?? base.policyWeight, 0),
    freshnessAt: cleanText(override.freshness_at || base.freshnessAt || override.updated_at || base.updatedAt),
    tags: Array.isArray(override.tags)
      ? override.tags
      : (Array.isArray(base.tags) ? base.tags : []),
    metadata: override.metadata && typeof override.metadata === 'object'
      ? override.metadata
      : (base.metadata && typeof base.metadata === 'object' ? base.metadata : {}),
    updatedAt: cleanText(override.updated_at || base.updatedAt),
    lexicalScore: toFiniteNumber(override.lexical_score ?? override.bm25_raw ?? base.lexicalScore, 0),
    bm25Raw: toFiniteNumber(override.bm25_raw ?? override.lexical_score ?? base.bm25Raw ?? base.lexicalScore, 0),
    vectorScore: toFiniteNumber(override.vector_score ?? base.vectorScore, 0),
    fusedScore: toFiniteNumber(override.fusedScore ?? base.fusedScore, 0),
    rrfScore: toFiniteNumber(override.rrfScore ?? base.rrfScore, 0),
    lexicalRank: toPositiveInteger(override.lexicalRank ?? base.lexicalRank, 0),
    vectorRank: toPositiveInteger(override.vectorRank ?? base.vectorRank, 0),
  }
  return {
    ...merged,
    retrievalText: cleanText(
      override.retrieval_text
      || base.retrievalText
      || merged?.metadata?.retrievalText,
    ) || buildCandidateRetrievalText(merged),
  }
}

function rrfFuse(lexicalRows = [], vectorRows = [], options = {}) {
  const k = Math.max(1, toPositiveInteger(options.rrfK, DEFAULT_RRF_K))
  const merged = new Map()

  lexicalRows.forEach((row, index) => {
    const offerId = cleanText(row?.offer_id)
    if (!offerId) return
    const rank = index + 1
    const current = merged.get(offerId) || {}
    const nextRrfScore = toFiniteNumber(current.rrfScore ?? current.fusedScore, 0) + (1 / (k + rank))
    const next = mergeCandidate(current, {
      ...row,
      lexicalRank: rank,
      rrfScore: nextRrfScore,
      fusedScore: nextRrfScore,
    })
    merged.set(offerId, next)
  })

  vectorRows.forEach((row, index) => {
    const offerId = cleanText(row?.offer_id)
    if (!offerId) return
    const rank = index + 1
    const current = merged.get(offerId) || {}
    const nextRrfScore = toFiniteNumber(current.rrfScore ?? current.fusedScore, 0) + (1 / (k + rank))
    const next = mergeCandidate(current, {
      ...row,
      vectorRank: rank,
      rrfScore: nextRrfScore,
      fusedScore: nextRrfScore,
    })
    merged.set(offerId, next)
  })

  return [...merged.values()].sort((a, b) => {
    if (b.fusedScore !== a.fusedScore) return b.fusedScore - a.fusedScore
    if (b.vectorScore !== a.vectorScore) return b.vectorScore - a.vectorScore
    if (b.lexicalScore !== a.lexicalScore) return b.lexicalScore - a.lexicalScore
    return a.offerId.localeCompare(b.offerId)
  })
}

function normalizeHybridWeights(input = {}) {
  const sparseRaw = toFiniteNumber(input.sparseWeight, Number.NaN)
  const denseRaw = toFiniteNumber(input.denseWeight, Number.NaN)
  let sparseWeight = Number.isFinite(sparseRaw) && sparseRaw >= 0
    ? sparseRaw
    : DEFAULT_HYBRID_SPARSE_WEIGHT
  let denseWeight = Number.isFinite(denseRaw) && denseRaw >= 0
    ? denseRaw
    : DEFAULT_HYBRID_DENSE_WEIGHT

  if (sparseWeight + denseWeight <= 0) {
    sparseWeight = DEFAULT_HYBRID_SPARSE_WEIGHT
    denseWeight = DEFAULT_HYBRID_DENSE_WEIGHT
  }

  const totalWeight = sparseWeight + denseWeight
  if (!Number.isFinite(totalWeight) || totalWeight <= 0) {
    return {
      sparseWeight: DEFAULT_HYBRID_SPARSE_WEIGHT,
      denseWeight: DEFAULT_HYBRID_DENSE_WEIGHT,
    }
  }

  return {
    sparseWeight: round(sparseWeight / totalWeight),
    denseWeight: round(denseWeight / totalWeight),
  }
}

function applyHybridLinearFusion(candidates = [], policy = {}) {
  const list = Array.isArray(candidates) ? candidates : []
  const weights = normalizeHybridWeights({
    sparseWeight: policy.sparseWeight,
    denseWeight: policy.denseWeight,
  })

  let sparseMin = Number.POSITIVE_INFINITY
  let sparseMax = Number.NEGATIVE_INFINITY
  let denseMin = Number.POSITIVE_INFINITY
  let denseMax = Number.NEGATIVE_INFINITY

  for (const candidate of list) {
    const sparseRaw = toFiniteNumber(candidate?.bm25Raw ?? candidate?.lexicalScore, 0)
    const denseRaw = toFiniteNumber(candidate?.vectorScore, 0)
    sparseMin = Math.min(sparseMin, sparseRaw)
    sparseMax = Math.max(sparseMax, sparseRaw)
    denseMin = Math.min(denseMin, denseRaw)
    denseMax = Math.max(denseMax, denseRaw)
  }

  if (!Number.isFinite(sparseMin)) sparseMin = 0
  if (!Number.isFinite(sparseMax)) sparseMax = 0
  if (!Number.isFinite(denseMin)) denseMin = 0
  if (!Number.isFinite(denseMax)) denseMax = 0

  const sparseRange = sparseMax - sparseMin
  const scoredCandidates = list.map((candidate) => {
    const sparseRaw = toFiniteNumber(candidate?.bm25Raw ?? candidate?.lexicalScore, 0)
    const denseRaw = toFiniteNumber(candidate?.vectorScore, 0)
    const sparseScoreNormalized = sparseRange > 0
      ? clamp01((sparseRaw - sparseMin) / sparseRange, 0)
      : (sparseRaw > 0 ? 1 : 0)
    const denseScoreNormalized = clamp01((denseRaw + 1) / 2, 0)
    const rrfScore = toFiniteNumber(candidate?.rrfScore ?? candidate?.fusedScore, 0)
    const fusedScore = round(
      sparseScoreNormalized * weights.sparseWeight
      + denseScoreNormalized * weights.denseWeight,
    )

    return {
      ...candidate,
      bm25Raw: sparseRaw,
      sparseScoreNormalized,
      denseScoreNormalized,
      rrfScore,
      fusedScore,
      penaltyApplied: toFiniteNumber(candidate?.penaltyApplied, 0),
      eliminationReason: String(candidate?.eliminationReason || '').trim(),
    }
  })

  scoredCandidates.sort(compareHybridCandidates)

  return {
    candidates: scoredCandidates,
    stats: {
      sparseMin: round(sparseMin),
      sparseMax: round(sparseMax),
      denseMin: round(denseMin),
      denseMax: round(denseMax),
    },
    weights,
  }
}

function buildDebugOptions(candidates = [], options = {}) {
  const limit = Math.max(1, toPositiveInteger(options.limit, 20))
  const queryTokens = Array.isArray(options.queryTokens)
    ? options.queryTokens.map((item) => cleanText(item).toLowerCase()).filter(Boolean)
    : []
  return (Array.isArray(candidates) ? candidates : [])
    .slice(0, limit)
    .map((candidate) => {
      const queryTokensMatched = computeQueryTokenMatches(candidate, queryTokens).slice(0, 12)
      return {
        offerId: cleanText(candidate?.offerId),
        network: cleanText(candidate?.network).toLowerCase(),
        lexicalScore: round(toFiniteNumber(candidate?.lexicalScore, 0)),
        bm25Raw: round(toFiniteNumber(candidate?.bm25Raw, 0)),
        vectorScore: round(toFiniteNumber(candidate?.vectorScore, 0)),
        fusedScore: round(toFiniteNumber(candidate?.fusedScore, 0)),
        sparseScoreNormalized: round(toFiniteNumber(candidate?.sparseScoreNormalized, 0)),
        denseScoreNormalized: round(toFiniteNumber(candidate?.denseScoreNormalized, 0)),
        rrfScore: round(toFiniteNumber(candidate?.rrfScore, 0)),
        topicCoverageScore: round(toFiniteNumber(candidate?.topicCoverageScore, 0)),
        brandEntityHitCount: toPositiveInteger(candidate?.brandEntityHitCount, 0),
        penaltyApplied: round(toFiniteNumber(candidate?.penaltyApplied, 0)),
        eliminationReason: cleanText(candidate?.eliminationReason),
        retrievalTextPreview: clipText(buildCandidateRetrievalText(candidate), 180),
        queryTokensMatched,
        bm25TermHits: queryTokensMatched,
      }
    })
}

function normalizeHybridStrategy(value) {
  const strategy = cleanText(value).toLowerCase()
  if (!strategy) return DEFAULT_HYBRID_STRATEGY
  if (strategy !== DEFAULT_HYBRID_STRATEGY) return DEFAULT_HYBRID_STRATEGY
  return strategy
}

export async function retrieveOpportunityCandidates(input = {}, options = {}) {
  const startedAt = Date.now()
  const query = cleanText(input.query)
  const semanticQuery = cleanText(input.semanticQuery || input.vectorQuery || query)
  const sparseQuery = cleanText(input.sparseQuery || input.lexicalQuery || query || semanticQuery)
  const topicQuery = cleanText(input.topicQuery || semanticQuery || query || sparseQuery)
  const topicCoverageThreshold = clamp01(input.topicCoverageThreshold, DEFAULT_TOPIC_COVERAGE_THRESHOLD)
  const topicSignalTokens = tokenizeTopic(topicQuery)
  const sparseQueryTokens = Array.from(new Set(
    tokenizeTopic(sparseQuery || semanticQuery || query),
  )).slice(0, 24)
  const queryForFallback = sparseQuery || semanticQuery || query
  const queryMode = cleanText(input.queryMode).toLowerCase() || 'raw_query'
  const contextWindowMode = cleanText(input.contextWindowMode).toLowerCase() || 'latest_turn_only'
  const assistantEntityTokensRaw = Array.isArray(input.assistantEntityTokensRaw)
    ? input.assistantEntityTokensRaw.map((item) => cleanText(item).toLowerCase()).filter(Boolean)
    : []
  const assistantEntityTokensFiltered = Array.isArray(input.assistantEntityTokensFiltered)
    ? input.assistantEntityTokensFiltered.map((item) => cleanText(item).toLowerCase()).filter(Boolean)
    : []
  const filters = normalizeFilters(input.filters)
  const languageMatchMode = normalizeLocaleMatchMode(input.languageMatchMode)
  const languageResolved = resolveLanguageFilter(filters.language, languageMatchMode)
  const rrfK = Math.max(1, toPositiveInteger(input.rrfK, DEFAULT_RRF_K))
  const hybridStrategy = normalizeHybridStrategy(input.hybridStrategy)
  const hybridWeights = normalizeHybridWeights({
    sparseWeight: input.hybridSparseWeight ?? input.hybridSparse ?? input.sparseWeight,
    denseWeight: input.hybridDenseWeight ?? input.hybridDense ?? input.denseWeight,
  })
  const houseLowInfoFilterEnabled = parseBoolean(
    input.houseLowInfoFilterEnabled,
    DEFAULT_HOUSE_LOWINFO_FILTER_ENABLED,
  )
  const houseLowInfoLexicalThreshold = clamp01(
    input.minLexicalScore,
    DEFAULT_INTENT_MIN_LEXICAL_SCORE,
  )
  const lexicalTopK = toPositiveInteger(input.lexicalTopK, DEFAULT_LEXICAL_TOP_K)
  const vectorTopK = toPositiveInteger(input.vectorTopK, DEFAULT_VECTOR_TOP_K)
  const finalTopK = toPositiveInteger(input.finalTopK, DEFAULT_FINAL_TOP_K)
  const bm25RefreshIntervalMs = toPositiveInteger(
    input.bm25RefreshIntervalMs,
    DEFAULT_BM25_REFRESH_INTERVAL_MS,
  )
  const brandEntityTokens = normalizeBrandEntityTokens(input.brandEntityTokens)
  const brandMissPenalty = clamp01(input.brandMissPenalty, DEFAULT_BRAND_MISS_PENALTY)
  const houseShareCap = clamp01(input.houseShareCap, DEFAULT_HOUSE_SHARE_CAP)
  const emptyNetworkCounts = { partnerstack: 0, cj: 0, house: 0 }
  const emptyScoreStats = {
    sparseMin: 0,
    sparseMax: 0,
    denseMin: 0,
    denseMax: 0,
  }
  const scoringDebug = {
    strategy: hybridStrategy,
    sparseWeight: hybridWeights.sparseWeight,
    denseWeight: hybridWeights.denseWeight,
    sparseNormalization: 'min_max',
    denseNormalization: 'cosine_shift',
    rrfK,
  }
  const baseDebug = {
    filters,
    query: semanticQuery,
    queryMode,
    queryUsed: sparseQuery || semanticQuery,
    semanticQuery,
    sparseQuery,
    sparseQueryTokens,
    vectorInputTextPreview: clipText(semanticQuery, 220),
    topicQuery,
    topicCoverageThreshold,
    contextWindowMode,
    assistantEntityTokensRaw,
    assistantEntityTokensFiltered,
    languageMatchMode,
    languageResolved,
    fusionWeights: {
      sparse: hybridWeights.sparseWeight,
      dense: hybridWeights.denseWeight,
    },
    scoring: scoringDebug,
  }
  const buildDebug = (overrides = {}) => ({
    lexicalHitCount: 0,
    bm25HitCount: 0,
    vectorHitCount: 0,
    fusedHitCount: 0,
    networkCandidateCountsBeforeFilter: emptyNetworkCounts,
    networkCandidateCountsAfterFilter: emptyNetworkCounts,
    houseLowInfoFilteredCount: 0,
    scoreStats: emptyScoreStats,
    brandIntentDetected: false,
    brandEntityTokens: [],
    penaltiesApplied: [],
    houseShareBeforeCap: 0,
    houseShareAfterCap: 0,
    brandIntentBlockedNoHit: false,
    options: [],
    ...baseDebug,
    ...overrides,
    retrievalMs: Math.max(0, Date.now() - startedAt),
  })
  const pool = options.pool
  if (!pool) {
    const fallbackEnabled = options.enableFallbackWhenInventoryUnavailable !== false
    const fallbackProvider = typeof options.fallbackProvider === 'function'
      ? options.fallbackProvider
      : null
    if (fallbackEnabled && fallbackProvider) {
      try {
        const fallbackResult = await fallbackProvider({
          query: semanticQuery || queryForFallback,
          semanticQuery: semanticQuery || queryForFallback,
          sparseQuery: sparseQuery || queryForFallback,
          filters,
          lexicalTopK,
          vectorTopK,
          finalTopK,
        })
        const fallbackCandidates = Array.isArray(fallbackResult?.candidates)
          ? fallbackResult.candidates
          : createFallbackCandidatesFromOffers(
            Array.isArray(fallbackResult?.offers) ? fallbackResult.offers : [],
            {
              query: queryForFallback,
              filters,
              languageMatchMode,
            },
          )
        const hybridFallback = applyHybridLinearFusion(fallbackCandidates, hybridWeights)
        const lowInfoFiltered = applyHouseLowInfoFilter(hybridFallback.candidates, {
          enabled: houseLowInfoFilterEnabled,
          minLexicalScore: houseLowInfoLexicalThreshold,
        })
        const brandProtected = applyBrandIntentProtection(lowInfoFiltered.candidates, {
          brandEntityTokens,
          brandMissPenalty,
          houseShareCap,
          finalTopK,
        })
        const withTopicCoverage = brandProtected.candidates.map((candidate) => ({
          ...candidate,
          topicCoverageScore: computeTopicCoverageScore(candidate, topicSignalTokens),
        }))
        const sliced = withTopicCoverage.slice(0, finalTopK)
        if (sliced.length > 0) {
          return {
            candidates: sliced,
            debug: buildDebug({
              lexicalHitCount: hybridFallback.candidates
                .filter((item) => toFiniteNumber(item?.lexicalScore, 0) > 0)
                .length,
              bm25HitCount: hybridFallback.candidates
                .filter((item) => toFiniteNumber(item?.bm25Raw ?? item?.lexicalScore, 0) > 0)
                .length,
              vectorHitCount: hybridFallback.candidates
                .filter((item) => toFiniteNumber(item?.vectorScore, 0) > 0)
                .length,
              fusedHitCount: sliced.length,
              networkCandidateCountsBeforeFilter: lowInfoFiltered.beforeCounts,
              networkCandidateCountsAfterFilter: lowInfoFiltered.afterCounts,
              houseLowInfoFilteredCount: lowInfoFiltered.filteredCount,
              scoreStats: hybridFallback.stats,
              brandIntentDetected: brandProtected.brandIntentDetected,
              brandEntityTokens: brandProtected.brandEntityTokens,
              penaltiesApplied: brandProtected.penaltiesApplied,
              houseShareBeforeCap: brandProtected.houseShareBeforeCap,
              houseShareAfterCap: brandProtected.houseShareAfterCap,
              brandIntentBlockedNoHit: brandProtected.brandIntentBlockedNoHit,
              options: buildDebugOptions(withTopicCoverage, { limit: finalTopK, queryTokens: sparseQueryTokens }),
              mode: String(fallbackResult?.debug?.mode || 'connector_live_fallback'),
              fallbackMeta: fallbackResult?.debug && typeof fallbackResult.debug === 'object'
                ? fallbackResult.debug
                : {},
            }),
          }
        }
        return {
          candidates: [],
          debug: buildDebug({
            networkCandidateCountsBeforeFilter: lowInfoFiltered.beforeCounts,
            networkCandidateCountsAfterFilter: lowInfoFiltered.afterCounts,
            houseLowInfoFilteredCount: lowInfoFiltered.filteredCount,
            scoreStats: hybridFallback.stats,
            brandIntentDetected: brandProtected.brandIntentDetected,
            brandEntityTokens: brandProtected.brandEntityTokens,
            penaltiesApplied: brandProtected.penaltiesApplied,
            houseShareBeforeCap: brandProtected.houseShareBeforeCap,
            houseShareAfterCap: brandProtected.houseShareAfterCap,
            brandIntentBlockedNoHit: brandProtected.brandIntentBlockedNoHit,
            options: buildDebugOptions(withTopicCoverage, { limit: finalTopK, queryTokens: sparseQueryTokens }),
            mode: String(fallbackResult?.debug?.mode || 'connector_live_fallback_empty'),
            fallbackMeta: fallbackResult?.debug && typeof fallbackResult.debug === 'object'
              ? fallbackResult.debug
              : {},
          }),
        }
      } catch (error) {
        return {
          candidates: [],
          debug: buildDebug({
            mode: 'connector_live_fallback_error',
            fallbackError: error instanceof Error ? error.message : 'fallback_failed',
          }),
        }
      }
    }
    return {
      candidates: [],
      debug: buildDebug({
        mode: 'inventory_store_unavailable',
      }),
    }
  }

  const [lexicalRows, vectorRows] = await Promise.all([
    fetchBm25Candidates(pool, sparseQuery || queryForFallback, filters, lexicalTopK, {
      languageMatchMode,
      bm25RefreshIntervalMs,
    }),
    fetchVectorCandidates(pool, semanticQuery || queryForFallback, filters, vectorTopK, {
      languageMatchMode,
    }),
  ])

  const fused = rrfFuse(lexicalRows, vectorRows, {
    rrfK,
  })
  const hybrid = applyHybridLinearFusion(fused, {
    sparseWeight: hybridWeights.sparseWeight,
    denseWeight: hybridWeights.denseWeight,
  })
  const lowInfoFiltered = applyHouseLowInfoFilter(hybrid.candidates, {
    enabled: houseLowInfoFilterEnabled,
    minLexicalScore: houseLowInfoLexicalThreshold,
  })
  const brandProtected = applyBrandIntentProtection(lowInfoFiltered.candidates, {
    brandEntityTokens,
    brandMissPenalty,
    houseShareCap,
    finalTopK,
  })
  const withTopicCoverage = brandProtected.candidates.map((candidate) => ({
    ...candidate,
    topicCoverageScore: computeTopicCoverageScore(candidate, topicSignalTokens),
  }))
  const sliced = withTopicCoverage.slice(0, finalTopK)

  return {
    candidates: sliced,
    debug: buildDebug({
      lexicalHitCount: lexicalRows.length,
      bm25HitCount: lexicalRows.length,
      vectorHitCount: vectorRows.length,
      fusedHitCount: sliced.length,
      networkCandidateCountsBeforeFilter: lowInfoFiltered.beforeCounts,
      networkCandidateCountsAfterFilter: lowInfoFiltered.afterCounts,
      houseLowInfoFilteredCount: lowInfoFiltered.filteredCount,
      scoreStats: hybrid.stats,
      brandIntentDetected: brandProtected.brandIntentDetected,
      brandEntityTokens: brandProtected.brandEntityTokens,
      penaltiesApplied: brandProtected.penaltiesApplied,
      houseShareBeforeCap: brandProtected.houseShareBeforeCap,
      houseShareAfterCap: brandProtected.houseShareAfterCap,
      brandIntentBlockedNoHit: brandProtected.brandIntentBlockedNoHit,
      options: buildDebugOptions(withTopicCoverage, { limit: finalTopK, queryTokens: sparseQueryTokens }),
    }),
  }
}

export { rrfFuse }
