const DEFAULT_BM25_K1 = 1.2
const DEFAULT_BM25_B = 0.75
const DEFAULT_BM25_REFRESH_INTERVAL_MS = 10 * 60 * 1000
const DEFAULT_BM25_TOP_K = 120
const DEFAULT_BM25_COLD_START_WAIT_MS = 120
const MAX_BM25_COLD_START_WAIT_MS = 5_000
const BM25_STOPWORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'been', 'being', 'but', 'by', 'can', 'do', 'for', 'from', 'had', 'has',
  'have', 'if', 'in', 'into', 'is', 'it', 'its', 'of', 'on', 'or', 'that', 'the', 'their', 'there', 'these', 'this',
  'those', 'to', 'was', 'were', 'will', 'with', 'you', 'your', 'about', 'what', 'which', 'when', 'where', 'who', 'how',
  'why', 'i', 'me', 'my', 'mine', 'we', 'our', 'ours', 'he', 'him', 'his', 'she', 'her', 'hers', 'they', 'them',
  'wants', 'want', 'need', 'needs', 'girlfriend', 'boyfriend', 'wife', 'husband', 'partner',
  'tool', 'tools', 'platform', 'platforms', 'solution', 'solutions', 'service', 'services',
  'category', 'categories', 'automated', 'easiest', 'easy', 'recommended', 'approach', 'approaches', 'method', 'methods',
  'breakdown', 'all', 'one', 'workflow', 'step', 'steps',
  '推荐', '比较', '对比', '价格', '优惠', '什么', '怎么', '可以', '帮我', '一下',
])

const bm25IndexState = {
  activeIndex: null,
  buildPromise: null,
  lastBuildAtMs: 0,
  lastBuildError: '',
  refreshTimer: null,
  refreshIntervalMs: DEFAULT_BM25_REFRESH_INTERVAL_MS,
  pool: null,
}
const BM25_BUILD_WAIT_TIMEOUT = Symbol('bm25_build_wait_timeout')

function cleanText(value) {
  return String(value || '').trim().replace(/\s+/g, ' ')
}

function toFiniteNumber(value, fallback = 0) {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  return n
}

function toPositiveInteger(value, fallback = 0) {
  const n = Number(value)
  if (!Number.isFinite(n) || n <= 0) return fallback
  return Math.floor(n)
}

function normalizeColdStartWaitMs(value) {
  const normalized = toPositiveInteger(value, DEFAULT_BM25_COLD_START_WAIT_MS)
  return Math.min(MAX_BM25_COLD_START_WAIT_MS, Math.max(1, normalized))
}

async function waitForIndexBuild(buildPromise, coldStartWaitMs) {
  if (!buildPromise || typeof buildPromise.then !== 'function') {
    return bm25IndexState.activeIndex
  }
  const waitMs = normalizeColdStartWaitMs(coldStartWaitMs)
  let timeout = null
  try {
    const indexOrTimeout = await Promise.race([
      buildPromise,
      new Promise((resolve) => {
        timeout = setTimeout(() => resolve(BM25_BUILD_WAIT_TIMEOUT), waitMs)
      }),
    ])
    return indexOrTimeout === BM25_BUILD_WAIT_TIMEOUT
      ? bm25IndexState.activeIndex
      : indexOrTimeout
  } finally {
    if (timeout) clearTimeout(timeout)
  }
}

function round(value, precision = 6) {
  const n = Number(value)
  if (!Number.isFinite(n)) return 0
  return Number(n.toFixed(precision))
}

function tokenize(value = '') {
  return cleanText(value)
    .toLowerCase()
    .split(/[^a-z0-9\u4e00-\u9fff]+/g)
    .map((item) => item.trim())
    .filter((item) => {
      if (!item) return false
      if (item.length < 2) return false
      if (BM25_STOPWORDS.has(item)) return false
      return true
    })
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

function buildMetadataCorpus(metadata = {}) {
  if (!metadata || typeof metadata !== 'object') return ''
  if (cleanText(metadata.retrievalText)) {
    return cleanText(metadata.retrievalText)
  }
  const fields = [
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
  return [...fields.map((item) => cleanText(item)), ...nested].filter(Boolean).join(' ')
}

function normalizeServingRow(row = {}) {
  const metadata = row?.metadata && typeof row.metadata === 'object' ? row.metadata : {}
  return {
    offer_id: cleanText(row.offer_id),
    network: cleanText(row.network).toLowerCase(),
    upstream_offer_id: cleanText(row.upstream_offer_id),
    title: cleanText(row.title),
    description: cleanText(row.description),
    target_url: cleanText(row.target_url),
    market: cleanText(row.market).toUpperCase(),
    language: cleanText(row.language).toLowerCase().replace(/_/g, '-'),
    availability: cleanText(row.availability || 'active').toLowerCase(),
    quality: toFiniteNumber(row.quality, 0),
    bid_hint: toFiniteNumber(row.bid_hint, 0),
    policy_weight: toFiniteNumber(row.policy_weight, 0),
    tags: Array.isArray(row.tags) ? row.tags : [],
    metadata,
    refreshed_at: cleanText(row.refreshed_at || row.updated_at),
    updated_at: cleanText(row.updated_at || row.refreshed_at),
  }
}

function buildDocumentCorpus(row = {}) {
  const tags = Array.isArray(row.tags) ? row.tags.join(' ') : ''
  const metadataText = buildMetadataCorpus(row.metadata)
  const urlText = extractUrlTokens(row.target_url).join(' ')
  return cleanText(`${row.title} ${row.description} ${tags} ${metadataText} ${urlText}`)
}

function toTimestampValue(value = '') {
  const stamp = Date.parse(cleanText(value))
  if (!Number.isFinite(stamp)) return 0
  return stamp
}

function compareByScoreAndStability(a = {}, b = {}) {
  if (b.lexical_score !== a.lexical_score) return b.lexical_score - a.lexical_score
  const timeA = toTimestampValue(a.updated_at || a.refreshed_at || a.freshness_at)
  const timeB = toTimestampValue(b.updated_at || b.refreshed_at || b.freshness_at)
  if (timeB !== timeA) return timeB - timeA
  return String(a.offer_id || '').localeCompare(String(b.offer_id || ''))
}

function normalizeAcceptedLanguages(value = []) {
  if (!Array.isArray(value)) return []
  return Array.from(new Set(
    value
      .map((item) => cleanText(item).toLowerCase().replace(/_/g, '-'))
      .filter(Boolean),
  ))
}

function normalizeNetworks(value = []) {
  if (!Array.isArray(value)) return []
  return Array.from(new Set(value.map((item) => cleanText(item).toLowerCase()).filter(Boolean)))
}

function matchesFilters(row = {}, filters = {}, acceptedLanguages = []) {
  const networks = normalizeNetworks(filters?.networks)
  if (networks.length > 0 && !networks.includes(cleanText(row.network).toLowerCase())) {
    return false
  }

  const market = cleanText(filters?.market).toUpperCase()
  if (market && cleanText(row.market).toUpperCase() !== market) {
    return false
  }

  const accepted = normalizeAcceptedLanguages(acceptedLanguages)
  if (accepted.length > 0) {
    const language = cleanText(row.language).toLowerCase().replace(/_/g, '-')
    if (!accepted.includes(language)) return false
  }
  return true
}

function buildInMemoryBm25Index(rows = [], options = {}) {
  const k1 = toFiniteNumber(options.k1, DEFAULT_BM25_K1)
  const b = toFiniteNumber(options.b, DEFAULT_BM25_B)
  const docs = new Map()
  const postings = new Map()
  let totalDocLength = 0

  for (const row of rows) {
    const normalized = normalizeServingRow(row)
    if (!normalized.offer_id) continue
    if (normalized.availability !== 'active') continue
    const tokens = tokenize(buildDocumentCorpus(normalized))
    const termFreq = new Map()
    for (const token of tokens) {
      termFreq.set(token, (termFreq.get(token) || 0) + 1)
    }

    const docLength = Math.max(0, tokens.length)
    totalDocLength += docLength
    docs.set(normalized.offer_id, {
      row: normalized,
      docLength,
      termFreq,
    })

    for (const [token, tf] of termFreq.entries()) {
      const current = postings.get(token) || []
      current.push({
        offerId: normalized.offer_id,
        tf,
      })
      postings.set(token, current)
    }
  }

  const docCount = docs.size
  const avgDocLength = docCount > 0 ? (totalDocLength / docCount) : 1
  const idf = new Map()
  for (const [token, list] of postings.entries()) {
    const df = list.length
    const raw = Math.log(1 + ((docCount - df + 0.5) / (df + 0.5)))
    idf.set(token, Number.isFinite(raw) && raw > 0 ? raw : 0)
  }

  return {
    docs,
    postings,
    idf,
    docCount,
    avgDocLength: avgDocLength > 0 ? avgDocLength : 1,
    params: {
      k1: Number.isFinite(k1) ? k1 : DEFAULT_BM25_K1,
      b: Number.isFinite(b) ? b : DEFAULT_BM25_B,
    },
    builtAt: new Date().toISOString(),
  }
}

async function fetchServingRows(pool) {
  const result = await pool.query(`
    SELECT
      offer_id,
      network,
      title,
      description,
      target_url,
      market,
      language,
      availability,
      quality,
      bid_hint,
      policy_weight,
      tags,
      metadata,
      refreshed_at
    FROM offer_inventory_serving_snapshot
    WHERE availability = 'active'
  `)
  return Array.isArray(result?.rows) ? result.rows : []
}

async function rebuildBm25Index(pool, options = {}) {
  if (!pool) {
    return bm25IndexState.activeIndex
  }
  const targetPool = pool || bm25IndexState.pool
  bm25IndexState.pool = targetPool
  if (!targetPool) return bm25IndexState.activeIndex
  if (bm25IndexState.buildPromise) return bm25IndexState.buildPromise

  const nextPromise = (async () => {
    try {
      const rows = await fetchServingRows(targetPool)
      const nextIndex = buildInMemoryBm25Index(rows, options)
      bm25IndexState.activeIndex = nextIndex
      bm25IndexState.lastBuildAtMs = Date.now()
      bm25IndexState.lastBuildError = ''
      return nextIndex
    } catch (error) {
      bm25IndexState.lastBuildError = error instanceof Error ? error.message : 'bm25_index_build_failed'
      if (bm25IndexState.activeIndex) return bm25IndexState.activeIndex
      throw error
    } finally {
      bm25IndexState.buildPromise = null
    }
  })()
  bm25IndexState.buildPromise = nextPromise
  return nextPromise
}

function ensureRefreshTimer(refreshIntervalMs = DEFAULT_BM25_REFRESH_INTERVAL_MS) {
  const normalizedInterval = toPositiveInteger(refreshIntervalMs, DEFAULT_BM25_REFRESH_INTERVAL_MS)
  if (bm25IndexState.refreshTimer && bm25IndexState.refreshIntervalMs === normalizedInterval) return
  if (bm25IndexState.refreshTimer) clearInterval(bm25IndexState.refreshTimer)
  bm25IndexState.refreshIntervalMs = normalizedInterval
  bm25IndexState.refreshTimer = setInterval(() => {
    if (!bm25IndexState.pool) return
    void rebuildBm25Index(bm25IndexState.pool, {
      k1: DEFAULT_BM25_K1,
      b: DEFAULT_BM25_B,
    })
  }, normalizedInterval)
  if (typeof bm25IndexState.refreshTimer.unref === 'function') {
    bm25IndexState.refreshTimer.unref()
  }
}

async function ensureBm25Index(pool, options = {}) {
  const refreshIntervalMs = toPositiveInteger(
    options.refreshIntervalMs,
    DEFAULT_BM25_REFRESH_INTERVAL_MS,
  )
  const coldStartWaitMs = normalizeColdStartWaitMs(options.coldStartWaitMs)
  ensureRefreshTimer(refreshIntervalMs)
  const poolChanged = Boolean(pool && pool !== bm25IndexState.pool)
  if (poolChanged) {
    bm25IndexState.pool = pool
    const buildPromise = rebuildBm25Index(pool, options)
    if (bm25IndexState.activeIndex) {
      return buildPromise
    }
    return waitForIndexBuild(buildPromise, coldStartWaitMs)
  }

  if (!bm25IndexState.activeIndex) {
    const buildPromise = rebuildBm25Index(pool || bm25IndexState.pool, options)
    return waitForIndexBuild(buildPromise, coldStartWaitMs)
  }

  const ageMs = Math.max(0, Date.now() - bm25IndexState.lastBuildAtMs)
  if (ageMs >= refreshIntervalMs && !bm25IndexState.buildPromise) {
    void rebuildBm25Index(pool || bm25IndexState.pool, options)
  }
  return bm25IndexState.activeIndex
}

export async function queryBm25Candidates(input = {}) {
  const pool = input?.pool || null
  if (!pool) return []

  const query = cleanText(input.query)
  if (!query) return []

  const topK = toPositiveInteger(input.topK, DEFAULT_BM25_TOP_K)
  const filters = input?.filters && typeof input.filters === 'object' ? input.filters : {}
  const acceptedLanguages = normalizeAcceptedLanguages(input.acceptedLanguages)
  const index = await ensureBm25Index(pool, {
    k1: toFiniteNumber(input.k1, DEFAULT_BM25_K1),
    b: toFiniteNumber(input.b, DEFAULT_BM25_B),
    refreshIntervalMs: toPositiveInteger(input.refreshIntervalMs, DEFAULT_BM25_REFRESH_INTERVAL_MS),
    coldStartWaitMs: toPositiveInteger(input.coldStartWaitMs, DEFAULT_BM25_COLD_START_WAIT_MS),
  })
  if (!index || index.docCount <= 0) return []

  const queryTokens = Array.from(new Set(tokenize(query))).filter((token) => index.idf.has(token))
  if (queryTokens.length <= 0) return []

  const scores = new Map()
  for (const token of queryTokens) {
    const postings = index.postings.get(token)
    if (!Array.isArray(postings) || postings.length <= 0) continue
    const idf = toFiniteNumber(index.idf.get(token), 0)
    if (idf <= 0) continue
    for (const posting of postings) {
      const doc = index.docs.get(posting.offerId)
      if (!doc?.row) continue
      if (!matchesFilters(doc.row, filters, acceptedLanguages)) continue
      const tf = Math.max(0, toFiniteNumber(posting.tf, 0))
      if (tf <= 0) continue
      const lengthRatio = doc.docLength > 0
        ? (doc.docLength / Math.max(1, index.avgDocLength))
        : 1
      const denominator = tf + index.params.k1 * (1 - index.params.b + index.params.b * lengthRatio)
      if (denominator <= 0) continue
      const partial = idf * ((tf * (index.params.k1 + 1)) / denominator)
      scores.set(posting.offerId, toFiniteNumber(scores.get(posting.offerId), 0) + partial)
    }
  }

  const scoredRows = [...scores.entries()]
    .map(([offerId, score]) => {
      const doc = index.docs.get(offerId)
      if (!doc?.row) return null
      const row = doc.row
      return {
        ...row,
        freshness_at: row.refreshed_at,
        lexical_score: round(score),
        bm25_raw: round(score),
      }
    })
    .filter(Boolean)
    .sort(compareByScoreAndStability)
    .slice(0, topK)

  return scoredRows
}

export function getBm25IndexStatus() {
  return {
    ready: Boolean(bm25IndexState.activeIndex),
    docCount: toPositiveInteger(bm25IndexState.activeIndex?.docCount, 0),
    lastBuildAtMs: bm25IndexState.lastBuildAtMs,
    lastBuildError: bm25IndexState.lastBuildError,
    refreshIntervalMs: bm25IndexState.refreshIntervalMs,
  }
}

export function resetBm25IndexCache() {
  if (bm25IndexState.refreshTimer) {
    clearInterval(bm25IndexState.refreshTimer)
  }
  bm25IndexState.activeIndex = null
  bm25IndexState.buildPromise = null
  bm25IndexState.lastBuildAtMs = 0
  bm25IndexState.lastBuildError = ''
  bm25IndexState.refreshTimer = null
  bm25IndexState.refreshIntervalMs = DEFAULT_BM25_REFRESH_INTERVAL_MS
  bm25IndexState.pool = null
}
