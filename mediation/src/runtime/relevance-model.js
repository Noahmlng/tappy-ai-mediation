const DEFAULT_THRESHOLD_VERSION = 'v1_default_2026_03_01'
const DEFAULT_THRESHOLDS_BY_PLACEMENT = Object.freeze({
  chat_intent_recommendation_v1: { strict: 0.5, relaxed: 0.38 },
  chat_from_answer_v1: { strict: 0.58, relaxed: 0.44 },
})
const DEFAULT_FALLBACK_THRESHOLDS = Object.freeze({ strict: 0.6, relaxed: 0.46 })

const COMPONENT_WEIGHTS = Object.freeze({
  topic: 0.45,
  entity: 0.25,
  intentFit: 0.2,
  qualitySupport: 0.1,
})

const VERTICAL_FAMILIES = Object.freeze({
  electronics: 'shopping_goods',
  fashion: 'shopping_goods',
  beauty: 'shopping_goods',
  home: 'shopping_goods',
  travel: 'travel',
  software: 'digital_service',
  finance: 'finance',
  general: 'general',
})

const INTENT_SIGNAL_TOKENS = new Set([
  'buy', 'best', 'deal', 'deals', 'coupon', 'discount', 'price', 'pricing', 'recommend', 'compare',
  'review', 'reviews', 'subscription', 'shopping', 'gift', 'purchase',
  '买', '购买', '推荐', '对比', '比较', '价格', '折扣', '优惠', '哪个好', '测评', '评测',
])
const ENTITY_STOPWORDS = new Set([
  'the', 'and', 'for', 'with', 'from', 'that', 'this', 'have', 'will', 'your', 'about', 'which',
  'best', 'compare', 'recommend', 'price', 'deal', 'deals', 'review', 'camera', 'cameras',
  '推荐', '对比', '比较', '价格', '哪个好', '评测', '测评', '相机',
])

const QUERY_VERTICAL_RULES = Object.freeze([
  {
    vertical: 'electronics',
    keywords: [
      'camera', 'cameras', 'dslr', 'mirrorless', 'lens', 'lenses', 'vlog', 'vlogging', 'nikon', 'canon',
      'sony', 'fuji', 'fujifilm', 'gopro', 'iphone', 'macbook', 'laptop',
      '相机', '镜头', '摄影', '拍摄', '微单', '单反',
    ],
  },
  {
    vertical: 'travel',
    keywords: [
      'travel', 'trip', 'flight', 'flights', 'hotel', 'hotels', 'vacation', 'tour', 'booking',
      '旅行', '旅游', '酒店', '机票', '行程', '度假',
    ],
  },
  {
    vertical: 'finance',
    keywords: [
      'stock', 'stocks', 'broker', 'brokerage', 'trading', 'etf', 'crypto', 'wallet', 'portfolio', 'invest',
      'investing', 'finance', 'financial', 'loan', 'credit', 'tax',
      '股票', '基金', '理财', '投资', '交易', '券商', '加密', '钱包', '贷款',
    ],
  },
  {
    vertical: 'software',
    keywords: [
      'saas', 'software', 'cloud', 'api', 'hosting', 'developer', 'ai', 'tool', 'platform',
      '软件', '工具', '平台', '云', '接口',
    ],
  },
  {
    vertical: 'fashion',
    keywords: [
      'fashion', 'dress', 'bag', 'shoes', 'shoe', 'apparel', 'outfit',
      '时尚', '服饰', '穿搭', '鞋', '包',
    ],
  },
  {
    vertical: 'beauty',
    keywords: [
      'beauty', 'makeup', 'skincare', 'cosmetic', 'fragrance',
      '美妆', '护肤', '化妆',
    ],
  },
  {
    vertical: 'home',
    keywords: [
      'home', 'kitchen', 'furniture', 'decor', 'appliance',
      '家居', '厨房', '家具', '家电',
    ],
  },
])

const AMBIGUOUS_ENTITY_RULES = Object.freeze([
  {
    token: 'fuji',
    lockVertical: 'electronics',
    disambiguators: ['camera', 'cameras', 'dslr', 'mirrorless', 'lens', 'lenses', '相机', '镜头', '摄影', '拍摄'],
  },
])

function cleanText(value) {
  return String(value || '').trim().replace(/\s+/g, ' ')
}

function toFiniteNumber(value, fallback = 0) {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  return n
}

function clamp01(value, fallback = 0) {
  const n = toFiniteNumber(value, fallback)
  if (!Number.isFinite(n)) return fallback
  if (n < 0) return 0
  if (n > 1) return 1
  return n
}

function tokenize(value = '') {
  return cleanText(value)
    .toLowerCase()
    .split(/[^a-z0-9\u4e00-\u9fff]+/g)
    .map((item) => item.trim())
    .filter((item) => item.length >= 2)
}

function uniqueTokens(values = []) {
  const dedupe = new Set()
  const output = []
  for (const value of values) {
    const token = cleanText(value).toLowerCase()
    if (!token || dedupe.has(token)) continue
    dedupe.add(token)
    output.push(token)
  }
  return output
}

function normalizeVertical(value = '') {
  const token = cleanText(value).toLowerCase()
  if (!token) return 'general'

  for (const rule of QUERY_VERTICAL_RULES) {
    if (token === rule.vertical) return rule.vertical
    if (rule.keywords.some((keyword) => token.includes(keyword))) return rule.vertical
  }
  return 'general'
}

export function resolveVerticalFamily(vertical = '') {
  const normalized = normalizeVertical(vertical)
  return VERTICAL_FAMILIES[normalized] || 'general'
}

export function isSameVerticalFamily(left = '', right = '') {
  return resolveVerticalFamily(left) === resolveVerticalFamily(right)
}

function inferQueryVertical(tokens = [], text = '') {
  const scores = new Map()
  for (const rule of QUERY_VERTICAL_RULES) {
    let score = 0
    for (const keyword of rule.keywords) {
      if (tokens.includes(keyword)) {
        score += 2
      } else if (text.includes(keyword)) {
        score += 1
      }
    }
    if (score > 0) {
      scores.set(rule.vertical, score)
    }
  }

  if (scores.size <= 0) {
    return { vertical: 'general', score: 0 }
  }

  const ranked = Array.from(scores.entries())
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1]
      return a[0].localeCompare(b[0])
    })
  return {
    vertical: ranked[0][0],
    score: ranked[0][1],
  }
}

function resolveLockedVertical(tokens = [], text = '') {
  for (const rule of AMBIGUOUS_ENTITY_RULES) {
    const hasToken = tokens.includes(rule.token) || text.includes(rule.token)
    if (!hasToken) continue
    const matchedDisambiguator = rule.disambiguators.some((item) => tokens.includes(item) || text.includes(item))
    if (!matchedDisambiguator) continue
    return {
      vertical: rule.lockVertical,
      reason: `ambiguous_entity:${rule.token}->${rule.lockVertical}`,
    }
  }
  return { vertical: '', reason: '' }
}

function inferCandidateVertical(candidate = {}) {
  const metadata = candidate?.metadata && typeof candidate.metadata === 'object' ? candidate.metadata : {}
  const intentCardCatalog = metadata?.intentCardCatalog && typeof metadata.intentCardCatalog === 'object'
    ? metadata.intentCardCatalog
    : {}
  const category = cleanText(
    intentCardCatalog.category
    || metadata.category
    || metadata.vertical
    || metadata.verticalL2
    || metadata.vertical_l2
    || metadata.verticalL1
    || metadata.vertical_l1,
  )
  if (category) {
    return normalizeVertical(category)
  }

  const tags = Array.isArray(candidate?.tags)
    ? candidate.tags
    : (Array.isArray(metadata?.matchTags) ? metadata.matchTags : [])
  const corpus = [
    candidate?.title,
    candidate?.description,
    candidate?.entityText,
    candidate?.merchantName,
    category,
    tags.join(' '),
  ]
    .map((item) => cleanText(item))
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
  if (!corpus) return 'general'
  return inferQueryVertical(tokenize(corpus), corpus).vertical
}

function extractQueryEntityTokens(query = '', queryTokens = []) {
  const source = Array.isArray(queryTokens) ? queryTokens : tokenize(query)
  return source
    .filter((token) => {
      if (ENTITY_STOPWORDS.has(token)) return false
      if (INTENT_SIGNAL_TOKENS.has(token)) return false
      if (token.length < 3 && !/[\u4e00-\u9fff]{2,}/.test(token)) return false
      return true
    })
    .slice(0, 12)
}

function toCandidateCorpus(candidate = {}) {
  const metadata = candidate?.metadata && typeof candidate.metadata === 'object' ? candidate.metadata : {}
  const tags = Array.isArray(candidate?.tags)
    ? candidate.tags
    : (Array.isArray(metadata?.matchTags) ? metadata.matchTags : [])
  return [
    candidate?.title,
    candidate?.description,
    candidate?.targetUrl,
    candidate?.entityText,
    candidate?.merchantName,
    metadata?.category,
    metadata?.vertical,
    metadata?.verticalL2,
    metadata?.vertical_l2,
    tags.join(' '),
  ]
    .map((item) => cleanText(item))
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
}

function tokenOverlapRatio(queryTokens = [], candidateTokens = []) {
  const left = Array.isArray(queryTokens) ? queryTokens : []
  const rightSet = new Set(Array.isArray(candidateTokens) ? candidateTokens : [])
  if (left.length <= 0 || rightSet.size <= 0) return 0
  let hit = 0
  for (const token of new Set(left)) {
    if (rightSet.has(token)) hit += 1
  }
  return hit / Math.max(1, new Set(left).size)
}

function normalizeQuality(value) {
  const n = toFiniteNumber(value, 0)
  if (n <= 0) return 0
  if (n <= 1) return clamp01(n)
  return clamp01(n / 100)
}

function computeTopicScore(context = {}, candidate = {}, candidateVertical = 'general', candidateTokens = []) {
  const targetVertical = context.lockedVertical || context.queryVertical || 'general'
  const sameFamily = targetVertical === 'general'
    ? true
    : isSameVerticalFamily(targetVertical, candidateVertical)
  const overlap = tokenOverlapRatio(context.queryTokens, candidateTokens)
  const similarity = clamp01(Math.max(
    toFiniteNumber(candidate.fusedScore, 0),
    toFiniteNumber(candidate.vectorScore, 0),
    toFiniteNumber(candidate.lexicalScore, 0),
  ))

  const base = targetVertical === 'general'
    ? 0.26
    : (sameFamily ? 0.58 : 0.06)

  let score = base + overlap * 0.28 + similarity * 0.16
  if (context.lockedVertical && !sameFamily) {
    score = Math.min(score, 0.24)
  }
  return clamp01(score)
}

function computeEntityScore(context = {}, candidateCorpus = '', sameFamily = true) {
  const entityTokens = Array.isArray(context.queryEntityTokens) ? context.queryEntityTokens : []
  if (entityTokens.length <= 0) return 0.55

  const hitCount = entityTokens.filter((token) => candidateCorpus.includes(token)).length
  let score = 0.2 + (hitCount / entityTokens.length) * 0.7
  if (context.lockedVertical && !sameFamily) {
    score = Math.min(score, 0.2)
  }
  return clamp01(score)
}

function computeIntentFitScore(context = {}, candidate = {}, candidateVertical = 'general') {
  const intentClass = cleanText(context.intentClass).toLowerCase()
  const intentScore = clamp01(context.intentScore, 0.6)
  const commerceSignal = clamp01(
    normalizeQuality(candidate.quality) * 0.55
    + (toFiniteNumber(candidate.bidHint, 0) > 0 ? 0.25 : 0)
    + (cleanText(candidate.availability || 'active').toLowerCase() === 'active' ? 0.2 : 0),
  )

  if (intentClass === 'non_commercial') {
    return 0.12
  }

  let score = 0.45 + intentScore * 0.3 + commerceSignal * 0.25
  const hasIntentSignal = context.queryTokens.some((token) => INTENT_SIGNAL_TOKENS.has(token))
  if (hasIntentSignal && candidateVertical === 'general') {
    score -= 0.08
  }
  return clamp01(score)
}

function computeQualitySupportScore(candidate = {}, candidateTokens = []) {
  const quality = normalizeQuality(candidate.quality)
  const titleLen = cleanText(candidate.title).length
  const descriptionLen = cleanText(candidate.description).length
  const density = clamp01((candidateTokens.length / 20), 0)
  const infoCoverage = clamp01(
    (titleLen >= 12 ? 0.35 : 0.15)
    + (descriptionLen >= 28 ? 0.4 : (descriptionLen > 0 ? 0.2 : 0))
    + density * 0.25,
  )
  return clamp01(quality * 0.55 + infoCoverage * 0.45)
}

function buildExplanationList(input = {}) {
  const lines = []
  if (input.lockedVertical) {
    lines.push(`locked_vertical=${input.lockedVertical}`)
  }
  if (input.queryVertical) {
    lines.push(`query_vertical=${input.queryVertical}`)
  }
  if (input.candidateVertical) {
    lines.push(`candidate_vertical=${input.candidateVertical}`)
  }
  if (typeof input.overlap === 'number') {
    lines.push(`topic_overlap=${input.overlap.toFixed(4)}`)
  }
  if (typeof input.entityHitCount === 'number' && typeof input.entityTokenCount === 'number') {
    lines.push(`entity_match=${input.entityHitCount}/${input.entityTokenCount}`)
  }
  return lines
}

function round(value) {
  return Number(clamp01(value).toFixed(6))
}

export function buildRelevanceContext(input = {}) {
  const query = cleanText(input.query)
  const answerText = cleanText(input.answerText)
  const intentClass = cleanText(input.intentClass).toLowerCase()
  const intentScore = clamp01(input.intentScore, 0.6)
  const joined = `${query} ${answerText}`.trim().toLowerCase()
  const queryTokens = uniqueTokens(tokenize(joined))
  const queryVertical = inferQueryVertical(queryTokens, joined).vertical
  const lock = resolveLockedVertical(queryTokens, joined)

  return {
    query,
    answerText,
    queryTokens,
    queryVertical,
    lockedVertical: lock.vertical || '',
    lockReason: lock.reason || '',
    queryEntityTokens: extractQueryEntityTokens(query, queryTokens),
    intentClass,
    intentScore,
  }
}

export function scoreCandidateRelevance(candidate = {}, input = {}) {
  const context = input?.context && typeof input.context === 'object'
    ? input.context
    : buildRelevanceContext(input)
  const candidateVertical = inferCandidateVertical(candidate)
  const targetVertical = context.lockedVertical || context.queryVertical || 'general'
  const sameVerticalFamily = targetVertical === 'general'
    ? true
    : isSameVerticalFamily(targetVertical, candidateVertical)
  const candidateCorpus = toCandidateCorpus(candidate)
  const candidateTokens = uniqueTokens(tokenize(candidateCorpus))
  const topicScore = computeTopicScore(context, candidate, candidateVertical, candidateTokens)
  const entityScore = computeEntityScore(context, candidateCorpus, sameVerticalFamily)
  const intentFitScore = computeIntentFitScore(context, candidate, candidateVertical)
  const qualitySupportScore = computeQualitySupportScore(candidate, candidateTokens)
  const relevanceScore = (
    topicScore * COMPONENT_WEIGHTS.topic
    + entityScore * COMPONENT_WEIGHTS.entity
    + intentFitScore * COMPONENT_WEIGHTS.intentFit
    + qualitySupportScore * COMPONENT_WEIGHTS.qualitySupport
  )
  const entityHitCount = context.queryEntityTokens.filter((token) => candidateCorpus.includes(token)).length
  const overlap = tokenOverlapRatio(context.queryTokens, candidateTokens)

  return {
    relevanceScore: round(relevanceScore),
    componentScores: {
      topicScore: round(topicScore),
      entityScore: round(entityScore),
      intentFitScore: round(intentFitScore),
      qualitySupportScore: round(qualitySupportScore),
    },
    verticalDecision: {
      queryVertical: context.queryVertical || 'general',
      lockedVertical: context.lockedVertical || '',
      candidateVertical,
      targetVertical,
      sameVerticalFamily,
      lockReason: context.lockReason || '',
    },
    explanations: buildExplanationList({
      queryVertical: context.queryVertical,
      lockedVertical: context.lockedVertical,
      candidateVertical,
      overlap,
      entityHitCount,
      entityTokenCount: context.queryEntityTokens.length,
    }),
  }
}

export function resolveThresholdsForPlacement(placementId = '', policy = {}) {
  const thresholds = policy?.thresholds && typeof policy.thresholds === 'object'
    ? policy.thresholds
    : {}
  const normalizedPlacementId = cleanText(placementId)
  const defaults = DEFAULT_THRESHOLDS_BY_PLACEMENT[normalizedPlacementId] || DEFAULT_FALLBACK_THRESHOLDS
  const placementThreshold = thresholds[normalizedPlacementId] && typeof thresholds[normalizedPlacementId] === 'object'
    ? thresholds[normalizedPlacementId]
    : {}
  const strict = clamp01(
    placementThreshold.strict ?? placementThreshold.strictThreshold ?? defaults.strict,
    defaults.strict,
  )
  const relaxedRaw = clamp01(
    placementThreshold.relaxed ?? placementThreshold.relaxedThreshold ?? defaults.relaxed,
    defaults.relaxed,
  )
  const relaxed = Math.min(strict, relaxedRaw)

  return {
    strict,
    relaxed,
    thresholdVersion: cleanText(policy.thresholdVersion) || DEFAULT_THRESHOLD_VERSION,
  }
}

export {
  DEFAULT_FALLBACK_THRESHOLDS,
  DEFAULT_THRESHOLD_VERSION,
  DEFAULT_THRESHOLDS_BY_PLACEMENT,
}
