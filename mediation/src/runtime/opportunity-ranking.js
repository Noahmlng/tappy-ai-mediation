import {
  computeCandidateEconomicPricing,
  getPricingModelWeights,
  getPricingMediationDefaults,
} from './pricing-model.js'
import {
  buildRelevanceContext,
  resolveThresholdsForPlacement,
  scoreCandidateRelevance,
} from './relevance-model.js'

const DEFAULT_SCORE_FLOOR = 0.32
const DEFAULT_INTENT_MIN_LEXICAL_SCORE = 0.02
const DEFAULT_INTENT_MIN_VECTOR_SCORE = 0.14
const DEFAULT_INTENT_MIN_VECTOR_SCORE_FLOOR = 0.14
const DEFAULT_TOPIC_COVERAGE_THRESHOLD = 0.1
const RELEVANCE_GATED_PLACEMENTS = new Set(['chat_intent_recommendation_v1', 'chat_from_answer_v1'])
const RELEVANCE_POLICY_MODES = new Set(['observe', 'shadow', 'enforce'])

function cleanText(value) {
  return String(value || '').trim().replace(/\s+/g, ' ')
}

function clamp01(value) {
  const n = Number(value)
  if (!Number.isFinite(n)) return 0
  if (n < 0) return 0
  if (n > 1) return 1
  return n
}

function toFiniteNumber(value, fallback = 0) {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  return n
}

function parseBoolean(value, fallback = false) {
  const normalized = String(value ?? '').trim().toLowerCase()
  if (!normalized) return fallback
  if (['1', 'true', 'yes', 'on', 'enabled'].includes(normalized)) return true
  if (['0', 'false', 'no', 'off', 'disabled'].includes(normalized)) return false
  return fallback
}

function normalizeRelevancePolicyMode(value, fallback = 'enforce') {
  const mode = cleanText(value).toLowerCase()
  if (!mode) return fallback
  if (!RELEVANCE_POLICY_MODES.has(mode)) return fallback
  return mode
}

function shouldApplyRelevanceGate(placementId = '', relevancePolicy = {}) {
  if (relevancePolicy?.enabled === false) return false
  return RELEVANCE_GATED_PLACEMENTS.has(cleanText(placementId))
}

function normalizeQuality(value) {
  const n = toFiniteNumber(value, 0)
  if (n <= 0) return 0
  if (n <= 1) return clamp01(n)
  return clamp01(n / 100)
}

function normalizePolicyWeight(value) {
  const n = toFiniteNumber(value, 0)
  return clamp01((n + 2) / 4)
}

function freshnessScore(value) {
  const text = cleanText(value)
  if (!text) return 0.45
  const parsed = Date.parse(text)
  if (!Number.isFinite(parsed)) return 0.45

  const ageHours = Math.max(0, (Date.now() - parsed) / (1000 * 60 * 60))
  if (ageHours <= 24) return 1
  if (ageHours <= 72) return 0.8
  if (ageHours <= 7 * 24) return 0.55
  if (ageHours <= 14 * 24) return 0.35
  return 0.15
}

function containsBlockedTopic(text, blockedTopics = []) {
  const corpus = cleanText(text).toLowerCase()
  if (!corpus) return ''
  for (const topic of blockedTopics) {
    const normalizedTopic = cleanText(topic).toLowerCase()
    if (!normalizedTopic) continue
    if (corpus.includes(normalizedTopic)) return normalizedTopic
  }
  return ''
}

function resolveBidImageUrl(candidate = {}) {
  const metadata = candidate?.metadata && typeof candidate.metadata === 'object' ? candidate.metadata : {}
  return cleanText(
    metadata.image_url
    || metadata.imageUrl
    || metadata.brand_image_url
    || metadata.brandImageUrl
    || metadata.icon_url
    || metadata.iconUrl,
  )
}

function resolveCampaignId(candidate = {}) {
  const metadata = candidate?.metadata && typeof candidate.metadata === 'object' ? candidate.metadata : {}
  return cleanText(
    metadata.campaignId
    || metadata.campaign_id
    || metadata.programId
    || metadata.program_id
    || metadata.advertiserId
    || metadata.advertiser_id,
  )
}

function resolveAdvertiserName(candidate = {}) {
  const metadata = candidate?.metadata && typeof candidate.metadata === 'object' ? candidate.metadata : {}
  const prioritized = [
    metadata.merchant,
    metadata.merchantName,
    metadata.advertiserName,
    metadata.partnerName,
    metadata.brandName,
    metadata.brand_name,
    metadata.brandId,
    metadata.brand_id,
  ]

  for (const value of prioritized) {
    const advertiser = cleanText(value)
    if (advertiser) return advertiser
  }

  return cleanText(candidate.network || 'inventory') || 'inventory'
}

function toBid(candidate = {}, context = {}) {
  const title = cleanText(candidate.title)
  const targetUrl = cleanText(candidate.targetUrl)
  if (!title || !targetUrl) return null

  const bidId = cleanText(candidate.offerId) || `bid_${Date.now()}`
  const pricing = candidate?.pricing && typeof candidate.pricing === 'object' ? candidate.pricing : null
  const price = Math.max(0, toFiniteNumber(pricing?.cpcUsd ?? candidate.bidHint, 0))
  const campaignId = resolveCampaignId(candidate)

  return {
    price,
    advertiser: resolveAdvertiserName(candidate),
    headline: title,
    description: cleanText(candidate.description) || title,
    cta_text: 'Learn More',
    url: targetUrl,
    image_url: resolveBidImageUrl(candidate),
    dsp: cleanText(candidate.network),
    bidId,
    ...(campaignId ? { campaignId } : {}),
    placement: cleanText(context.placement || 'block') || 'block',
    variant: 'opportunity_first_v1',
    pricing: pricing
      ? {
          modelVersion: cleanText(pricing.modelVersion),
          pricingSemanticsVersion: cleanText(pricing.pricingSemanticsVersion),
          billingUnit: cleanText(pricing.billingUnit).toLowerCase() || 'cpc',
          targetRpmUsd: toFiniteNumber(pricing.targetRpmUsd, 0),
          ecpmUsd: toFiniteNumber(pricing.ecpmUsd, 0),
          cpcUsd: toFiniteNumber(pricing.cpcUsd, 0),
          cpaUsd: toFiniteNumber(pricing.cpaUsd, 0),
          pClick: toFiniteNumber(pricing.pClick, 0),
          pConv: toFiniteNumber(pricing.pConv, 0),
          triggerType: cleanText(pricing.triggerType),
          network: cleanText(pricing.network || candidate.network || ''),
          rawSignal: pricing.rawSignal && typeof pricing.rawSignal === 'object'
            ? {
                rawBidValue: toFiniteNumber(pricing.rawSignal.rawBidValue, 0),
                rawUnit: cleanText(pricing.rawSignal.rawUnit),
                normalizedFactor: toFiniteNumber(pricing.rawSignal.normalizedFactor, 1),
              }
            : {
                rawBidValue: Math.max(0, toFiniteNumber(candidate.bidHint, 0)),
                rawUnit: 'bid_hint',
                normalizedFactor: 1,
              },
        }
      : undefined,
  }
}

function scoreCandidate(candidate = {}, input = {}) {
  const intentScore = clamp01(input.intentScore)
  const nativeSimilarity = Math.max(
    toFiniteNumber(candidate.fusedScore),
    toFiniteNumber(candidate.vectorScore),
    toFiniteNumber(candidate.lexicalScore),
  )
  const similarity = clamp01(
    Number.isFinite(candidate.relevanceScore)
      ? Math.max(candidate.relevanceScore, nativeSimilarity)
      : nativeSimilarity
  )
  const quality = normalizeQuality(candidate.quality)
  const policyWeight = normalizePolicyWeight(candidate.policyWeight)
  const freshness = freshnessScore(candidate.freshnessAt || candidate.updatedAt)
  const availability = cleanText(candidate.availability).toLowerCase() === 'active' ? 1 : 0

  const rankScore = Number((
    similarity * 0.35
    + intentScore * 0.25
    + quality * 0.15
    + policyWeight * 0.1
    + freshness * 0.1
    + availability * 0.05
  ).toFixed(6))
  const pricing = computeCandidateEconomicPricing({
    candidate,
    placementId: input.placementId,
    triggerType: input.triggerType,
  })
  const weights = getPricingModelWeights()
  const auctionScore = Number((
    rankScore * weights.rankWeight
    + pricing.economicScore * weights.economicWeight
  ).toFixed(6))

  return {
    ...candidate,
    rankScore,
    auctionScore,
    pricing,
    rankFeatures: {
      intentScore,
      similarity,
      quality,
      policyWeight,
      freshness,
      availability,
      relevanceScore: clamp01(candidate.relevanceScore),
    },
  }
}

function buildScoredRelevanceCandidates(candidates = [], input = {}) {
  const context = buildRelevanceContext({
    query: input.query,
    answerText: input.answerText,
    intentClass: input.intentClass,
    intentScore: input.intentScore,
  })

  return candidates.map((candidate) => {
    const relevance = scoreCandidateRelevance(candidate, { context })
    return {
      ...candidate,
      relevanceScore: relevance.relevanceScore,
      relevanceComponentScores: relevance.componentScores,
      relevanceVerticalDecision: relevance.verticalDecision,
      relevanceExplanations: relevance.explanations,
    }
  })
}

function buildRelevanceGateSnapshot(input = {}) {
  return {
    applied: Boolean(input.applied),
    mode: cleanText(input.mode),
    placementId: cleanText(input.placementId),
    minLexicalScore: clamp01(input.minLexicalScore),
    minVectorScore: clamp01(input.minVectorScore),
    strictThreshold: clamp01(input.strictThreshold),
    relaxedThreshold: clamp01(input.relaxedThreshold),
    thresholdVersion: cleanText(input.thresholdVersion),
    sameVerticalFallbackEnabled: input.sameVerticalFallbackEnabled !== false,
    baseEligibleCount: toFiniteNumber(input.baseEligibleCount, 0),
    strictEligibleCount: toFiniteNumber(input.strictEligibleCount, 0),
    relaxedEligibleCount: toFiniteNumber(input.relaxedEligibleCount, 0),
    filteredCount: toFiniteNumber(input.filteredCount, 0),
    eligibleCount: toFiniteNumber(input.eligibleCount, 0),
    triggered: Boolean(input.triggered),
    gateStage: cleanText(input.gateStage || 'blocked') || 'blocked',
    blockedReason: cleanText(input.blockedReason),
    verticalDecision: input.verticalDecision && typeof input.verticalDecision === 'object'
      ? { ...input.verticalDecision }
      : {
          queryVertical: 'general',
          lockedVertical: '',
          targetVertical: 'general',
        },
  }
}

function chooseRelevanceEligibleCandidates(baseEligible = [], input = {}) {
  const placementId = cleanText(input.placementId)
  const configuredMinLexicalScore = clamp01(
    input.minLexicalScore ?? DEFAULT_INTENT_MIN_LEXICAL_SCORE,
    DEFAULT_INTENT_MIN_LEXICAL_SCORE,
  )
  const configuredMinVectorScore = clamp01(
    input.minVectorScore ?? DEFAULT_INTENT_MIN_VECTOR_SCORE,
    DEFAULT_INTENT_MIN_VECTOR_SCORE,
  )
  const minLexicalScore = Math.max(configuredMinLexicalScore, DEFAULT_INTENT_MIN_LEXICAL_SCORE)
  const minVectorScore = Math.max(configuredMinVectorScore, DEFAULT_INTENT_MIN_VECTOR_SCORE_FLOOR)
  const relevancePolicy = input.relevancePolicyV2 && typeof input.relevancePolicyV2 === 'object'
    ? input.relevancePolicyV2
    : {}
  const applied = shouldApplyRelevanceGate(placementId, relevancePolicy)
  const mode = normalizeRelevancePolicyMode(relevancePolicy.mode, 'enforce')
  const thresholds = resolveThresholdsForPlacement(placementId, relevancePolicy)
  const sameVerticalFallbackEnabled = parseBoolean(relevancePolicy.sameVerticalFallbackEnabled, true)

  if (!applied || baseEligible.length <= 0) {
    return {
      eligible: baseEligible,
      gateStage: applied ? 'strict' : 'disabled',
      blockedReason: '',
      strictEligibleCount: baseEligible.length,
      relaxedEligibleCount: baseEligible.length,
      filteredCount: 0,
      shadowDecision: null,
      scoredCandidates: baseEligible,
      gate: buildRelevanceGateSnapshot({
        applied,
        mode,
        placementId,
        minLexicalScore,
        minVectorScore,
        strictThreshold: thresholds.strict,
        relaxedThreshold: thresholds.relaxed,
        thresholdVersion: thresholds.thresholdVersion,
        sameVerticalFallbackEnabled,
        baseEligibleCount: baseEligible.length,
        strictEligibleCount: baseEligible.length,
        relaxedEligibleCount: baseEligible.length,
        filteredCount: 0,
        eligibleCount: baseEligible.length,
        triggered: false,
        gateStage: applied ? 'strict' : 'disabled',
        blockedReason: '',
      }),
      relevanceDebug: {
        relevanceScore: 0,
        componentScores: {
          topicScore: 0,
          entityScore: 0,
          intentFitScore: 0,
          qualitySupportScore: 0,
        },
        gateStage: applied ? 'strict' : 'disabled',
        thresholdsApplied: {
          strict: thresholds.strict,
          relaxed: thresholds.relaxed,
          thresholdVersion: thresholds.thresholdVersion,
          mode,
        },
        verticalDecision: {
          queryVertical: 'general',
          lockedVertical: '',
          targetVertical: 'general',
          candidateVertical: 'general',
          sameVerticalFamily: true,
          lockReason: '',
        },
        explanations: [],
      },
    }
  }

  const scoredCandidates = buildScoredRelevanceCandidates(baseEligible, input)
  const lexicalVectorEligible = scoredCandidates.filter((candidate) => (
    toFiniteNumber(candidate?.lexicalScore, 0) >= minLexicalScore
    || toFiniteNumber(candidate?.vectorScore, 0) >= minVectorScore
  ))
  const preFilteredCount = Math.max(0, scoredCandidates.length - lexicalVectorEligible.length)
  const strictEligible = lexicalVectorEligible
    .filter((candidate) => candidate.relevanceScore >= thresholds.strict)

  const firstVertical = lexicalVectorEligible[0]?.relevanceVerticalDecision
    && typeof lexicalVectorEligible[0].relevanceVerticalDecision === 'object'
    ? lexicalVectorEligible[0].relevanceVerticalDecision
    : {}
  const targetVertical = cleanText(firstVertical.targetVertical || firstVertical.queryVertical || 'general') || 'general'

  const relaxedPool = sameVerticalFallbackEnabled
    ? lexicalVectorEligible.filter((candidate) => candidate?.relevanceVerticalDecision?.sameVerticalFamily === true)
    : []
  const relaxedEligible = relaxedPool
    .filter((candidate) => candidate.relevanceScore >= thresholds.relaxed)

  let gateStage = 'strict'
  let blockedReason = ''
  let eligible = strictEligible

  const strictBlocked = strictEligible.length === 0
  const hasCrossVerticalRelaxedHit = lexicalVectorEligible.some((candidate) => (
    candidate.relevanceScore >= thresholds.relaxed
    && candidate?.relevanceVerticalDecision?.sameVerticalFamily !== true
  ))

  if (strictBlocked) {
    if (sameVerticalFallbackEnabled && targetVertical !== 'general' && relaxedEligible.length > 0) {
      gateStage = 'relaxed'
      eligible = relaxedEligible
    } else {
      gateStage = 'blocked'
      blockedReason = hasCrossVerticalRelaxedHit
        ? 'relevance_blocked_cross_vertical'
        : 'relevance_blocked_strict'
      eligible = []
    }
  }

  const shadowDecision = {
    gateStage,
    blockedReason,
    strictEligibleCount: strictEligible.length,
    relaxedEligibleCount: relaxedEligible.length,
    strictThreshold: thresholds.strict,
    relaxedThreshold: thresholds.relaxed,
    targetVertical,
  }
  const enforceEligible = eligible

  if (mode === 'observe') {
    eligible = scoredCandidates
    gateStage = 'observe'
    blockedReason = ''
  } else if (mode === 'shadow') {
    eligible = scoredCandidates
    gateStage = 'shadow'
    blockedReason = ''
  }

  const enforceFilteredCount = Math.max(0, preFilteredCount + lexicalVectorEligible.length - enforceEligible.length)
  const filteredCount = mode === 'enforce' ? enforceFilteredCount : 0
  const winnerLike = eligible[0] || scoredCandidates[0] || null

  return {
    eligible,
    gateStage,
    blockedReason,
    strictEligibleCount: strictEligible.length,
    relaxedEligibleCount: relaxedEligible.length,
    filteredCount,
    shadowDecision,
    scoredCandidates,
    gate: buildRelevanceGateSnapshot({
      applied,
      mode,
      placementId,
      minLexicalScore,
      minVectorScore,
      strictThreshold: thresholds.strict,
      relaxedThreshold: thresholds.relaxed,
      thresholdVersion: thresholds.thresholdVersion,
        sameVerticalFallbackEnabled,
        baseEligibleCount: baseEligible.length,
        strictEligibleCount: strictEligible.length,
        relaxedEligibleCount: relaxedEligible.length,
        filteredCount: mode === 'enforce' ? filteredCount : enforceFilteredCount,
        eligibleCount: eligible.length,
        triggered: mode === 'enforce' ? filteredCount > 0 : enforceFilteredCount > 0,
        gateStage,
        blockedReason,
        verticalDecision: {
        queryVertical: cleanText(firstVertical.queryVertical || 'general') || 'general',
        lockedVertical: cleanText(firstVertical.lockedVertical),
        targetVertical,
      },
    }),
    relevanceDebug: {
      relevanceScore: winnerLike ? clamp01(winnerLike.relevanceScore) : 0,
      componentScores: winnerLike?.relevanceComponentScores && typeof winnerLike.relevanceComponentScores === 'object'
        ? winnerLike.relevanceComponentScores
        : {
            topicScore: 0,
            entityScore: 0,
            intentFitScore: 0,
            qualitySupportScore: 0,
          },
      gateStage,
      thresholdsApplied: {
        strict: thresholds.strict,
        relaxed: thresholds.relaxed,
        thresholdVersion: thresholds.thresholdVersion,
        mode,
      },
      verticalDecision: winnerLike?.relevanceVerticalDecision && typeof winnerLike.relevanceVerticalDecision === 'object'
        ? winnerLike.relevanceVerticalDecision
        : {
            queryVertical: cleanText(firstVertical.queryVertical || 'general') || 'general',
            lockedVertical: cleanText(firstVertical.lockedVertical),
            targetVertical,
            candidateVertical: cleanText(firstVertical.candidateVertical || 'general') || 'general',
            sameVerticalFamily: true,
            lockReason: cleanText(firstVertical.lockReason),
          },
      explanations: Array.isArray(winnerLike?.relevanceExplanations)
        ? winnerLike.relevanceExplanations
        : [],
      strictEligibleCount: strictEligible.length,
      relaxedEligibleCount: relaxedEligible.length,
      blockedReason,
      shadowDecision: mode === 'shadow' ? shadowDecision : null,
    },
  }
}

export function rankOpportunityCandidates(input = {}) {
  const candidates = Array.isArray(input.candidates) ? input.candidates : []
  const pricingDefaults = getPricingMediationDefaults()
  const weights = getPricingModelWeights()
  const placementId = cleanText(input.placementId)
  const scoreFloor = clamp01(input.scoreFloor ?? DEFAULT_SCORE_FLOOR)
  const blockedTopics = Array.isArray(input.blockedTopics)
    ? input.blockedTopics.map((item) => cleanText(item)).filter(Boolean)
    : []
  const query = cleanText(input.query)
  const answerText = cleanText(input.answerText)
  const topicCoverageGateEnabled = input.topicCoverageGateEnabled === true
  const topicCoverageThreshold = clamp01(
    input.topicCoverageThreshold ?? DEFAULT_TOPIC_COVERAGE_THRESHOLD,
  )

  const blockedTopic = containsBlockedTopic(`${query} ${answerText}`, blockedTopics)
  if (blockedTopic) {
    return {
      winner: null,
      ranked: [],
      reasonCode: 'policy_blocked',
      debug: {
        policyBlockedTopic: blockedTopic,
        candidateCount: candidates.length,
        scoreFloor,
        relevanceGate: buildRelevanceGateSnapshot({
          applied: false,
          mode: 'disabled',
          placementId,
          strictThreshold: 0,
          relaxedThreshold: 0,
          thresholdVersion: '',
          baseEligibleCount: 0,
          strictEligibleCount: 0,
          relaxedEligibleCount: 0,
          filteredCount: 0,
          eligibleCount: 0,
          triggered: false,
          gateStage: 'blocked',
          blockedReason: '',
        }),
        relevanceFilteredCount: 0,
        topicCoverageGateEnabled,
        topicCoverageThreshold,
        topicCoverageFilteredCount: 0,
        relevanceDebug: {
          relevanceScore: 0,
          componentScores: {
            topicScore: 0,
            entityScore: 0,
            intentFitScore: 0,
            qualitySupportScore: 0,
          },
          gateStage: 'blocked',
          thresholdsApplied: { strict: 0, relaxed: 0, thresholdVersion: '', mode: 'disabled' },
          verticalDecision: {
            queryVertical: 'general',
            lockedVertical: '',
            targetVertical: 'general',
            candidateVertical: 'general',
            sameVerticalFamily: true,
            lockReason: '',
          },
          explanations: [],
          strictEligibleCount: 0,
          relaxedEligibleCount: 0,
          blockedReason: '',
          shadowDecision: null,
        },
        pricingModel: pricingDefaults.modelVersion,
      },
    }
  }

  if (candidates.length === 0) {
    return {
      winner: null,
      ranked: [],
      reasonCode: 'inventory_no_match',
      debug: {
        candidateCount: 0,
        scoreFloor,
        relevanceGate: buildRelevanceGateSnapshot({
          applied: false,
          mode: 'disabled',
          placementId,
          strictThreshold: 0,
          relaxedThreshold: 0,
          thresholdVersion: '',
          baseEligibleCount: 0,
          strictEligibleCount: 0,
          relaxedEligibleCount: 0,
          filteredCount: 0,
          eligibleCount: 0,
          triggered: false,
          gateStage: 'blocked',
          blockedReason: '',
        }),
        relevanceFilteredCount: 0,
        topicCoverageGateEnabled,
        topicCoverageThreshold,
        topicCoverageFilteredCount: 0,
        relevanceDebug: {
          relevanceScore: 0,
          componentScores: {
            topicScore: 0,
            entityScore: 0,
            intentFitScore: 0,
            qualitySupportScore: 0,
          },
          gateStage: 'blocked',
          thresholdsApplied: { strict: 0, relaxed: 0, thresholdVersion: '', mode: 'disabled' },
          verticalDecision: {
            queryVertical: 'general',
            lockedVertical: '',
            targetVertical: 'general',
            candidateVertical: 'general',
            sameVerticalFamily: true,
            lockReason: '',
          },
          explanations: [],
          strictEligibleCount: 0,
          relaxedEligibleCount: 0,
          blockedReason: '',
          shadowDecision: null,
        },
        pricingModel: pricingDefaults.modelVersion,
      },
    }
  }

  const baseEligible = candidates
    .filter((item) => cleanText(item?.title) && cleanText(item?.targetUrl))
    .filter((item) => cleanText(item?.availability || 'active').toLowerCase() === 'active')
  const topicCoverageEligible = topicCoverageGateEnabled
    ? baseEligible.filter((item) => (
      toFiniteNumber(item?.topicCoverageScore, 0) >= topicCoverageThreshold
      || toFiniteNumber(item?.brandEntityHitCount, 0) > 0
    ))
    : baseEligible
  const topicCoverageFilteredCount = Math.max(0, baseEligible.length - topicCoverageEligible.length)

  const relevanceSelection = chooseRelevanceEligibleCandidates(topicCoverageEligible, {
    ...input,
    placementId,
    query,
    answerText,
  })
  const eligible = relevanceSelection.eligible
  const relevanceFilteredCount = relevanceSelection.filteredCount
  const relevanceGate = relevanceSelection.gate

  if (eligible.length === 0) {
    return {
      winner: null,
      ranked: [],
      reasonCode: cleanText(relevanceSelection.blockedReason) || 'inventory_no_match',
      debug: {
        candidateCount: candidates.length,
        eligibleCount: 0,
        scoreFloor,
        relevanceGate,
        relevanceFilteredCount,
        topicCoverageGateEnabled,
        topicCoverageThreshold,
        topicCoverageFilteredCount,
        relevanceDebug: relevanceSelection.relevanceDebug,
        pricingModel: pricingDefaults.modelVersion,
      },
    }
  }

  const scored = eligible
    .map((candidate) => scoreCandidate(candidate, {
      intentScore: input.intentScore,
      placementId,
      triggerType: input.triggerType,
    }))
  const ranked = [...scored]
    .sort((a, b) => {
      if (b.auctionScore !== a.auctionScore) return b.auctionScore - a.auctionScore
      if (b.rankScore !== a.rankScore) return b.rankScore - a.rankScore
      if (b.relevanceScore !== a.relevanceScore) return b.relevanceScore - a.relevanceScore
      if (b.fusedScore !== a.fusedScore) return b.fusedScore - a.fusedScore
      return String(a.offerId || '').localeCompare(String(b.offerId || ''))
    })

  const auctionWinner = ranked[0] || null
  const topRankCandidate = [...scored].sort((a, b) => {
    if (b.rankScore !== a.rankScore) return b.rankScore - a.rankScore
    if (b.auctionScore !== a.auctionScore) return b.auctionScore - a.auctionScore
    return String(a.offerId || '').localeCompare(String(b.offerId || ''))
  })[0] || null
  const rankDominanceFloor = clamp01(pricingDefaults.rankDominanceFloor ?? 0.5)
  const rankDominanceMargin = clamp01(pricingDefaults.rankDominanceMargin ?? 0.1)
  const scoreFloorOrGuard = Math.max(scoreFloor, rankDominanceFloor)
  const shouldProtectRankWinner = Boolean(
    auctionWinner
    && topRankCandidate
    && auctionWinner.offerId !== topRankCandidate.offerId
    && topRankCandidate.rankScore >= scoreFloorOrGuard
    && (topRankCandidate.rankScore - auctionWinner.rankScore) >= rankDominanceMargin,
  )
  const winner = shouldProtectRankWinner ? topRankCandidate : auctionWinner
  const relevanceGateWithWinner = {
    ...relevanceGate,
    winnerLexicalScore: winner ? toFiniteNumber(winner.lexicalScore, 0) : 0,
    winnerVectorScore: winner ? toFiniteNumber(winner.vectorScore, 0) : 0,
    winnerRelevanceScore: winner ? toFiniteNumber(winner.relevanceScore, 0) : 0,
  }

  if (!winner || winner.rankScore < scoreFloor) {
    return {
      winner: null,
      ranked,
      reasonCode: 'rank_below_floor',
      debug: {
        candidateCount: candidates.length,
        eligibleCount: eligible.length,
        topRankScore: winner ? winner.rankScore : 0,
        topAuctionScore: winner ? winner.auctionScore : 0,
        topEconomicScore: winner?.pricing ? winner.pricing.economicScore : 0,
        rankDominanceApplied: false,
        scoreFloor,
        relevanceGate: relevanceGateWithWinner,
        relevanceFilteredCount,
        topicCoverageGateEnabled,
        topicCoverageThreshold,
        topicCoverageFilteredCount,
        relevanceDebug: {
          ...relevanceSelection.relevanceDebug,
          relevanceScore: winner ? clamp01(winner.relevanceScore) : relevanceSelection.relevanceDebug.relevanceScore,
          componentScores: winner?.relevanceComponentScores || relevanceSelection.relevanceDebug.componentScores,
          verticalDecision: winner?.relevanceVerticalDecision || relevanceSelection.relevanceDebug.verticalDecision,
          explanations: winner?.relevanceExplanations || relevanceSelection.relevanceDebug.explanations,
        },
        pricingModel: pricingDefaults.modelVersion,
        pricingWeights: weights,
      },
    }
  }

  return {
    winner: {
      ...winner,
      bid: toBid(winner, {
        placement: input.placement,
      }),
    },
    ranked,
    reasonCode: relevanceSelection.gateStage === 'relaxed'
      ? 'relevance_pass_relaxed_same_vertical'
      : 'served',
    debug: {
      candidateCount: candidates.length,
      eligibleCount: eligible.length,
      topRankScore: winner.rankScore,
      topAuctionScore: winner.auctionScore,
      topEconomicScore: winner?.pricing ? winner.pricing.economicScore : 0,
      rankDominanceApplied: shouldProtectRankWinner,
      rankDominanceFloor,
      rankDominanceMargin,
      scoreFloor,
      relevanceGate: relevanceGateWithWinner,
      relevanceFilteredCount,
      topicCoverageGateEnabled,
      topicCoverageThreshold,
      topicCoverageFilteredCount,
      relevanceDebug: {
        ...relevanceSelection.relevanceDebug,
        relevanceScore: clamp01(winner.relevanceScore),
        componentScores: winner?.relevanceComponentScores || relevanceSelection.relevanceDebug.componentScores,
        verticalDecision: winner?.relevanceVerticalDecision || relevanceSelection.relevanceDebug.verticalDecision,
        explanations: winner?.relevanceExplanations || relevanceSelection.relevanceDebug.explanations,
      },
      pricingModel: pricingDefaults.modelVersion,
      pricingWeights: weights,
    },
  }
}

export { toBid as mapRankedCandidateToBid }
