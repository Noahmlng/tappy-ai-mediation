import assert from 'node:assert/strict'
import test from 'node:test'

import { inferIntentByRules, scoreIntentOpportunityFirst } from '../../src/runtime/intent-scoring.js'
import { retrieveOpportunityCandidates } from '../../src/runtime/opportunity-retrieval.js'
import { rankOpportunityCandidates } from '../../src/runtime/opportunity-ranking.js'
import { createOpportunityWriter } from '../../src/runtime/opportunity-writer.js'

test('opportunity-first intent: commerce query is scored by rules and supports no-llm mode', async () => {
  const rule = inferIntentByRules({
    query: 'best iphone deals and macbook air coupon',
    answerText: '',
  })

  assert.equal(['shopping', 'purchase_intent', 'product_exploration'].includes(rule.class), true)
  assert.equal(rule.score > 0.4, true)

  const scored = await scoreIntentOpportunityFirst({
    query: 'hostinger coupon and shopify pricing',
    answerText: '',
    locale: 'en-US',
  }, {
    useLlmFallback: false,
  })

  assert.equal(scored.source, 'rule')
  assert.equal(scored.score > 0.3, true)
  assert.equal(typeof scored.class, 'string')

  const financeRule = inferIntentByRules({
    query: 'Which broker has lower ETF fees and better options tools?',
    answerText: 'Compare brokerage pricing and trading platform quality.',
  })
  assert.equal(financeRule.class !== 'non_commercial', true)
  assert.equal(financeRule.score >= 0.34, true)

  const chineseRule = inferIntentByRules({
    query: '我想给女朋友买会员，帮我对比一下哪个平台工具更推荐',
    answerText: '你可以比较价格和优惠再决定购买。',
  })
  assert.equal(chineseRule.class !== 'non_commercial', true)
  assert.equal(chineseRule.score >= 0.22, true)
  assert.equal(Array.isArray(chineseRule.ruleMeta?.matchedKeywords), true)
  assert.equal(chineseRule.ruleMeta?.matchedKeywords?.length > 0, true)
})

test('opportunity retrieval: connector fallback returns sortable candidates without postgres pool', async () => {
  const result = await retrieveOpportunityCandidates({
    query: 'low fee brokerage for etf trading',
    filters: {
      networks: ['house'],
      market: 'US',
      language: 'en-US',
    },
    finalTopK: 5,
  }, {
    pool: null,
    enableFallbackWhenInventoryUnavailable: true,
    fallbackProvider: async () => ({
      offers: [
        {
          offerId: 'house:finance:001',
          sourceNetwork: 'house',
          sourceId: 'house_raw_001',
          sourceType: 'offer',
          title: 'Low-fee ETF Brokerage',
          description: 'US brokerage with low options and ETF fees',
          targetUrl: 'https://example.com/broker',
          market: 'US',
          locale: 'en-US',
          availability: 'active',
          qualityScore: 0.9,
          bidValue: 6.2,
          metadata: {
            policyWeight: 0.2,
            tags: ['brokerage', 'etf', 'trading'],
          },
        },
      ],
      debug: {
        mode: 'connector_live_fallback',
      },
    }),
  })

  assert.equal(result.debug.mode, 'connector_live_fallback')
  assert.equal(result.candidates.length, 1)
  assert.equal(result.candidates[0].offerId, 'house:finance:001')
  assert.equal(result.candidates[0].fusedScore > 0, true)
  assert.equal(typeof result.candidates[0].rrfScore, 'number')
  assert.equal(result.debug.queryMode, 'raw_query')
  assert.equal(typeof result.debug.queryUsed, 'string')
  assert.equal(typeof result.debug.scoring, 'object')
  assert.equal(result.debug.scoring.strategy, 'rrf_then_linear')
  assert.equal(typeof result.debug.bm25HitCount, 'number')
  assert.equal(typeof result.debug.brandIntentDetected, 'boolean')
  assert.equal(Array.isArray(result.debug.brandEntityTokens), true)
  assert.equal(Array.isArray(result.debug.penaltiesApplied), true)
  assert.equal(typeof result.debug.scoreStats, 'object')
  assert.equal(typeof result.debug.scoreStats.sparseMin, 'number')
  assert.equal(typeof result.debug.scoreStats.denseMax, 'number')

  const disabled = await retrieveOpportunityCandidates({
    query: 'low fee brokerage for etf trading',
    filters: {
      networks: ['house'],
      market: 'US',
      language: 'en-US',
    },
    finalTopK: 5,
  }, {
    pool: null,
    enableFallbackWhenInventoryUnavailable: false,
    fallbackProvider: async () => ({ offers: [] }),
  })

  assert.equal(disabled.debug.mode, 'inventory_store_unavailable')
  assert.equal(disabled.candidates.length, 0)
})

test('opportunity-first ranking: emits stable reason codes for miss and low-rank paths', () => {
  const miss = rankOpportunityCandidates({
    candidates: [],
    query: 'generic conversation',
    answerText: 'no commerce intent',
    intentScore: 0.1,
  })
  assert.equal(miss.reasonCode, 'inventory_no_match')

  const lowRank = rankOpportunityCandidates({
    candidates: [
      {
        offerId: 'house:product:001',
        network: 'house',
        title: 'camera bundle',
        description: 'entry camera',
        targetUrl: 'https://example.com/camera',
        availability: 'active',
        quality: 0.1,
        bidHint: 0.01,
        policyWeight: 0,
        lexicalScore: 0.01,
        vectorScore: 0.01,
        fusedScore: 0.01,
      },
    ],
    query: 'camera deals',
    answerText: '',
    intentScore: 0.2,
    scoreFloor: 0.8,
  })
  assert.equal(lowRank.reasonCode, 'rank_below_floor')

  const served = rankOpportunityCandidates({
    candidates: [
      {
        offerId: 'partnerstack:link:001',
        network: 'partnerstack',
        title: 'Canva Pro discount',
        description: 'save now',
        targetUrl: 'https://example.com/canva',
        availability: 'active',
        quality: 0.9,
        bidHint: 2.1,
        policyWeight: 0.4,
        lexicalScore: 0.6,
        vectorScore: 0.7,
        fusedScore: 0.75,
        metadata: {
          image_url: 'https://cdn.example.com/canva.png',
        },
      },
    ],
    query: 'canva pro discount',
    answerText: '',
    intentScore: 0.86,
  })

  assert.equal(served.reasonCode, 'served')
  assert.equal(Boolean(served.winner?.bid), true)
  assert.equal(served.winner.bid.dsp, 'partnerstack')
  assert.equal(typeof served.winner.bid.price, 'number')
  assert.equal(Boolean(served.winner.bid.pricing), true)
  assert.equal(served.winner.bid.pricing.modelVersion, 'cpa_mock_v2')
  assert.equal(typeof served.winner.bid.pricing.cpaUsd, 'number')
  assert.equal(typeof served.winner.bid.pricing.ecpmUsd, 'number')
  assert.equal(typeof served.winner.bid.pricing.pConv, 'number')
  assert.equal(served.winner.bid.image_url, 'https://cdn.example.com/canva.png')
})

test('opportunity-first ranking: bid advertiser prefers merchant, then brandId, then network fallback', () => {
  const buildCandidate = (overrides = {}) => ({
    offerId: 'house:product:001',
    network: 'house',
    title: 'Developer tools deal',
    description: 'high intent offer',
    targetUrl: 'https://example.com/developer-tools',
    availability: 'active',
    quality: 0.9,
    bidHint: 3.2,
    policyWeight: 0.1,
    lexicalScore: 0.71,
    vectorScore: 0.78,
    fusedScore: 0.8,
    metadata: {},
    ...overrides,
  })

  const withMerchant = rankOpportunityCandidates({
    candidates: [buildCandidate({ metadata: { merchant: 'Verizon' } })],
    query: 'best developer tools discount',
    answerText: '',
    intentScore: 0.83,
  })
  assert.equal(withMerchant.reasonCode, 'served')
  assert.equal(withMerchant.winner?.bid?.advertiser, 'Verizon')

  const withBrandId = rankOpportunityCandidates({
    candidates: [buildCandidate({ metadata: { brandId: 'brand_verizon' } })],
    query: 'best developer tools discount',
    answerText: '',
    intentScore: 0.83,
  })
  assert.equal(withBrandId.reasonCode, 'served')
  assert.equal(withBrandId.winner?.bid?.advertiser, 'brand_verizon')

  const withNetworkFallback = rankOpportunityCandidates({
    candidates: [buildCandidate({ network: 'cj', metadata: {} })],
    query: 'best developer tools discount',
    answerText: '',
    intentScore: 0.83,
  })
  assert.equal(withNetworkFallback.reasonCode, 'served')
  assert.equal(withNetworkFallback.winner?.bid?.advertiser, 'cj')
})

test('opportunity-first ranking: economic score can break close rank ties', () => {
  const ranked = rankOpportunityCandidates({
    candidates: [
      {
        offerId: 'partnerstack:offer:high_rank_low_econ',
        network: 'partnerstack',
        title: 'Creator suite tools trial',
        description: 'creator suite low payout trial',
        targetUrl: 'https://example.com/a',
        availability: 'active',
        quality: 0.9,
        bidHint: 0.2,
        policyWeight: 0.2,
        lexicalScore: 0.8,
        vectorScore: 0.78,
        fusedScore: 0.81,
      },
      {
        offerId: 'partnerstack:offer:slightly_lower_rank_high_econ',
        network: 'partnerstack',
        title: 'Creator suite annual plan',
        description: 'higher payout',
        targetUrl: 'https://example.com/b',
        availability: 'active',
        quality: 0.86,
        bidHint: 9.5,
        policyWeight: 0.2,
        lexicalScore: 0.77,
        vectorScore: 0.76,
        fusedScore: 0.78,
      },
    ],
    placementId: 'chat_from_answer_v1',
    query: 'best creator suite deals',
    answerText: '',
    intentScore: 0.82,
  })

  assert.equal(ranked.reasonCode, 'served')
  assert.equal(Boolean(ranked.winner), true)
  assert.equal(ranked.winner.offerId, 'partnerstack:offer:slightly_lower_rank_high_econ')
  assert.equal(ranked.ranked[0].auctionScore > ranked.ranked[1].auctionScore, true)
  assert.equal(ranked.ranked[0].rankScore < ranked.ranked[1].rankScore, true)
})

test('opportunity-first ranking: high relevance still wins when rank gap is large', () => {
  const ranked = rankOpportunityCandidates({
    candidates: [
      {
        offerId: 'partnerstack:offer:very_relevant_low_econ',
        network: 'partnerstack',
        title: 'Creator suite trusted offer',
        description: 'high relevance',
        targetUrl: 'https://example.com/c',
        availability: 'active',
        quality: 0.95,
        bidHint: 0.2,
        policyWeight: 0.2,
        lexicalScore: 0.94,
        vectorScore: 0.92,
        fusedScore: 0.93,
      },
      {
        offerId: 'partnerstack:offer:lower_relevance_high_econ',
        network: 'partnerstack',
        title: 'Creator suite annual plan',
        description: 'higher payout',
        targetUrl: 'https://example.com/d',
        availability: 'active',
        quality: 0.86,
        bidHint: 9.5,
        policyWeight: 0.2,
        lexicalScore: 0.77,
        vectorScore: 0.76,
        fusedScore: 0.78,
      },
    ],
    placementId: 'chat_from_answer_v1',
    query: 'best creator suite deals',
    answerText: '',
    intentScore: 0.82,
  })

  assert.equal(ranked.reasonCode, 'served')
  assert.equal(Boolean(ranked.winner), true)
  assert.equal(ranked.winner.offerId, 'partnerstack:offer:very_relevant_low_econ')
  assert.equal(ranked.ranked[0].rankScore > ranked.ranked[1].rankScore, true)
  assert.equal(ranked.ranked[0].auctionScore > ranked.ranked[1].auctionScore, true)
})

test('opportunity writer: state fallback records opportunity->delivery->event chain', async () => {
  const state = {}
  const requestContext = new Map()
  const writer = createOpportunityWriter({
    pool: null,
    state,
    requestContext,
  })

  const opportunity = await writer.createOpportunityRecord({
    requestId: 'req_chain_001',
    appId: 'sample-client-app',
    placementId: 'chat_from_answer_v1',
    payload: { query: 'vpn deals' },
  })
  assert.equal(Boolean(opportunity.opportunityKey), true)

  const delivery = await writer.writeDeliveryRecord({
    requestId: 'req_chain_001',
    appId: 'sample-client-app',
    placementId: 'chat_from_answer_v1',
    opportunityKey: opportunity.opportunityKey,
    deliveryStatus: 'served',
    payload: { reasonCode: 'served' },
  })
  assert.equal(delivery.deliveryStatus, 'served')

  const event = await writer.writeEventRecord({
    requestId: 'req_chain_001',
    appId: 'sample-client-app',
    placementId: 'chat_from_answer_v1',
    eventType: 'sdk_event',
    kind: 'click',
    eventStatus: 'recorded',
    payload: { click: true },
  })

  assert.equal(Boolean(event.eventKey), true)
  assert.equal(Array.isArray(state.opportunityRecords), true)
  assert.equal(Array.isArray(state.deliveryRecords), true)
  assert.equal(Array.isArray(state.opportunityEventRecords), true)

  const stored = state.opportunityRecords.find((item) => item.opportunityKey === opportunity.opportunityKey)
  assert.equal(Boolean(stored), true)
  assert.equal(stored.state, 'clicked')
})
