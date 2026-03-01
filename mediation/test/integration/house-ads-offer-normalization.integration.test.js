import assert from 'node:assert/strict'
import test from 'node:test'

import { __normalizeOffersInternal } from '../../scripts/house-ads/normalize-offers.js'
import { __qaOffersInternal } from '../../scripts/house-ads/qa-offers.js'

test('normalize product offer accepts missing price and remains valid', () => {
  const signal = {
    brand_id: 'brand_example',
    brand_name: 'Example Brand',
    vertical_l1: 'consumer_electronics',
    vertical_l2: 'wearables',
    market: 'US',
    updated_at: '2026-03-01T00:00:00.000Z',
  }
  const candidate = {
    title: 'Example Smartwatch',
    snippet: 'Great smartwatch',
    target_url: 'https://example.com/products/watch',
    image_url: 'https://example.com/images/watch.jpg',
    currency: 'USD',
    availability: 'in_stock',
    confidence: 0.9,
  }

  const offer = __normalizeOffersInternal.normalizeProductOffer(signal, candidate, 1)
  assert.equal(Boolean(offer), true)
  assert.equal('price' in offer, false)

  const errors = __normalizeOffersInternal.validateOffer(offer)
  assert.equal(errors.includes('invalid_price'), false)
})

test('qa validates real_only category, sensitive block and image hard gate', async () => {
  const brandMap = new Map([
    ['brand_example', { official_domain: 'example.com' }],
  ])

  const rawOffer = {
    offer_id: 'offer_1',
    campaign_id: 'campaign_1',
    brand_id: 'brand_example',
    offer_type: 'product',
    vertical_l1: 'consumer_electronics',
    vertical_l2: 'wearables',
    market: 'US',
    title: 'Wearable Product',
    snippet: 'Snippet',
    target_url: 'https://example.com/products/watch',
    image_url: 'https://example.com/images/watch.jpg',
    status: 'active',
    language: 'en-US',
    disclosure: 'Sponsored',
    source_type: 'synthetic',
    confidence_score: 0.9,
    freshness_ttl_hours: 48,
    last_verified_at: '2026-03-01T00:00:00.000Z',
    product_id: 'prd_1',
    merchant: 'Example',
    currency: 'USD',
    availability: 'in_stock',
    tags: ['wearables'],
  }

  const withPriceMissingTag = __qaOffersInternal.ensurePriceMissingTag(rawOffer)
  assert.equal(withPriceMissingTag.tags.includes('price_missing'), true)

  const reasons = __qaOffersInternal.validateOffer(
    withPriceMissingTag,
    brandMap,
    {
      minConfidenceReal: 0.55,
      minConfidenceSynthetic: 0.6,
      sourcePolicy: 'real_only',
      imageHardGate: true,
      sensitiveBlock: true,
    },
    { valid_image: false, status_class: '4xx' },
  )
  assert.equal(reasons.includes('category_requires_real_source'), true)
  assert.equal(reasons.some((reason) => reason.startsWith('invalid_image_url:')), true)

  const sensitiveReasons = __qaOffersInternal.validateOffer(
    {
      ...withPriceMissingTag,
      vertical_l1: 'health_wellness',
      vertical_l2: 'supplements',
      title: 'Weight loss supplement',
      source_type: 'real',
    },
    brandMap,
    {
      minConfidenceReal: 0.55,
      minConfidenceSynthetic: 0.6,
      sourcePolicy: 'real_only',
      imageHardGate: false,
      sensitiveBlock: true,
    },
    null,
  )
  assert.equal(sensitiveReasons.includes('category_deferred_sensitive'), true)
  assert.equal(sensitiveReasons.includes('blocked_keyword_sensitive'), true)
})
