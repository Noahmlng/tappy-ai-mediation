import assert from 'node:assert/strict'
import test from 'node:test'

import { __publishSupabaseInternal } from '../../scripts/house-ads/publish-offers-to-supabase.js'

test('publish-to-supabase normalizes suspect brand status and optional price fields', () => {
  const brand = __publishSupabaseInternal.normalizeBrandRow({
    brand_id: 'brand_a',
    brand_name: 'Brand A',
    official_domain: 'brand-a.com',
    vertical_l1: 'sustainability',
    vertical_l2: 'eco_friendly_goods',
    status: 'suspect',
  })
  assert.equal(brand.status, 'inactive')

  const offerNoPrice = __publishSupabaseInternal.normalizeOfferRow({
    offer_id: 'offer_1',
    campaign_id: 'campaign_1',
    brand_id: 'brand_a',
    offer_type: 'product',
    vertical_l1: 'sustainability',
    vertical_l2: 'eco_friendly_goods',
    title: 'Eco Product',
    target_url: 'https://brand-a.com/p/1',
    status: 'active',
    source_type: 'real',
    confidence_score: 0.8,
    tags: ['eco'],
  })
  assert.equal(offerNoPrice.price, null)
  assert.equal(offerNoPrice.currency, 'USD')
  assert.deepEqual(offerNoPrice.tags_json, ['eco'])
})
