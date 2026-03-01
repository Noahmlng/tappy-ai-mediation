import test from 'node:test'
import assert from 'node:assert/strict'

import { __buildOfferJobsInternal } from '../../scripts/house-ads/build-offer-jobs.js'
import { __synthesizeOffersInternal } from '../../scripts/house-ads/synthesize-offers.js'

test('build-offer-jobs resolves canonical landing URL from brand evidence', () => {
  const resolved = __buildOfferJobsInternal.resolveCanonicalLandingUrl(
    {
      evidence: {
        homepage_url: 'https://shop.example.com/products/abc?utm_source=test#hero',
      },
      official_domain: 'example.com',
    },
    'example.com',
  )
  assert.equal(resolved, 'https://shop.example.com/')
})

test('synthesize-offers prefers canonical landing URL for synthetic target URL', () => {
  const target = __synthesizeOffersInternal.resolveSyntheticTargetUrl({
    canonical_landing_url: 'https://brand.example.com/pricing?utm_source=tappy',
    official_domain: 'fallback.example.com',
  })
  assert.equal(target, 'https://brand.example.com/pricing')

  const fallback = __synthesizeOffersInternal.resolveSyntheticTargetUrl({
    canonical_landing_url: '',
    official_domain: 'fallback.example.com',
  })
  assert.equal(fallback, 'https://fallback.example.com/')
})

test('synthesize-offers generates verified image endpoint URL format', () => {
  const imageUrl = __synthesizeOffersInternal.buildSyntheticImageUrl(
    { official_domain: 'brand.example.com' },
    2,
  )
  assert.equal(imageUrl.startsWith('https://picsum.photos/seed/'), true)
  assert.equal(imageUrl.endsWith('/640/360'), true)
})

test('synthesize-offers uses canonical target URL and image endpoint in output offer', () => {
  const offer = __synthesizeOffersInternal.buildSyntheticOffer(
    {
      job_id: 'offer_job_123',
      brand_id: 'brand_123',
      brand_name: 'BrandName',
      canonical_landing_url: 'https://brand.example.com/',
      official_domain: 'brand.example.com',
      vertical_l1: 'developer_tools',
      vertical_l2: 'dev_platform',
      market: 'US',
      synthetic_hints: {
        keyword_seed: ['ai platform'],
      },
    },
    0,
    { min: 20, max: 80, currency: 'USD' },
  )

  assert.equal(offer.target_url, 'https://brand.example.com/')
  assert.equal(offer.image_url.startsWith('https://picsum.photos/seed/'), true)
  assert.equal(offer.image_url.endsWith('/640/360'), true)
  assert.equal(offer.offer_type, 'product')
  assert.equal(offer.source_type, 'synthetic')
})
