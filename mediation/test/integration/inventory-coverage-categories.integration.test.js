import assert from 'node:assert/strict'
import test from 'node:test'

import { __coverageCategoriesInternal } from '../../scripts/inventory/coverage-categories.js'

test('coverage-categories derives category/brand and detects missing price', () => {
  const row = {
    offer_id: 'house:product:offer_1',
    image_url: 'https://cdn.example.com/a.jpg',
    metadata: {
      verticalL1: 'consumer_electronics',
      verticalL2: 'wearables',
      brandId: 'brand_1',
      price_missing: true,
    },
  }

  assert.equal(__coverageCategoriesInternal.categoryKeyFromRow(row), 'consumer_electronics::wearables')
  assert.equal(__coverageCategoriesInternal.brandKeyFromRow(row), 'brand_1')
  assert.equal(__coverageCategoriesInternal.isPriceMissing(row), true)
  assert.equal(__coverageCategoriesInternal.looksLikeImageUrl(row.image_url), true)
})
