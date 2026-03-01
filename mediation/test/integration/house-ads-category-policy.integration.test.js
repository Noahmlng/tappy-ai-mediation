import assert from 'node:assert/strict'
import test from 'node:test'

import { VERTICAL_TAXONOMY } from '../../scripts/house-ads/lib/vertical-taxonomy.js'
import {
  ENABLED_CATEGORIES,
  DEFERRED_SENSITIVE_CATEGORIES,
  SOURCE_POLICY_BY_CATEGORY,
  isEnabledNewCategory,
  isDeferredSensitiveCategory,
  sourcePolicyForCategory,
} from '../../scripts/house-ads/lib/category-policy.js'

test('new categories are present in taxonomy and configured as real_only', () => {
  const taxonomyKeys = new Set(VERTICAL_TAXONOMY.map((item) => `${item.vertical_l1}::${item.vertical_l2}`))
  for (const category of ENABLED_CATEGORIES) {
    assert.equal(taxonomyKeys.has(category), true, `missing taxonomy category: ${category}`)
    assert.equal(SOURCE_POLICY_BY_CATEGORY[category], 'real_only')
  }
})

test('sensitive categories are deferred and not in enabled rollout list', () => {
  for (const category of DEFERRED_SENSITIVE_CATEGORIES) {
    assert.equal(ENABLED_CATEGORIES.includes(category), false)
  }

  assert.equal(isEnabledNewCategory('health_wellness', 'fitness'), true)
  assert.equal(isDeferredSensitiveCategory('health_wellness', 'supplements'), true)
  assert.equal(sourcePolicyForCategory('consumer_electronics', 'wearables', 'default'), 'real_only')
  assert.equal(sourcePolicyForCategory('finance', 'digital_banking', 'default'), 'default')
})
