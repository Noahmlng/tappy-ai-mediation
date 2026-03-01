const ENABLED_CATEGORIES = Object.freeze([
  'health_wellness::fitness',
  'food_beverage::grocery_delivery',
  'food_beverage::meal_kits',
  'food_beverage::beverages',
  'mobility::electric_vehicles',
  'mobility::car_rentals',
  'mobility::car_accessories',
  'mobility::electric_transportation',
  'gaming_entertainment::movies_streaming',
  'gaming_entertainment::gaming',
  'gaming_entertainment::music',
  'consumer_electronics::wearables',
  'consumer_electronics::vr_ar',
  'consumer_electronics::smart_home_devices',
  'sustainability::eco_friendly_goods',
  'sustainability::renewable_energy',
])

const DEFERRED_SENSITIVE_CATEGORIES = Object.freeze([
  'health_wellness::mental_health',
  'health_wellness::supplements',
])

const SOURCE_POLICY_BY_CATEGORY = Object.freeze(
  Object.fromEntries(ENABLED_CATEGORIES.map((category) => [category, 'real_only'])),
)

const KEYWORD_BLOCKLIST = Object.freeze([
  'alcohol',
  'beer',
  'wine',
  'whisky',
  'whiskey',
  'vodka',
  'rum',
  'tequila',
  'gin',
  'liquor',
  'mental health',
  'therapy',
  'psychiatry',
  'psychotherapist',
  'supplement',
  'supplements',
  'vitamin',
  'weight loss pill',
  'fat burner',
])

function normalizeText(value) {
  return String(value || '').trim().toLowerCase()
}

export function categoryKey(verticalL1 = '', verticalL2 = '') {
  return `${normalizeText(verticalL1)}::${normalizeText(verticalL2)}`
}

export function isEnabledNewCategory(verticalL1 = '', verticalL2 = '') {
  return ENABLED_CATEGORIES.includes(categoryKey(verticalL1, verticalL2))
}

export function isDeferredSensitiveCategory(verticalL1 = '', verticalL2 = '') {
  return DEFERRED_SENSITIVE_CATEGORIES.includes(categoryKey(verticalL1, verticalL2))
}

export function sourcePolicyForCategory(verticalL1 = '', verticalL2 = '', fallback = 'default') {
  const key = categoryKey(verticalL1, verticalL2)
  return SOURCE_POLICY_BY_CATEGORY[key] || fallback
}

export function hasBlockedKeyword(value = '') {
  const text = normalizeText(value)
  if (!text) return false
  return KEYWORD_BLOCKLIST.some((keyword) => text.includes(keyword))
}

export {
  ENABLED_CATEGORIES,
  DEFERRED_SENSITIVE_CATEGORIES,
  SOURCE_POLICY_BY_CATEGORY,
  KEYWORD_BLOCKLIST,
}
