const REQUIRED_ENV_VARS = [
  'DEEPSEEK_API_KEY',
  'OPENROUTER_API_KEY',
  'OPENROUTER_MODEL',
  'CJ_TOKEN',
  'PARTNERSTACK_API_KEY'
]
const SUPPORTED_MEDIATION_NETWORKS = new Set(['partnerstack', 'cj', 'house'])
const DEFAULT_ENABLED_MEDIATION_NETWORKS = ['partnerstack', 'house']
const DEFAULT_LOCALE_MATCH_MODE = 'locale_or_base'
const SUPPORTED_LOCALE_MATCH_MODES = new Set(['exact', 'locale_or_base'])
const SUPPORTED_RELEVANCE_POLICY_MODES = new Set(['observe', 'shadow', 'enforce'])
const DEFAULT_RETRIEVAL_QUERY_MODE = 'latest_user_plus_entities'
const SUPPORTED_RETRIEVAL_QUERY_MODES = new Set([
  'latest_user_plus_entities',
  'recent_user_turns_concat',
])
const DEFAULT_RELEVANCE_THRESHOLDS = Object.freeze({
  chat_intent_recommendation_v1: { strict: 0.5, relaxed: 0.38 },
  chat_from_answer_v1: { strict: 0.58, relaxed: 0.44 },
})

function readEnv(env, key, { required = false } = {}) {
  const value = env[key]
  if (typeof value !== 'string' || value.trim().length === 0) {
    if (required) {
      throw new Error(`[config] Missing required environment variable: ${key}`)
    }
    return ''
  }
  return value.trim()
}

function toPositiveInteger(value, fallback) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric <= 0) return fallback
  return Math.floor(numeric)
}

function toNumberInRange(value, fallback, min = 0, max = 1) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return fallback
  if (numeric < min || numeric > max) return fallback
  return numeric
}

function parseBoolean(value, fallback = false) {
  const normalized = String(value ?? '').trim().toLowerCase()
  if (!normalized) return fallback
  if (['1', 'true', 'yes', 'on', 'enabled'].includes(normalized)) return true
  if (['0', 'false', 'no', 'off', 'disabled'].includes(normalized)) return false
  return fallback
}

function parseLocaleMatchMode(value) {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return DEFAULT_LOCALE_MATCH_MODE
  if (normalized === 'base_or_locale') return 'locale_or_base'
  if (!SUPPORTED_LOCALE_MATCH_MODES.has(normalized)) return DEFAULT_LOCALE_MATCH_MODE
  return normalized
}

function parseEnabledNetworks(rawValue) {
  const parsed = String(rawValue || '')
    .split(',')
    .map((item) => String(item || '').trim().toLowerCase())
    .filter((item) => item && SUPPORTED_MEDIATION_NETWORKS.has(item))
  const deduped = Array.from(new Set(parsed))
  return deduped.length > 0 ? deduped : [...DEFAULT_ENABLED_MEDIATION_NETWORKS]
}

function parseJsonObject(value, fallback = {}) {
  const text = String(value || '').trim()
  if (!text) return fallback
  try {
    const parsed = JSON.parse(text)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return fallback
    return parsed
  } catch {
    return fallback
  }
}

function parseRelevancePolicyMode(value) {
  const mode = String(value || '').trim().toLowerCase()
  if (!mode || !SUPPORTED_RELEVANCE_POLICY_MODES.has(mode)) return 'enforce'
  return mode
}

function parseRetrievalQueryMode(value) {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return DEFAULT_RETRIEVAL_QUERY_MODE
  if (normalized === 'latest_user') return DEFAULT_RETRIEVAL_QUERY_MODE
  if (normalized === 'recent_turns_concat') return 'recent_user_turns_concat'
  if (!SUPPORTED_RETRIEVAL_QUERY_MODES.has(normalized)) return DEFAULT_RETRIEVAL_QUERY_MODE
  return normalized
}

function normalizeThresholdsMap(raw = {}, defaults = DEFAULT_RELEVANCE_THRESHOLDS) {
  const source = raw && typeof raw === 'object' ? raw : {}
  const normalized = { ...defaults }

  for (const [placementId, row] of Object.entries(source)) {
    if (!row || typeof row !== 'object' || Array.isArray(row)) continue
    const strict = toNumberInRange(row.strict ?? row.strictThreshold, NaN)
    const relaxed = toNumberInRange(row.relaxed ?? row.relaxedThreshold, NaN)
    if (!Number.isFinite(strict) || !Number.isFinite(relaxed)) continue
    normalized[String(placementId)] = {
      strict,
      relaxed: Math.min(strict, relaxed),
    }
  }

  return normalized
}

export function loadRuntimeConfig(env = process.env, options = {}) {
  const strict = options?.strict === true
  const relevanceThresholdsJson = parseJsonObject(
    readEnv(env, 'MEDIATION_RELEVANCE_V2_THRESHOLDS_JSON', { required: false }),
    {},
  )
  const relevanceThresholds = normalizeThresholdsMap(relevanceThresholdsJson, DEFAULT_RELEVANCE_THRESHOLDS)

  return {
    deepseek: {
      apiKey: readEnv(env, 'DEEPSEEK_API_KEY', { required: strict }),
      model: readEnv(env, 'DEEPSEEK_MODEL', { required: false }) || 'deepseek-chat',
      baseUrl: readEnv(env, 'DEEPSEEK_BASE_URL', { required: false }) || 'https://api.deepseek.com/chat/completions',
      intentMaxTokens: toPositiveInteger(readEnv(env, 'DEEPSEEK_INTENT_MAX_TOKENS', { required: false }), 96),
    },
    openrouter: {
      apiKey: readEnv(env, 'OPENROUTER_API_KEY', { required: strict }),
      model: readEnv(env, 'OPENROUTER_MODEL', { required: strict })
    },
    cj: {
      token: readEnv(env, 'CJ_TOKEN', { required: strict })
    },
    partnerstack: {
      apiKey: readEnv(env, 'PARTNERSTACK_API_KEY', { required: strict })
    },
    houseAds: {
      source: readEnv(env, 'HOUSE_ADS_SOURCE', { required: false }) || 'supabase',
      dbCacheTtlMs: toPositiveInteger(readEnv(env, 'HOUSE_ADS_DB_CACHE_TTL_MS', { required: false }), 15000),
      dbFetchLimit: toPositiveInteger(readEnv(env, 'HOUSE_ADS_DB_FETCH_LIMIT', { required: false }), 1500),
      dbUrl: readEnv(env, 'SUPABASE_DB_URL', { required: false })
    },
    networkPolicy: {
      enabledNetworks: parseEnabledNetworks(readEnv(env, 'MEDIATION_ENABLED_NETWORKS', { required: false }))
    },
    languagePolicy: {
      localeMatchMode: parseLocaleMatchMode(readEnv(env, 'MEDIATION_LOCALE_MATCH_MODE', { required: false })),
    },
    retrievalPolicy: {
      lexicalTopK: toPositiveInteger(
        readEnv(env, 'MEDIATION_RETRIEVAL_LEXICAL_TOP_K', { required: false }),
        120,
      ),
      vectorTopK: toPositiveInteger(
        readEnv(env, 'MEDIATION_RETRIEVAL_VECTOR_TOP_K', { required: false }),
        120,
      ),
      finalTopK: toPositiveInteger(
        readEnv(env, 'MEDIATION_RETRIEVAL_FINAL_TOP_K', { required: false }),
        40,
      ),
      bm25RefreshIntervalMs: toPositiveInteger(
        readEnv(env, 'MEDIATION_BM25_REFRESH_INTERVAL_MS', { required: false }),
        10 * 60 * 1000,
      ),
      queryMode: parseRetrievalQueryMode(
        readEnv(env, 'MEDIATION_RETRIEVAL_QUERY_MODE', { required: false }),
      ),
      sparseQueryMaxTokens: toPositiveInteger(
        readEnv(env, 'MEDIATION_RETRIEVAL_SPARSE_QUERY_MAX_TOKENS', { required: false }),
        18,
      ),
      assistantEntityMaxCount: toPositiveInteger(
        readEnv(env, 'MEDIATION_RETRIEVAL_ASSISTANT_ENTITY_MAX_COUNT', { required: false }),
        16,
      ),
      brandIntent: {
        houseMissPenalty: toNumberInRange(
          readEnv(env, 'MEDIATION_BRAND_INTENT_HOUSE_MISS_PENALTY', { required: false }),
          0.08,
          0,
          1,
        ),
        houseShareCap: toNumberInRange(
          readEnv(env, 'MEDIATION_BRAND_INTENT_HOUSE_SHARE_CAP', { required: false }),
          0.6,
          0,
          1,
        ),
      },
      hybrid: {
        strategy: 'rrf_then_linear',
        sparseWeight: toNumberInRange(
          readEnv(env, 'MEDIATION_HYBRID_SPARSE_WEIGHT', { required: false }),
          0.8,
          0,
          1,
        ),
        denseWeight: toNumberInRange(
          readEnv(env, 'MEDIATION_HYBRID_DENSE_WEIGHT', { required: false }),
          0.2,
          0,
          1,
        ),
      },
    },
    relevancePolicy: {
      minLexicalScore: toNumberInRange(
        readEnv(env, 'MEDIATION_INTENT_MIN_LEXICAL_SCORE', { required: false }),
        0.02,
      ),
      minVectorScore: toNumberInRange(
        readEnv(env, 'MEDIATION_INTENT_MIN_VECTOR_SCORE', { required: false }),
        0.14,
      ),
      intentScoreFloor: toNumberInRange(
        readEnv(env, 'MEDIATION_INTENT_SCORE_FLOOR', { required: false }),
        0.38,
      ),
      houseLowInfoFilterEnabled: parseBoolean(
        readEnv(env, 'MEDIATION_HOUSE_LOWINFO_FILTER_ENABLED', { required: false }),
        true,
      ),
      topicCoverageThreshold: toNumberInRange(
        readEnv(env, 'MEDIATION_TOPIC_COVERAGE_THRESHOLD', { required: false }),
        0.05,
      ),
      compositeGateStrict: toNumberInRange(
        readEnv(env, 'MEDIATION_COMPOSITE_GATE_STRICT', { required: false }),
        0.44,
      ),
      compositeGateRelaxed: toNumberInRange(
        readEnv(env, 'MEDIATION_COMPOSITE_GATE_RELAXED', { required: false }),
        0.36,
      ),
      compositeGateThresholdVersion: readEnv(
        env,
        'MEDIATION_COMPOSITE_GATE_THRESHOLD_VERSION',
        { required: false },
      ) || 'composite_single_gate_v1',
    },
    relevancePolicyV2: {
      enabled: parseBoolean(
        readEnv(env, 'MEDIATION_RELEVANCE_V2_ENABLED', { required: false }),
        true,
      ),
      mode: parseRelevancePolicyMode(
        readEnv(env, 'MEDIATION_RELEVANCE_V2_MODE', { required: false }) || 'enforce',
      ),
      thresholdVersion: readEnv(env, 'MEDIATION_RELEVANCE_V2_THRESHOLD_VERSION', { required: false })
        || 'v1_default_2026_03_01',
      sameVerticalFallbackEnabled: parseBoolean(
        readEnv(env, 'MEDIATION_RELEVANCE_V2_SAME_VERTICAL_FALLBACK_ENABLED', { required: false }),
        true,
      ),
      rolloutPercent: toPositiveInteger(
        readEnv(env, 'MEDIATION_RELEVANCE_V2_ROLLOUT_PERCENT', { required: false }),
        100,
      ),
      thresholds: relevanceThresholds,
      calibration: {
        enabled: parseBoolean(
          readEnv(env, 'MEDIATION_RELEVANCE_V2_CALIBRATION_ENABLED', { required: false }),
          true,
        ),
        minSamples: toPositiveInteger(
          readEnv(env, 'MEDIATION_RELEVANCE_V2_CALIBRATION_MIN_SAMPLES', { required: false }),
          200,
        ),
        maxDeltaPerDay: toNumberInRange(
          readEnv(env, 'MEDIATION_RELEVANCE_V2_CALIBRATION_MAX_DELTA_PER_DAY', { required: false }),
          0.03,
        ),
        snapshotPath: readEnv(env, 'MEDIATION_RELEVANCE_V2_CALIBRATION_SNAPSHOT_PATH', { required: false })
          || 'config/relevance-thresholds.snapshot.json',
      },
    },
  }
}

export { REQUIRED_ENV_VARS }
