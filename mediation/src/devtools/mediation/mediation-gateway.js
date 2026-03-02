import { createHash } from 'node:crypto'
import http from 'node:http'
import { fileURLToPath } from 'node:url'

import defaultPlacements from '../../../config/default-placements.json' with { type: 'json' }
import { loadRuntimeConfig } from '../../config/runtime-config.js'
import { runAdsRetrievalPipeline } from '../../runtime/index.js'
import { getAllNetworkHealth } from '../../runtime/network-health-state.js'
import { inferIntentWithLlm } from '../../providers/intent/index.js'
import { scoreIntentOpportunityFirst } from '../../runtime/intent-scoring.js'
import { retrieveOpportunityCandidates } from '../../runtime/opportunity-retrieval.js'
import { rankOpportunityCandidates, mapRankedCandidateToBid } from '../../runtime/opportunity-ranking.js'
import { runGlobalPlacementAuction } from '../../runtime/global-placement-auction.js'
import { createOpportunityWriter } from '../../runtime/opportunity-writer.js'
import {
  syncInventoryNetworks,
  getInventoryStatus,
  buildInventoryEmbeddings,
  materializeServingSnapshot,
} from '../../runtime/inventory-sync.js'
import {
  createIntentCardVectorIndex,
  normalizeIntentCardCatalogItems,
  retrieveIntentCardTopK,
} from '../../providers/intent-card/index.js'
import { normalizeUnifiedOffers } from '../../offers/index.js'
import { handleRuntimeRoutes } from './runtime-routes.js'
import { handleControlPlaneRoutes } from './control-plane-routes.js'

function readEnvValue(env, key, fallback = '') {
  const value = env[key]
  if (typeof value === 'string' && value.trim().length > 0) {
    return value.trim()
  }
  return String(fallback || '').trim()
}

function parseFeatureSwitch(value, fallback = true) {
  const normalized = String(value ?? '').trim().toLowerCase()
  if (!normalized) return fallback
  if (['1', 'true', 'on', 'yes', 'enabled'].includes(normalized)) return true
  if (['0', 'false', 'off', 'no', 'disabled'].includes(normalized)) return false
  return fallback
}

function parseEnforcementMode(value, fallback = 'on') {
  const normalized = String(value ?? '').trim().toLowerCase()
  if (!normalized) return fallback
  if (normalized === 'on' || normalized === 'enabled') return 'on'
  if (normalized === 'off' || normalized === 'disabled') return 'off'
  if (normalized === 'monitor' || normalized === 'monitor_only' || normalized === 'observe') return 'monitor_only'
  return fallback
}

function parseRelevancePolicyMode(value, fallback = 'enforce') {
  const normalized = String(value ?? '').trim().toLowerCase()
  if (!normalized) return fallback
  if (!RELEVANCE_POLICY_MODES.has(normalized)) return fallback
  return normalized
}

function normalizeCorsOrigin(value) {
  const raw = String(value || '').trim()
  if (!raw) return ''
  let parsed
  try {
    parsed = new URL(raw)
  } catch {
    return ''
  }
  const protocol = String(parsed.protocol || '').toLowerCase()
  if (protocol !== 'http:' && protocol !== 'https:') return ''
  return parsed.origin
}

function parseCorsOriginList(input) {
  const pieces = Array.isArray(input)
    ? input
    : String(input || '')
      .split(',')
      .map((item) => String(item || '').trim())
  const dedup = new Set()
  const origins = []
  for (const value of pieces) {
    const normalized = normalizeCorsOrigin(value)
    if (!normalized || dedup.has(normalized)) continue
    dedup.add(normalized)
    origins.push(normalized)
  }
  return origins
}

function parseRequiredCoreInventoryNetworks(input) {
  const allowed = new Set(['partnerstack', 'cj', 'house'])
  const fallback = ['partnerstack', 'house']
  const raw = String(input || '')
    .split(',')
    .map((item) => String(item || '').trim().toLowerCase())
    .filter(Boolean)
  const dedup = []
  for (const network of raw) {
    if (!allowed.has(network)) continue
    if (dedup.includes(network)) continue
    dedup.push(network)
  }
  return dedup.length > 0 ? dedup : fallback
}

function loadProductionGatewayConfig(env = process.env, options = {}) {
  const strict = options?.strict !== false
  const supabaseDbUrl = readEnvValue(env, 'SUPABASE_DB_URL', '')
  if (strict && !supabaseDbUrl) {
    throw new Error('SUPABASE_DB_URL is required in production mode.')
  }

  const allowedCorsOrigins = parseCorsOriginList(readEnvValue(env, 'MEDIATION_ALLOWED_ORIGINS', ''))
  if (strict && allowedCorsOrigins.length === 0) {
    throw new Error('MEDIATION_ALLOWED_ORIGINS must include at least one allowed origin.')
  }

  return {
    supabaseDbUrl,
    allowedCorsOrigins,
  }
}

function assertProductionGatewayConfig(config = GATEWAY_CONFIG) {
  const supabaseDbUrl = String(config?.supabaseDbUrl || '').trim()
  if (!supabaseDbUrl) {
    throw new Error('SUPABASE_DB_URL is required in production mode.')
  }
  const allowedCorsOrigins = Array.isArray(config?.allowedCorsOrigins)
    ? config.allowedCorsOrigins
    : []
  if (allowedCorsOrigins.length === 0) {
    throw new Error('MEDIATION_ALLOWED_ORIGINS must include at least one allowed origin.')
  }
}

const GATEWAY_CONFIG = loadProductionGatewayConfig(process.env, { strict: false })
const REQUEST_BASE_ORIGIN = 'http://mediation.local'
const SETTLEMENT_DB_URL = GATEWAY_CONFIG.supabaseDbUrl
const SETTLEMENT_FACT_TABLE = 'mediation_settlement_conversion_facts'
const RUNTIME_DECISION_LOG_TABLE = 'mediation_runtime_decision_logs'
const RUNTIME_EVENT_LOG_TABLE = 'mediation_runtime_event_logs'
const CONTROL_PLANE_APPS_TABLE = 'control_plane_apps'
const CONTROL_PLANE_APP_ENVIRONMENTS_TABLE = 'control_plane_app_environments'
const CONTROL_PLANE_API_KEYS_TABLE = 'control_plane_api_keys'
const CONTROL_PLANE_DASHBOARD_USERS_TABLE = 'control_plane_dashboard_users'
const CONTROL_PLANE_DASHBOARD_SESSIONS_TABLE = 'control_plane_dashboard_sessions'
const CONTROL_PLANE_INTEGRATION_TOKENS_TABLE = 'control_plane_integration_tokens'
const CONTROL_PLANE_AGENT_ACCESS_TOKENS_TABLE = 'control_plane_agent_access_tokens'
const CONTROL_PLANE_ALLOWED_ORIGINS_TABLE = 'control_plane_allowed_origins'
const CAMPAIGNS_TABLE = 'campaigns'
const CAMPAIGN_BUDGET_LIMITS_TABLE = 'campaign_budget_limits'
const BUDGET_RESERVATIONS_TABLE = 'budget_reservations'
const BUDGET_LEDGER_TABLE = 'budget_ledger'
const PLACEMENT_ID_MIGRATION_TABLES = Object.freeze([
  { table: CONTROL_PLANE_INTEGRATION_TOKENS_TABLE, column: 'placement_id' },
  { table: CONTROL_PLANE_AGENT_ACCESS_TOKENS_TABLE, column: 'placement_id' },
  { table: RUNTIME_DECISION_LOG_TABLE, column: 'placement_id' },
  { table: RUNTIME_EVENT_LOG_TABLE, column: 'placement_id' },
  { table: SETTLEMENT_FACT_TABLE, column: 'placement_id' },
])

const DB_POOL_MAX = 1
const DB_POOL_IDLE_TIMEOUT_MS = 10000
const DB_POOL_CONNECTION_TIMEOUT_MS = 5000
const REQUIRE_DURABLE_SETTLEMENT = true
const STRICT_MANUAL_INTEGRATION = true
const REQUIRE_RUNTIME_LOG_DB_PERSISTENCE = true
const MAX_DECISION_LOGS = 500
const MAX_EVENT_LOGS = 500
const MAX_PLACEMENT_AUDIT_LOGS = 500
const MAX_NETWORK_FLOW_LOGS = 300
const MAX_CONTROL_PLANE_AUDIT_LOGS = 800
const MAX_INTEGRATION_TOKENS = 500
const MAX_AGENT_ACCESS_TOKENS = 1200
const MAX_DASHBOARD_USERS = 500
const MAX_DASHBOARD_SESSIONS = 1500
const CONTROL_PLANE_REFRESH_THROTTLE_MS = 1000
const DECISION_REASON_ENUM = new Set(['served', 'no_fill', 'blocked', 'error'])
const RELEVANCE_POLICY_MODES = new Set(['observe', 'shadow', 'enforce'])
const CONTROL_PLANE_ENVIRONMENTS = new Set(['prod'])
const CONTROL_PLANE_KEY_STATUS = new Set(['active', 'revoked'])
const DEFAULT_CONTROL_PLANE_APP_ID = ''
const DEFAULT_CONTROL_PLANE_ORG_ID = ''
const TRACKING_ACCOUNT_QUERY_PARAM = 'aid'
const DASHBOARD_SESSION_PREFIX = 'dsh_'
const DASHBOARD_SESSION_TTL_SECONDS = 86400 * 30
const DASHBOARD_AUTH_REQUIRED = true
const RUNTIME_AUTH_REQUIRED = true
const INVENTORY_FALLBACK_WHEN_UNAVAILABLE = true
const INVENTORY_READINESS_CACHE_TTL_MS = 30_000
const CORE_INVENTORY_NETWORKS = Object.freeze(
  parseRequiredCoreInventoryNetworks(process.env.MEDIATION_REQUIRED_CORE_NETWORKS),
)
const SUPPORTED_MEDIATION_NETWORKS = new Set(['partnerstack', 'cj', 'house'])
const DEFAULT_ENABLED_MEDIATION_NETWORKS = Object.freeze(['partnerstack', 'house'])
const INVENTORY_SYNC_COMMAND = 'npm --prefix ./mediation run inventory:sync:all'
const MIN_AGENT_ACCESS_TTL_SECONDS = 60
const MAX_AGENT_ACCESS_TTL_SECONDS = 900
const TOKEN_EXCHANGE_FORBIDDEN_FIELDS = new Set([
  'appId',
  'app_id',
  'environment',
  'env',
  'placementId',
  'placement_id',
  'scope',
  'sourceTokenId',
  'source_token_id',
  'tokenType',
  'token_type',
])
const BOOTSTRAP_ALLOWED_CORS_ORIGINS = GATEWAY_CONFIG.allowedCorsOrigins
const API_SERVICE_ROLE = 'all'
const RUNTIME_ROUTE_MATCHERS = Object.freeze([
  { type: 'prefix', value: '/api/v1/mediation/' },
  { type: 'prefix', value: '/api/v1/sdk/' },
  { type: 'prefix', value: '/api/v2/' },
  { type: 'prefix', value: '/api/v1/intent-card/' },
])
const CONTROL_PLANE_ROUTE_MATCHERS = Object.freeze([
  { type: 'prefix', value: '/api/v1/public/' },
  { type: 'prefix', value: '/api/v1/dashboard/' },
  { type: 'prefix', value: '/api/v1/internal/inventory/' },
])

function normalizeApiServiceRole(value) {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/-/g, '_')
  if (normalized === 'runtime') return 'runtime'
  if (normalized === 'control_plane' || normalized === 'control') return 'control_plane'
  return 'all'
}

function routeMatches(pathname, matcher) {
  if (!matcher || typeof matcher !== 'object') return false
  const target = String(matcher.value || '').trim()
  if (!target) return false
  if (matcher.type === 'exact') return pathname === target
  if (matcher.type === 'prefix') return pathname.startsWith(target)
  return false
}

function resolveRoutePlane(pathname) {
  if (pathname === '/api/health') return 'shared'
  if (RUNTIME_ROUTE_MATCHERS.some((matcher) => routeMatches(pathname, matcher))) return 'runtime'
  if (CONTROL_PLANE_ROUTE_MATCHERS.some((matcher) => routeMatches(pathname, matcher))) return 'control_plane'
  return 'unknown'
}

function isRouteAllowedForServiceRole(pathname, role) {
  const normalizedRole = normalizeApiServiceRole(role)
  const routePlane = resolveRoutePlane(pathname)
  if (routePlane === 'shared') return true
  if (normalizedRole === 'all') return true
  if (routePlane === 'unknown') return false
  return routePlane === normalizedRole
}

const PLACEMENT_ID_FROM_ANSWER = 'chat_from_answer_v1'
const PLACEMENT_ID_INTENT_RECOMMENDATION = 'chat_intent_recommendation_v1'
const LEGACY_PLACEMENT_ID_MAP = Object.freeze({
  chat_inline_v1: PLACEMENT_ID_FROM_ANSWER,
  chat_followup_v1: PLACEMENT_ID_INTENT_RECOMMENDATION,
})
const PLACEMENT_KEY_BY_ID = {
  [PLACEMENT_ID_FROM_ANSWER]: 'attach.post_answer_render',
  [PLACEMENT_ID_INTENT_RECOMMENDATION]: 'next_step.intent_card',
  search_parallel_v1: 'intervention.search_parallel',
}

const EVENT_SURFACE_MAP = {
  answer_completed: 'CHAT_INLINE',
  followup_generation: 'FOLLOW_UP',
  follow_up_generation: 'FOLLOW_UP',
  web_search_called: 'AGENT_PANEL',
}

const ATTACH_MVP_PLACEMENT_KEY = 'attach.post_answer_render'
const ATTACH_MVP_EVENT = 'answer_completed'
const NEXT_STEP_INTENT_CARD_PLACEMENT_KEY = 'next_step.intent_card'
const V2_BID_EVENT = 'v2_bid_request'
const CPC_PRICING_SEMANTICS_VERSION = 'cpc_v1'
const BUDGET_ENFORCEMENT_MODE = parseEnforcementMode(process.env.BUDGET_ENFORCEMENT, 'off')
const RISK_ENFORCEMENT_MODE = parseEnforcementMode(process.env.RISK_ENFORCEMENT, 'off')
const BUDGET_RESERVATION_TTL_MS = 15 * 60 * 1000
const V2_BID_BUDGET_MS = Object.freeze({
  intent: 300,
  retrieval: 350,
  ranking: 200,
  delivery: 200,
  total: 1000,
})
const V2_BID_ALLOWED_FIELDS = new Set([
  'userId',
  'chatId',
  'placementId',
  'messages',
])
const V2_BID_MESSAGE_ALLOWED_FIELDS = new Set([
  'role',
  'content',
  'timestamp',
])
const V2_BID_MESSAGE_ROLES = new Set(['user', 'assistant', 'system'])
const DEFAULT_RETRIEVAL_QUERY_MODE = 'latest_user_plus_entities'
const RETRIEVAL_QUERY_MODES = new Set([
  DEFAULT_RETRIEVAL_QUERY_MODE,
  'recent_user_turns_concat',
])
const RETRIEVAL_ENTITY_STOPWORDS = new Set([
  'the', 'and', 'for', 'with', 'from', 'that', 'this', 'those', 'these', 'have', 'will', 'your', 'about', 'which',
  'what', 'when', 'where', 'who', 'how', 'why', 'would', 'could', 'should', 'very', 'more', 'most',
  'best', 'better', 'recommend', 'recommendation', 'recommendations', 'compare', 'comparison', 'price', 'prices',
  'pricing', 'deal', 'deals', 'tool', 'tools', 'platform', 'platforms',
  'i', 'me', 'my', 'mine', 'we', 'our', 'ours', 'he', 'him', 'his', 'she', 'her', 'hers', 'they', 'them',
  'wants', 'want', 'need', 'needs', 'girlfriend', 'boyfriend', 'wife', 'husband', 'partner',
  'assistant', 'user', 'please', 'thanks',
  '推荐', '比较', '对比', '价格', '优惠', '哪个好', '什么', '怎么', '可以', '帮我', '一下',
])
const RETRIEVAL_ASSISTANT_ENTITY_STOPWORDS = new Set([
  ...RETRIEVAL_ENTITY_STOPWORDS,
  'fastest', 'popular', 'workflow', 'voice', 'dubbing', 'translation', 'translate', 'upload', 'video', 'videos',
  'language', 'languages', 'quality', 'creator', 'creators', 'content', 'market', 'leader', 'free', 'trial', 'paid',
  'option', 'options', 'step', 'steps', 'core', 'difference', 'gold', 'standard', 'model', 'models', 'all', 'one',
  'platform', 'platforms', 'tool', 'tools', 'solution', 'solutions', 'app', 'apps',
  'category', 'categories', 'automated', 'easiest', 'easy', 'recommended', 'approach', 'approaches', 'method', 'methods',
  'breakdown', 'best', 'top', 'tier',
  'assistant',
  '人工', '智能', '视频', '翻译', '配音', '工具', '平台', '方案',
])
const MANAGED_ROUTING_MODE = 'managed_mediation'
const NEXT_STEP_INTENT_CARD_EVENTS = new Set(['followup_generation', 'follow_up_generation'])
const POSTBACK_EVENT_TYPES = new Set(['postback'])
const POSTBACK_TYPES = new Set(['conversion'])
const POSTBACK_STATUS = new Set(['pending', 'success', 'failed'])
const CONVERSION_FACT_TYPES = Object.freeze({
  CPA: 'cpa_conversion',
  CPC: 'cpc_click',
})
const NEXT_STEP_INTENT_CLASSES = new Set([
  'shopping',
  'purchase_intent',
  'gifting',
  'product_exploration',
  'non_commercial',
  'other',
])
const NEXT_STEP_INTENT_POST_RULES = Object.freeze({
  intentThresholdFloor: 0.35,
  cooldownSeconds: 20,
  maxPerSession: 2,
  maxPerUserPerDay: 5,
})
const DEFAULT_RISK_RULES = Object.freeze({
  clickBurstWindowSec: 30,
  clickBurstLimit: 6,
  duplicateClickWindowSec: 300,
  ctrWarnThreshold: 0.25,
  ctrBlockThreshold: 0.45,
  ctrMinImpressions: 12,
  degradeMultiplier: 0.7,
})
const NEXT_STEP_SENSITIVE_TOPICS = [
  'medical',
  'medicine',
  'health diagnosis',
  'legal',
  'lawsuit',
  'self-harm',
  'suicide',
  'minor',
  'underage',
  'adult',
  'gambling',
  'drug',
  'diagnosis',
  '处方',
  '医疗',
  '法律',
  '未成年',
  '自残',
  '自杀',
  '赌博',
  '毒品',
  '成人',
]
const ATTACH_MVP_ALLOWED_FIELDS = new Set([
  'requestId',
  'appId',
  'app_id',
  'accountId',
  'account_id',
  'sessionId',
  'turnId',
  'query',
  'answerText',
  'intentScore',
  'locale',
  'kind',
  'adId',
  'placementId',
])
const NEXT_STEP_INTENT_CARD_ALLOWED_FIELDS = new Set([
  'requestId',
  'appId',
  'app_id',
  'accountId',
  'account_id',
  'sessionId',
  'turnId',
  'userId',
  'event',
  'placementId',
  'placementKey',
  'kind',
  'adId',
  'context',
])
const NEXT_STEP_INTENT_CARD_CONTEXT_ALLOWED_FIELDS = new Set([
  'query',
  'answerText',
  'recent_turns',
  'locale',
  'intent_class',
  'intent_score',
  'preference_facets',
  'constraints',
  'blocked_topics',
  'expected_revenue',
  'debug',
])
const INTENT_CARD_RETRIEVE_ALLOWED_FIELDS = new Set([
  'query',
  'facets',
  'topK',
  'minScore',
  'catalog',
])
const POSTBACK_CONVERSION_ALLOWED_FIELDS = new Set([
  'eventType',
  'event',
  'kind',
  'requestId',
  'appId',
  'accountId',
  'account_id',
  'sessionId',
  'turnId',
  'userId',
  'placementId',
  'placementKey',
  'adId',
  'postbackType',
  'postbackStatus',
  'conversionId',
  'conversion_id',
  'eventSeq',
  'eventAt',
  'event_at',
  'currency',
  'cpaUsd',
  'cpa_usd',
  'payoutUsd',
  'payout_usd',
])
const DASHBOARD_REGISTER_ALLOWED_FIELDS = new Set([
  'email',
  'password',
  'displayName',
  'display_name',
  'accountId',
  'account_id',
  'appId',
  'app_id',
])
const DASHBOARD_LOGIN_ALLOWED_FIELDS = new Set([
  'email',
  'password',
])

const runtimeMemory = {
  cooldownBySessionPlacement: new Map(),
  perSessionPlacementCount: new Map(),
  perUserPlacementDayCount: new Map(),
  opportunityContextByRequest: new Map(),
  inventoryReadinessSummary: null,
  inventoryReadinessCheckedAtMs: 0,
  risk: {
    config: { ...DEFAULT_RISK_RULES },
    clickBurstByActor: new Map(),
    clickSeenByRequestAd: new Map(),
    campaignPerfById: new Map(),
  },
}

function nowIso() {
  return new Date().toISOString()
}

function createId(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
}

function parseCollectionLimit(value, fallback) {
  const raw = String(value ?? '').trim()
  if (!raw) return Math.max(0, Math.floor(Number(fallback) || 0))
  const n = Number(raw)
  if (!Number.isFinite(n)) return Math.max(0, Math.floor(Number(fallback) || 0))
  if (n <= 0) return 0
  return Math.floor(n)
}

function round(value, digits = 4) {
  const factor = 10 ** digits
  return Math.round(value * factor) / factor
}

function applyCollectionLimit(rows = [], limit = 0) {
  const list = Array.isArray(rows) ? rows : []
  if (!Number.isFinite(limit) || limit <= 0) return list
  return list.slice(0, Math.floor(limit))
}

const settlementStore = {
  mode: 'supabase',
  pool: null,
  initPromise: null,
}

const corsOriginState = {
  origins: [...BOOTSTRAP_ALLOWED_CORS_ORIGINS],
  originSet: new Set(BOOTSTRAP_ALLOWED_CORS_ORIGINS),
}

const controlPlaneRefreshState = {
  lastLoadedAt: 0,
  refreshPromise: null,
}

function isSupabaseSettlementStore() {
  return settlementStore.mode === 'supabase' && Boolean(settlementStore.pool)
}

function isPostgresSettlementStore() {
  return isSupabaseSettlementStore()
}

function setAllowedCorsOrigins(originsInput = []) {
  const origins = parseCorsOriginList(originsInput)
  if (origins.length === 0) {
    throw new Error('At least one allowed CORS origin is required.')
  }
  corsOriginState.origins = origins
  corsOriginState.originSet = new Set(origins)
  return origins
}

function getAllowedCorsOrigins() {
  return Array.isArray(corsOriginState.origins) ? [...corsOriginState.origins] : []
}

async function listAllowedCorsOriginsFromSupabase(pool = null) {
  const db = pool || settlementStore.pool
  if (!db) return []
  const result = await db.query(`
    SELECT origin, created_at, updated_at
    FROM ${CONTROL_PLANE_ALLOWED_ORIGINS_TABLE}
    ORDER BY created_at ASC, origin ASC
  `)
  const rows = Array.isArray(result.rows) ? result.rows : []
  const dedup = new Set()
  const items = []
  for (const row of rows) {
    const origin = normalizeCorsOrigin(row?.origin)
    if (!origin || dedup.has(origin)) continue
    dedup.add(origin)
    items.push({
      origin,
      createdAt: normalizeDbTimestamp(row?.created_at, nowIso()),
      updatedAt: normalizeDbTimestamp(row?.updated_at, nowIso()),
    })
  }
  return items
}

async function replaceAllowedCorsOriginsInSupabase(originsInput = [], pool = null) {
  const db = pool || settlementStore.pool
  const origins = parseCorsOriginList(originsInput)
  if (origins.length === 0) {
    throw new Error('origins must include at least one valid http/https origin.')
  }
  if (!db) {
    setAllowedCorsOrigins(origins)
    return origins.map((origin) => ({
      origin,
      createdAt: nowIso(),
      updatedAt: nowIso(),
    }))
  }

  const client = typeof db.connect === 'function' ? await db.connect() : null
  const runner = client || db

  try {
    await runner.query('BEGIN')
    await runner.query(`DELETE FROM ${CONTROL_PLANE_ALLOWED_ORIGINS_TABLE}`)
    for (const origin of origins) {
      await runner.query(
        `
          INSERT INTO ${CONTROL_PLANE_ALLOWED_ORIGINS_TABLE} (
            origin,
            created_at,
            updated_at
          )
          VALUES ($1, NOW(), NOW())
        `,
        [origin],
      )
    }
    await runner.query('COMMIT')
  } catch (error) {
    await runner.query('ROLLBACK').catch(() => {})
    throw error
  } finally {
    if (client) client.release()
  }

  const loaded = await listAllowedCorsOriginsFromSupabase(db)
  setAllowedCorsOrigins(loaded.map((item) => item.origin))
  return loaded
}

async function refreshAllowedCorsOriginsFromSupabase(pool = null) {
  const db = pool || settlementStore.pool
  if (!db) return false
  const rows = await listAllowedCorsOriginsFromSupabase(db)
  if (rows.length > 0) {
    setAllowedCorsOrigins(rows.map((item) => item.origin))
    return true
  }
  await replaceAllowedCorsOriginsInSupabase(BOOTSTRAP_ALLOWED_CORS_ORIGINS, db)
  return true
}

function normalizeAllowedCorsOriginsPayload(payload, fieldName = 'origins') {
  const source = payload && typeof payload === 'object' ? payload : {}
  const raw = source.origins
  if (raw === undefined || raw === null) {
    throw new Error(`${fieldName} is required.`)
  }
  const normalized = parseCorsOriginList(raw)
  if (normalized.length === 0) {
    throw new Error(`${fieldName} must include at least one valid http/https origin.`)
  }
  const sourceValues = Array.isArray(raw)
    ? raw
    : String(raw).split(',')
  for (const value of sourceValues) {
    const text = String(value || '').trim()
    if (!text) continue
    if (!normalizeCorsOrigin(text)) {
      throw new Error(`Invalid origin: ${text}`)
    }
  }
  return normalized
}

async function refreshControlPlaneStateFromStore(options = {}) {
  if (!isSupabaseSettlementStore()) return false

  const force = options?.force === true
  const now = Date.now()
  const elapsedMs = now - toPositiveInteger(controlPlaneRefreshState.lastLoadedAt, 0)
  if (!force && elapsedMs >= 0 && elapsedMs < CONTROL_PLANE_REFRESH_THROTTLE_MS) {
    return false
  }

  if (controlPlaneRefreshState.refreshPromise) {
    await controlPlaneRefreshState.refreshPromise
    return true
  }

  controlPlaneRefreshState.refreshPromise = (async () => {
    await Promise.all([
      loadControlPlaneStateFromSupabase(),
      refreshAllowedCorsOriginsFromSupabase(),
    ])
    controlPlaneRefreshState.lastLoadedAt = Date.now()
    return true
  })()

  try {
    await controlPlaneRefreshState.refreshPromise
    return true
  } catch (error) {
    if (!force) {
      console.error(
        '[mediation-gateway] control plane refresh skipped due to transient error:',
        error instanceof Error ? error.message : String(error),
      )
      return false
    }
    throw error
  } finally {
    controlPlaneRefreshState.refreshPromise = null
  }
}

function createOpportunityChainWriter() {
  return createOpportunityWriter({
    pool: isPostgresSettlementStore() ? settlementStore.pool : null,
    state,
    requestContext: runtimeMemory.opportunityContextByRequest,
  })
}

function isLlmIntentFallbackEnabled() {
  return true
}

function resolveRuntimeEnabledNetworks(runtimeConfigInput = null) {
  const runtimeConfig = runtimeConfigInput && typeof runtimeConfigInput === 'object'
    ? runtimeConfigInput
    : loadRuntimeConfig(process.env, { strict: false })
  const configured = Array.isArray(runtimeConfig?.networkPolicy?.enabledNetworks)
    ? runtimeConfig.networkPolicy.enabledNetworks
    : []
  const normalized = Array.from(
    new Set(
      configured
        .map((item) => String(item || '').trim().toLowerCase())
        .filter((item) => SUPPORTED_MEDIATION_NETWORKS.has(item))
    )
  )
  return normalized.length > 0 ? normalized : [...DEFAULT_ENABLED_MEDIATION_NETWORKS]
}

function deriveInventoryNetworksFromPlacement(placement = {}, runtimeConfig = null) {
  const runtimeEnabledNetworks = resolveRuntimeEnabledNetworks(runtimeConfig)
  const runtimeEnabledSet = new Set(runtimeEnabledNetworks)
  const bidders = Array.isArray(placement?.bidders) ? placement.bidders : []
  const enabled = bidders
    .filter((item) => item?.enabled !== false)
    .map((item) => String(item?.networkId || '').trim().toLowerCase())
    .filter((item) => ['partnerstack', 'cj', 'house'].includes(item))
  const candidates = enabled.length === 0
    ? ['partnerstack', 'cj', 'house']
    : [...enabled]
  if (
    placement?.fallback?.store?.enabled === true
    && !candidates.includes('house')
  ) {
    candidates.push('house')
  }
  const filtered = candidates.filter((item) => runtimeEnabledSet.has(item))
  if (filtered.length > 0) return filtered
  return runtimeEnabledNetworks
}

function isInventoryFallbackEnabled() {
  return INVENTORY_FALLBACK_WHEN_UNAVAILABLE
}

function resolveRelevancePolicyThresholdsByPlacement(runtimeConfig = null) {
  const thresholds = runtimeConfig?.relevancePolicyV2?.thresholds
    && typeof runtimeConfig.relevancePolicyV2.thresholds === 'object'
    ? runtimeConfig.relevancePolicyV2.thresholds
    : {}
  return thresholds
}

function computeDeterministicRolloutBucket(seed = '') {
  const digest = createHash('sha1').update(String(seed || '')).digest('hex').slice(0, 8)
  const parsed = Number.parseInt(digest, 16)
  if (!Number.isFinite(parsed)) return 0
  return parsed % 100
}

function resolvePlacementRelevancePolicyV2({ requestId = '', placementId = '', placement = null, runtimeConfig = null }) {
  const runtimePolicy = runtimeConfig?.relevancePolicyV2 && typeof runtimeConfig.relevancePolicyV2 === 'object'
    ? runtimeConfig.relevancePolicyV2
    : {}
  const placementPolicy = placement?.relevancePolicyV2 && typeof placement.relevancePolicyV2 === 'object'
    ? placement.relevancePolicyV2
    : {}
  const thresholdsByPlacement = resolveRelevancePolicyThresholdsByPlacement(runtimeConfig)
  const placementThreshold = thresholdsByPlacement[String(placementId || '')]
    && typeof thresholdsByPlacement[String(placementId || '')] === 'object'
    ? thresholdsByPlacement[String(placementId || '')]
    : {}

  const strictThreshold = Number.isFinite(Number(placementPolicy.strictThreshold))
    ? clampNumber(placementPolicy.strictThreshold, 0, 1, 0.6)
    : clampNumber(placementThreshold.strict, 0, 1, 0.6)
  const relaxedThresholdRaw = Number.isFinite(Number(placementPolicy.relaxedThreshold))
    ? clampNumber(placementPolicy.relaxedThreshold, 0, 1, 0.46)
    : clampNumber(placementThreshold.relaxed, 0, 1, 0.46)
  const relaxedThreshold = Math.min(strictThreshold, relaxedThresholdRaw)

  const rolloutPercentRaw = Number.isFinite(Number(placementPolicy.rolloutPercent))
    ? toPositiveInteger(placementPolicy.rolloutPercent, 100)
    : toPositiveInteger(runtimePolicy.rolloutPercent, 100)
  const rolloutPercent = Math.max(1, Math.min(100, Number.isFinite(rolloutPercentRaw) ? rolloutPercentRaw : 100))

  const enabled = parseFeatureSwitch(
    placementPolicy.enabled,
    parseFeatureSwitch(runtimePolicy.enabled, true),
  )
  const configuredMode = parseRelevancePolicyMode(
    placementPolicy.mode || runtimePolicy.mode,
    'enforce',
  )
  let mode = configuredMode
  if (enabled && configuredMode === 'enforce' && rolloutPercent < 100) {
    const bucket = computeDeterministicRolloutBucket(`${requestId}|${placementId}`)
    mode = bucket < rolloutPercent ? 'enforce' : 'shadow'
  }

  return {
    enabled,
    mode,
    thresholdVersion: String(runtimePolicy.thresholdVersion || 'v1_default_2026_03_01').trim() || 'v1_default_2026_03_01',
    sameVerticalFallbackEnabled: parseFeatureSwitch(
      placementPolicy.sameVerticalFallbackEnabled,
      parseFeatureSwitch(runtimePolicy.sameVerticalFallbackEnabled, true),
    ),
    rolloutPercent,
    thresholds: {
      [String(placementId || '')]: {
        strict: strictThreshold,
        relaxed: relaxedThreshold,
      },
    },
    calibration: runtimePolicy.calibration && typeof runtimePolicy.calibration === 'object'
      ? runtimePolicy.calibration
      : {},
  }
}

function summarizeInventoryReadiness(statusInput = {}) {
  const status = statusInput && typeof statusInput === 'object' ? statusInput : {}
  const counts = Array.isArray(status.counts) ? status.counts : []
  const countsByNetwork = {}
  let totalOffers = 0

  for (const row of counts) {
    const network = String(row?.network || '').trim().toLowerCase()
    if (!network) continue
    const offerCount = toPositiveInteger(row?.offer_count, 0)
    countsByNetwork[network] = offerCount
    totalOffers += offerCount
  }

  const missingNetworks = CORE_INVENTORY_NETWORKS.filter((network) => toPositiveInteger(countsByNetwork[network], 0) <= 0)
  const coveredNetworks = CORE_INVENTORY_NETWORKS.filter((network) => toPositiveInteger(countsByNetwork[network], 0) > 0)
  const ready = totalOffers > 0 && missingNetworks.length === 0

  return {
    ready,
    totalOffers,
    coreNetworks: [...CORE_INVENTORY_NETWORKS],
    coveredNetworks,
    missingNetworks,
    countsByNetwork,
    mode: String(status.mode || '').trim(),
    checkedAt: String(status.checkedAt || nowIso()).trim() || nowIso(),
  }
}

async function getCachedInventoryReadinessSummary(options = {}) {
  const forceRefresh = options && options.forceRefresh === true
  const nowMs = Date.now()
  if (
    !forceRefresh
    && runtimeMemory.inventoryReadinessSummary
    && nowMs - toPositiveInteger(runtimeMemory.inventoryReadinessCheckedAtMs, 0) < INVENTORY_READINESS_CACHE_TTL_MS
  ) {
    return runtimeMemory.inventoryReadinessSummary
  }

  const status = await getInventoryStatus(isPostgresSettlementStore() ? settlementStore.pool : null)
  const summary = summarizeInventoryReadiness(status)
  runtimeMemory.inventoryReadinessSummary = summary
  runtimeMemory.inventoryReadinessCheckedAtMs = nowMs
  return summary
}

const SIMULATED_LIVE_FALLBACK_OFFERS = Object.freeze([
  {
    offerId: 'house:broker_low_fee',
    sourceNetwork: 'house',
    sourceId: 'house_broker_001',
    sourceType: 'offer',
    title: 'Low-Fee Brokerage for ETF + Options',
    description: 'Compare broker fee tiers, mobile UX, and options contract pricing.',
    targetUrl: 'https://example.com/offers/broker-low-fee',
    market: 'US',
    locale: 'en-US',
    availability: 'active',
    qualityScore: 0.88,
    bidValue: 7.1,
    metadata: {
      campaignId: 'cmp_house_broker_low_fee',
      policyWeight: 0.2,
      tags: ['broker', 'etf', 'options', 'fees'],
    },
  },
  {
    offerId: 'house:research_scanner',
    sourceNetwork: 'house',
    sourceId: 'house_research_001',
    sourceType: 'offer',
    title: 'Earnings Scanner + Analyst Alerts',
    description: 'Track revisions, analyst upgrades, and post-earnings moves in one tool.',
    targetUrl: 'https://example.com/offers/research-scanner',
    market: 'US',
    locale: 'en-US',
    availability: 'active',
    qualityScore: 0.84,
    bidValue: 6.6,
    metadata: {
      campaignId: 'cmp_house_research_scanner',
      policyWeight: 0.18,
      tags: ['research', 'earnings', 'analyst'],
    },
  },
  {
    offerId: 'house:crypto_exchange',
    sourceNetwork: 'house',
    sourceId: 'house_crypto_001',
    sourceType: 'offer',
    title: 'Crypto Exchange Fee & Depth Comparison',
    description: 'Compare maker-taker fees and orderbook depth for BTC and ETH.',
    targetUrl: 'https://example.com/offers/crypto-exchange',
    market: 'US',
    locale: 'en-US',
    availability: 'active',
    qualityScore: 0.83,
    bidValue: 6.2,
    metadata: {
      campaignId: 'cmp_house_crypto_exchange',
      policyWeight: 0.16,
      tags: ['crypto', 'exchange', 'fees', 'trading'],
    },
  },
  {
    offerId: 'partnerstack:budget_app',
    sourceNetwork: 'partnerstack',
    sourceId: 'ps_budget_001',
    sourceType: 'link',
    title: 'Budget App with Account Aggregation',
    description: 'Sync accounts, track categories, and plan monthly spending.',
    targetUrl: 'https://example.com/offers/budget-app',
    market: 'US',
    locale: 'en-US',
    availability: 'active',
    qualityScore: 0.79,
    bidValue: 2.9,
    metadata: {
      campaignId: 'cmp_partnerstack_budget_app',
      policyWeight: 0.14,
      tags: ['budget', 'finance app', 'credit', 'savings'],
    },
  },
  {
    offerId: 'cj:hardware_wallet',
    sourceNetwork: 'cj',
    sourceId: 'cj_wallet_001',
    sourceType: 'link',
    title: 'Hardware Wallet Security Picks',
    description: 'Secure element design, recovery UX, and firmware trust signals.',
    targetUrl: 'https://example.com/offers/hardware-wallet',
    market: 'US',
    locale: 'en-US',
    availability: 'active',
    qualityScore: 0.77,
    bidValue: 2.4,
    metadata: {
      campaignId: 'cmp_cj_hardware_wallet',
      policyWeight: 0.1,
      tags: ['hardware wallet', 'crypto', 'security'],
    },
  },
])

function buildSimulatedFallbackOffers(networks = []) {
  const normalizedNetworks = Array.isArray(networks)
    ? networks.map((item) => String(item || '').trim().toLowerCase()).filter(Boolean)
    : []
  const allowAll = normalizedNetworks.length === 0
  return SIMULATED_LIVE_FALLBACK_OFFERS.filter((offer) => (
    allowAll || normalizedNetworks.includes(String(offer.sourceNetwork || '').trim().toLowerCase())
  ))
}

const DEFAULT_CAMPAIGN_BUDGET_SEEDS = Object.freeze([
  'cmp_house_broker_low_fee',
  'cmp_house_research_scanner',
  'cmp_house_crypto_exchange',
  'cmp_partnerstack_budget_app',
  'cmp_cj_hardware_wallet',
])

async function seedDefaultCampaignBudgets(pool) {
  const db = pool || settlementStore.pool
  if (!db) return
  for (const campaignId of DEFAULT_CAMPAIGN_BUDGET_SEEDS) {
    await db.query(
      `
        INSERT INTO ${CAMPAIGNS_TABLE} (
          campaign_id,
          account_id,
          app_id,
          status,
          metadata,
          created_at,
          updated_at
        )
        VALUES ($1, '', '', 'active', '{"seed":"default_catalog"}'::jsonb, NOW(), NOW())
        ON CONFLICT (campaign_id) DO NOTHING
      `,
      [campaignId],
    )
    await db.query(
      `
        INSERT INTO ${CAMPAIGN_BUDGET_LIMITS_TABLE} (
          campaign_id,
          daily_budget_usd,
          lifetime_budget_usd,
          currency,
          timezone,
          created_at,
          updated_at
        )
        VALUES ($1, 500, 5000, 'USD', 'UTC', NOW(), NOW())
        ON CONFLICT (campaign_id) DO NOTHING
      `,
      [campaignId],
    )
  }
}

async function fetchLiveFallbackOpportunityCandidates(input = {}) {
  const filters = input?.filters && typeof input.filters === 'object' ? input.filters : {}
  const requestedNetworks = Array.isArray(filters.networks) ? filters.networks : []
  const allOffers = normalizeUnifiedOffers(buildSimulatedFallbackOffers(requestedNetworks))

  return {
    offers: allOffers,
    debug: {
      mode: 'connector_live_fallback',
      networkCount: requestedNetworks.length > 0 ? requestedNetworks.length : 3,
      fetchedOfferCount: allOffers.length,
      source: 'simulated_catalog',
      errors: [],
    },
  }
}

function mapOpportunityReasonToDecision(reasonCode = '', served = false) {
  if (served) return 'served'
  if (reasonCode === 'policy_blocked') return 'blocked'
  if (reasonCode === 'placement_unavailable') return 'blocked'
  if (reasonCode === 'risk_blocked') return 'blocked'
  if (
    reasonCode === 'inventory_no_match'
    || reasonCode === 'rank_below_floor'
    || reasonCode === 'relevance_blocked_strict'
    || reasonCode === 'relevance_blocked_cross_vertical'
    || reasonCode === 'inventory_empty'
    || reasonCode === 'budget_unconfigured'
    || reasonCode === 'budget_exhausted'
    || reasonCode === 'upstream_timeout'
    || reasonCode === 'upstream_error'
  ) {
    return 'no_fill'
  }
  return 'error'
}

function normalizeDbTimestamp(value, fallback = '') {
  if (!value) return fallback
  const parsed = Date.parse(String(value))
  if (!Number.isFinite(parsed)) return fallback
  return new Date(parsed).toISOString()
}

function toDbNullableTimestamptz(value) {
  const normalized = normalizeDbTimestamp(value, '')
  return normalized || null
}

function toDbJsonObject(value, fallback = {}) {
  return value && typeof value === 'object' ? value : fallback
}

function clampNumber(value, min, max, fallback) {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  return Math.min(max, Math.max(min, n))
}

function normalizeStringList(value) {
  if (!Array.isArray(value)) return []
  return Array.from(
    new Set(
      value
        .map((item) => String(item || '').trim().toLowerCase())
        .filter(Boolean),
    ),
  )
}

function mergeNormalizedStringLists(...values) {
  const merged = []
  for (const value of values) {
    if (!Array.isArray(value)) continue
    merged.push(...value)
  }
  return normalizeStringList(merged)
}

function normalizeDisclosure(value) {
  const text = String(value || '').trim()
  if (text === 'Ad' || text === 'Sponsored') return text
  return 'Sponsored'
}

function normalizeControlPlaneEnvironment(value, fallback = 'prod') {
  const normalized = String(value || '').trim().toLowerCase()
  if (CONTROL_PLANE_ENVIRONMENTS.has(normalized)) return normalized
  return fallback
}

function normalizeControlPlaneKeyStatus(value, fallback = 'active') {
  const normalized = String(value || '').trim().toLowerCase()
  if (CONTROL_PLANE_KEY_STATUS.has(normalized)) return normalized
  return fallback
}

function normalizeControlPlaneAccountId(value, fallback = '') {
  const normalized = String(value || '').trim()
  if (normalized) return normalized
  if (fallback === '') return ''
  return String(fallback || '').trim()
}

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase()
}

function hashPasswordWithSalt(password, salt) {
  return createHash('sha256').update(`${String(salt || '')}:${String(password || '')}`).digest('hex')
}

function passwordHashRecord(password) {
  const salt = randomToken(16)
  return {
    passwordSalt: salt,
    passwordHash: hashPasswordWithSalt(password, salt),
  }
}

function verifyPasswordRecord(password, record) {
  const passwordHash = String(record?.passwordHash || '').trim()
  const passwordSalt = String(record?.passwordSalt || '').trim()
  if (!passwordHash || !passwordSalt) return false
  return hashPasswordWithSalt(password, passwordSalt) === passwordHash
}

function randomToken(length = 12) {
  let token = ''
  while (token.length < length) {
    token += Math.random().toString(36).slice(2)
  }
  return token.slice(0, length)
}

function hashToken(value) {
  return createHash('sha256').update(String(value || '')).digest('hex')
}

function tokenFingerprint(value) {
  const digest = hashToken(value)
  return digest ? digest.slice(0, 16) : ''
}

function createMinimalAgentScope() {
  return {
    mediationConfigRead: true,
    sdkEvaluate: true,
    sdkEvents: true,
  }
}

function buildApiKeySecret(environment = 'prod', preferredSecret = '') {
  const env = normalizeControlPlaneEnvironment(environment)
  const preferred = String(preferredSecret || '').trim()
  if (preferred && preferred.startsWith(`sk_${env}_`)) {
    return preferred
  }
  return `sk_${env}_${randomToken(24)}`
}

function maskApiKeySecret(secret) {
  const value = String(secret || '')
  if (value.length < 10) return '****'
  return `${value.slice(0, 6)}...${value.slice(-4)}`
}

function buildControlPlaneAppRecord(raw = {}) {
  const timestamp = typeof raw.createdAt === 'string' ? raw.createdAt : nowIso()
  const appId = String(raw.appId || '').trim()
  if (!appId) return null
  const accountId = normalizeControlPlaneAccountId(
    raw.accountId || raw.account_id || raw.organizationId || raw.organization_id,
    '',
  )
  return {
    appId,
    accountId,
    organizationId: accountId,
    displayName: String(raw.displayName || '').trim() || appId,
    status: String(raw.status || '').trim() || 'active',
    metadata: raw.metadata && typeof raw.metadata === 'object' ? raw.metadata : {},
    createdAt: timestamp,
    updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : timestamp,
  }
}

function buildControlPlaneEnvironmentRecord(raw = {}) {
  const timestamp = typeof raw.createdAt === 'string' ? raw.createdAt : nowIso()
  const appId = String(raw.appId || '').trim()
  if (!appId) return null
  const environment = normalizeControlPlaneEnvironment(raw.environment)
  const accountId = normalizeControlPlaneAccountId(
    raw.accountId || raw.account_id || raw.organizationId || raw.organization_id,
    '',
  )
  return {
    environmentId: String(raw.environmentId || '').trim() || `env_${appId}_${environment}`,
    appId,
    accountId,
    environment,
    routingMode: MANAGED_ROUTING_MODE,
    apiBaseUrl: String(raw.apiBaseUrl || '').trim() || '/api/v1/sdk',
    status: String(raw.status || '').trim() || 'active',
    metadata: raw.metadata && typeof raw.metadata === 'object' ? raw.metadata : {},
    createdAt: timestamp,
    updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : timestamp,
  }
}

function createControlPlaneKeyRecord(input = {}) {
  const appId = String(input.appId || '').trim()
  if (!appId) {
    throw new Error('appId is required.')
  }
  const accountId = normalizeControlPlaneAccountId(input.accountId || input.account_id, '')
  if (!accountId) {
    throw new Error('accountId is required.')
  }
  const environment = normalizeControlPlaneEnvironment(input.environment)
  const keyName = String(input.keyName || '').trim() || `primary-${environment}`
  const keyId = String(input.keyId || '').trim() || `key_${randomToken(18)}`
  const createdAt = typeof input.createdAt === 'string' ? input.createdAt : nowIso()
  const updatedAt = typeof input.updatedAt === 'string' ? input.updatedAt : createdAt
  const secret = buildApiKeySecret(environment, input.secret)
  const keyPrefix = secret.slice(0, 14)
  const secretHash = createHash('sha256').update(secret).digest('hex')
  const status = normalizeControlPlaneKeyStatus(input.status, 'active')
  const revokedAt = status === 'revoked'
    ? (typeof input.revokedAt === 'string' ? input.revokedAt : updatedAt)
    : ''

  return {
    keyRecord: {
      keyId,
      appId,
      accountId,
      environment,
      keyName,
      keyPrefix,
      secretHash,
      status,
      revokedAt,
      lastUsedAt: typeof input.lastUsedAt === 'string' ? input.lastUsedAt : '',
      metadata: input.metadata && typeof input.metadata === 'object' ? input.metadata : {},
      maskedKey: maskApiKeySecret(secret),
      createdAt,
      updatedAt,
    },
    secret,
  }
}

function normalizeControlPlaneKeyRecord(raw) {
  if (!raw || typeof raw !== 'object') return null
  const keyId = String(raw.keyId || raw.key_id || raw.id || '').trim()
  if (!keyId) return null

  const appId = String(raw.appId || raw.app_id || '').trim()
  if (!appId) return null
  const accountId = normalizeControlPlaneAccountId(
    raw.accountId || raw.account_id || raw.organizationId || raw.organization_id,
    '',
  )
  const environment = normalizeControlPlaneEnvironment(raw.environment)
  const keyName = String(raw.keyName || raw.key_name || raw.name || '').trim() || `primary-${environment}`
  const status = normalizeControlPlaneKeyStatus(raw.status, 'active')
  const createdAt = typeof raw.createdAt === 'string'
    ? raw.createdAt
    : (typeof raw.created_at === 'string' ? raw.created_at : nowIso())
  const updatedAt = typeof raw.updatedAt === 'string'
    ? raw.updatedAt
    : (typeof raw.updated_at === 'string' ? raw.updated_at : createdAt)
  const keyPrefix = String(raw.keyPrefix || raw.key_prefix || '').trim()
  const maskedKey = String(raw.maskedKey || raw.keyMasked || raw.preview || '').trim() || (
    keyPrefix ? `${keyPrefix}...****` : '****'
  )
  const revokedAt = status === 'revoked'
    ? String(raw.revokedAt || raw.revoked_at || updatedAt)
    : ''

  return {
    keyId,
    appId,
    accountId,
    environment,
    keyName,
    keyPrefix,
    secretHash: String(raw.secretHash || raw.secret_hash || '').trim(),
    status,
    revokedAt,
    lastUsedAt: String(raw.lastUsedAt || raw.last_used_at || '').trim(),
    metadata: raw.metadata && typeof raw.metadata === 'object' ? raw.metadata : {},
    maskedKey,
    createdAt,
    updatedAt,
  }
}

function toPublicApiKeyRecord(record) {
  const item = normalizeControlPlaneKeyRecord(record)
  if (!item) return null
  return {
    keyId: item.keyId,
    appId: item.appId,
    accountId: item.accountId,
    name: item.keyName,
    environment: item.environment,
    status: item.status,
    maskedKey: item.maskedKey,
    createdAt: item.createdAt,
    lastUsedAt: item.lastUsedAt,
  }
}

function createIntegrationTokenRecord(input = {}) {
  const appId = String(input.appId || '').trim()
  if (!appId) {
    throw new Error('appId is required.')
  }
  const accountId = normalizeControlPlaneAccountId(input.accountId || input.account_id, '')
  if (!accountId) {
    throw new Error('accountId is required.')
  }
  const environment = normalizeControlPlaneEnvironment(input.environment)
  const placementId = normalizePlacementIdWithMigration(
    assertPlacementIdNotRenamed(String(input.placementId || '').trim() || PLACEMENT_ID_FROM_ANSWER, 'placementId'),
    PLACEMENT_ID_FROM_ANSWER,
  )
  const ttlMinutes = toPositiveInteger(input.ttlMinutes, 10)
  const ttlSeconds = ttlMinutes * 60
  const issuedAt = typeof input.issuedAt === 'string' ? input.issuedAt : nowIso()
  const updatedAt = typeof input.updatedAt === 'string' ? input.updatedAt : issuedAt
  const issuedAtMs = Date.parse(issuedAt)
  const expiresAtMs = (Number.isFinite(issuedAtMs) ? issuedAtMs : Date.now()) + ttlSeconds * 1000
  const expiresAt = new Date(expiresAtMs).toISOString()
  const token = `itk_${environment}_${randomToken(30)}`
  const tokenHash = hashToken(token)

  return {
    tokenRecord: {
      tokenId: String(input.tokenId || '').trim() || `itk_${randomToken(16)}`,
      appId,
      accountId,
      environment,
      placementId,
      tokenHash,
      tokenType: 'integration_token',
      oneTime: true,
      status: 'active',
      scope: createMinimalAgentScope(),
      issuedAt,
      expiresAt,
      usedAt: '',
      revokedAt: '',
      metadata: input.metadata && typeof input.metadata === 'object' ? input.metadata : {},
      updatedAt,
    },
    token,
  }
}

function normalizeIntegrationTokenRecord(raw) {
  if (!raw || typeof raw !== 'object') return null
  const tokenId = String(raw.tokenId || raw.token_id || raw.id || '').trim()
  if (!tokenId) return null

  const appId = String(raw.appId || raw.app_id || '').trim()
  if (!appId) return null
  const accountId = normalizeControlPlaneAccountId(
    raw.accountId || raw.account_id || raw.organizationId || raw.organization_id,
    '',
  )
  const environment = normalizeControlPlaneEnvironment(raw.environment)
  const placementId = normalizePlacementIdWithMigration(
    String(raw.placementId || raw.placement_id || '').trim(),
    PLACEMENT_ID_FROM_ANSWER,
  )
  const status = String(raw.status || '').trim().toLowerCase() || 'active'

  return {
    tokenId,
    appId,
    accountId,
    environment,
    placementId,
    tokenHash: String(raw.tokenHash || raw.token_hash || '').trim(),
    tokenType: 'integration_token',
    oneTime: true,
    status: ['active', 'used', 'expired', 'revoked'].includes(status) ? status : 'active',
    scope: raw.scope && typeof raw.scope === 'object'
      ? raw.scope
      : createMinimalAgentScope(),
    issuedAt: String(raw.issuedAt || raw.issued_at || nowIso()),
    expiresAt: String(raw.expiresAt || raw.expires_at || nowIso()),
    usedAt: String(raw.usedAt || raw.used_at || ''),
    revokedAt: String(raw.revokedAt || raw.revoked_at || ''),
    metadata: raw.metadata && typeof raw.metadata === 'object' ? raw.metadata : {},
    updatedAt: String(raw.updatedAt || raw.updated_at || raw.issuedAt || raw.issued_at || nowIso()),
  }
}

function toPublicIntegrationTokenRecord(record, plainToken = '') {
  const item = normalizeIntegrationTokenRecord(record)
  if (!item) return null
  const issuedAtMs = Date.parse(item.issuedAt)
  const expiresAtMs = Date.parse(item.expiresAt)
  const ttlSeconds = (
    Number.isFinite(issuedAtMs) && Number.isFinite(expiresAtMs) && expiresAtMs > issuedAtMs
      ? Math.floor((expiresAtMs - issuedAtMs) / 1000)
      : 0
  )

  return {
    tokenId: item.tokenId,
    tokenType: item.tokenType,
    integrationToken: plainToken || undefined,
    appId: item.appId,
    accountId: item.accountId,
    environment: item.environment,
    placementId: item.placementId,
    oneTime: item.oneTime,
    status: item.status,
    scope: item.scope,
    issuedAt: item.issuedAt,
    expiresAt: item.expiresAt,
    ttlSeconds,
  }
}

function createAgentAccessTokenRecord(input = {}) {
  const appId = String(input.appId || '').trim()
  if (!appId) {
    throw new Error('appId is required.')
  }
  const accountId = normalizeControlPlaneAccountId(input.accountId || input.account_id, '')
  if (!accountId) {
    throw new Error('accountId is required.')
  }
  const environment = normalizeControlPlaneEnvironment(input.environment)
  const placementId = normalizePlacementIdWithMigration(
    assertPlacementIdNotRenamed(String(input.placementId || '').trim() || PLACEMENT_ID_FROM_ANSWER, 'placementId'),
    PLACEMENT_ID_FROM_ANSWER,
  )
  const ttlSeconds = toPositiveInteger(input.ttlSeconds, 300)
  const issuedAt = typeof input.issuedAt === 'string' ? input.issuedAt : nowIso()
  const updatedAt = typeof input.updatedAt === 'string' ? input.updatedAt : issuedAt
  const issuedAtMs = Date.parse(issuedAt)
  const expiresAtMs = (Number.isFinite(issuedAtMs) ? issuedAtMs : Date.now()) + ttlSeconds * 1000
  const expiresAt = new Date(expiresAtMs).toISOString()
  const accessToken = `atk_${environment}_${randomToken(30)}`
  const tokenHash = hashToken(accessToken)

  return {
    tokenRecord: {
      tokenId: String(input.tokenId || '').trim() || `atk_${randomToken(16)}`,
      appId,
      accountId,
      environment,
      placementId,
      sourceTokenId: String(input.sourceTokenId || '').trim(),
      tokenHash,
      tokenType: 'agent_access_token',
      status: 'active',
      scope: input.scope && typeof input.scope === 'object'
        ? input.scope
        : createMinimalAgentScope(),
      issuedAt,
      expiresAt,
      metadata: input.metadata && typeof input.metadata === 'object' ? input.metadata : {},
      updatedAt,
    },
    accessToken,
  }
}

function normalizeAgentAccessTokenRecord(raw) {
  if (!raw || typeof raw !== 'object') return null
  const tokenId = String(raw.tokenId || raw.token_id || raw.id || '').trim()
  if (!tokenId) return null

  const appId = String(raw.appId || raw.app_id || '').trim()
  if (!appId) return null
  const accountId = normalizeControlPlaneAccountId(
    raw.accountId || raw.account_id || raw.organizationId || raw.organization_id,
    '',
  )
  const environment = normalizeControlPlaneEnvironment(raw.environment)
  const placementId = normalizePlacementIdWithMigration(
    String(raw.placementId || raw.placement_id || '').trim(),
    PLACEMENT_ID_FROM_ANSWER,
  )
  const status = String(raw.status || '').trim().toLowerCase() || 'active'

  return {
    tokenId,
    appId,
    accountId,
    environment,
    placementId,
    sourceTokenId: String(raw.sourceTokenId || raw.source_token_id || '').trim(),
    tokenHash: String(raw.tokenHash || raw.token_hash || '').trim(),
    tokenType: 'agent_access_token',
    status: ['active', 'expired', 'revoked'].includes(status) ? status : 'active',
    scope: raw.scope && typeof raw.scope === 'object'
      ? raw.scope
      : createMinimalAgentScope(),
    issuedAt: String(raw.issuedAt || raw.issued_at || nowIso()),
    expiresAt: String(raw.expiresAt || raw.expires_at || nowIso()),
    metadata: raw.metadata && typeof raw.metadata === 'object' ? raw.metadata : {},
    updatedAt: String(raw.updatedAt || raw.updated_at || raw.issuedAt || raw.issued_at || nowIso()),
  }
}

function toPublicAgentAccessTokenRecord(record, plainToken = '') {
  const item = normalizeAgentAccessTokenRecord(record)
  if (!item) return null
  const issuedAtMs = Date.parse(item.issuedAt)
  const expiresAtMs = Date.parse(item.expiresAt)
  const ttlSeconds = (
    Number.isFinite(issuedAtMs) && Number.isFinite(expiresAtMs) && expiresAtMs > issuedAtMs
      ? Math.floor((expiresAtMs - issuedAtMs) / 1000)
      : 0
  )
  return {
    tokenId: item.tokenId,
    tokenType: item.tokenType,
    accessToken: plainToken || undefined,
    sourceTokenId: item.sourceTokenId,
    appId: item.appId,
    accountId: item.accountId,
    environment: item.environment,
    placementId: item.placementId,
    status: item.status,
    scope: item.scope,
    issuedAt: item.issuedAt,
    expiresAt: item.expiresAt,
    ttlSeconds,
  }
}

function createDashboardUserRecord(input = {}) {
  const now = nowIso()
  const email = normalizeEmail(input.email)
  const accountId = normalizeControlPlaneAccountId(input.accountId || input.account_id)
  const appId = String(input.appId || input.app_id || '').trim()
  const displayName = String(input.displayName || input.display_name || '').trim() || email
  const { passwordHash, passwordSalt } = passwordHashRecord(String(input.password || ''))

  return {
    userId: String(input.userId || '').trim() || `usr_${randomToken(18)}`,
    email,
    displayName,
    accountId,
    appId,
    status: 'active',
    passwordHash,
    passwordSalt,
    createdAt: now,
    updatedAt: now,
    lastLoginAt: '',
    metadata: input.metadata && typeof input.metadata === 'object' ? input.metadata : {},
  }
}

function normalizeDashboardUserRecord(raw) {
  if (!raw || typeof raw !== 'object') return null
  const userId = String(raw.userId || raw.user_id || raw.id || '').trim()
  const email = normalizeEmail(raw.email)
  const accountId = normalizeControlPlaneAccountId(raw.accountId || raw.account_id || raw.organizationId || raw.organization_id, '')
  if (!userId || !email || !accountId) return null
  const status = String(raw.status || '').trim().toLowerCase() || 'active'
  return {
    userId,
    email,
    displayName: String(raw.displayName || raw.display_name || '').trim() || email,
    accountId,
    appId: String(raw.appId || raw.app_id || '').trim(),
    status: status === 'disabled' ? 'disabled' : 'active',
    passwordHash: String(raw.passwordHash || raw.password_hash || '').trim(),
    passwordSalt: String(raw.passwordSalt || raw.password_salt || '').trim(),
    createdAt: String(raw.createdAt || raw.created_at || nowIso()),
    updatedAt: String(raw.updatedAt || raw.updated_at || nowIso()),
    lastLoginAt: String(raw.lastLoginAt || raw.last_login_at || ''),
    metadata: raw.metadata && typeof raw.metadata === 'object' ? raw.metadata : {},
  }
}

function toPublicDashboardUserRecord(raw) {
  const user = normalizeDashboardUserRecord(raw)
  if (!user) return null
  return {
    userId: user.userId,
    email: user.email,
    displayName: user.displayName,
    accountId: user.accountId,
    appId: user.appId,
    status: user.status,
    createdAt: user.createdAt,
    updatedAt: user.updatedAt,
    lastLoginAt: user.lastLoginAt,
  }
}

function createDashboardSessionRecord(input = {}) {
  const issuedAt = nowIso()
  const ttlSeconds = Math.max(300, toPositiveInteger(input.ttlSeconds, DASHBOARD_SESSION_TTL_SECONDS))
  const expiresAtMs = Date.parse(issuedAt) + ttlSeconds * 1000
  const accessToken = `${DASHBOARD_SESSION_PREFIX}${randomToken(48)}`
  return {
    sessionRecord: {
      sessionId: String(input.sessionId || '').trim() || `dshs_${randomToken(16)}`,
      tokenHash: hashToken(accessToken),
      tokenType: 'dashboard_access_token',
      userId: String(input.userId || '').trim(),
      email: normalizeEmail(input.email || ''),
      accountId: normalizeControlPlaneAccountId(input.accountId || input.account_id),
      appId: String(input.appId || input.app_id || '').trim(),
      status: 'active',
      issuedAt,
      expiresAt: new Date(expiresAtMs).toISOString(),
      revokedAt: '',
      metadata: input.metadata && typeof input.metadata === 'object' ? input.metadata : {},
      updatedAt: issuedAt,
    },
    accessToken,
  }
}

function normalizeDashboardSessionRecord(raw) {
  if (!raw || typeof raw !== 'object') return null
  const sessionId = String(raw.sessionId || raw.session_id || raw.id || '').trim()
  const tokenHash = String(raw.tokenHash || raw.token_hash || '').trim()
  const userId = String(raw.userId || raw.user_id || '').trim()
  const accountId = normalizeControlPlaneAccountId(raw.accountId || raw.account_id || raw.organizationId || raw.organization_id, '')
  if (!sessionId || !tokenHash || !userId || !accountId) return null
  const status = String(raw.status || '').trim().toLowerCase() || 'active'
  return {
    sessionId,
    tokenHash,
    tokenType: 'dashboard_access_token',
    userId,
    email: normalizeEmail(raw.email || ''),
    accountId,
    appId: String(raw.appId || raw.app_id || '').trim(),
    status: status === 'revoked' || status === 'expired' ? status : 'active',
    issuedAt: String(raw.issuedAt || raw.issued_at || nowIso()),
    expiresAt: String(raw.expiresAt || raw.expires_at || nowIso()),
    revokedAt: String(raw.revokedAt || raw.revoked_at || ''),
    metadata: raw.metadata && typeof raw.metadata === 'object' ? raw.metadata : {},
    updatedAt: String(raw.updatedAt || raw.updated_at || nowIso()),
  }
}

function toPublicDashboardSessionRecord(raw, plainAccessToken = '') {
  const session = normalizeDashboardSessionRecord(raw)
  if (!session) return null
  const issuedAtMs = Date.parse(session.issuedAt)
  const expiresAtMs = Date.parse(session.expiresAt)
  const ttlSeconds = (
    Number.isFinite(issuedAtMs) && Number.isFinite(expiresAtMs) && expiresAtMs > issuedAtMs
      ? Math.floor((expiresAtMs - issuedAtMs) / 1000)
      : 0
  )
  return {
    sessionId: session.sessionId,
    tokenType: session.tokenType,
    accessToken: plainAccessToken || undefined,
    userId: session.userId,
    email: session.email,
    accountId: session.accountId,
    appId: session.appId,
    status: session.status,
    issuedAt: session.issuedAt,
    expiresAt: session.expiresAt,
    ttlSeconds,
  }
}

function cleanupExpiredIntegrationTokens() {
  const nowMs = Date.now()
  const rows = Array.isArray(state?.controlPlane?.integrationTokens) ? state.controlPlane.integrationTokens : []
  for (const row of rows) {
    if (!row || typeof row !== 'object') continue
    if (String(row.status || '').toLowerCase() !== 'active') continue
    const expiresAtMs = Date.parse(String(row.expiresAt || ''))
    if (!Number.isFinite(expiresAtMs)) continue
    if (expiresAtMs > nowMs) continue
    row.status = 'expired'
    row.updatedAt = nowIso()
  }
}

function cleanupExpiredAgentAccessTokens() {
  const nowMs = Date.now()
  const rows = Array.isArray(state?.controlPlane?.agentAccessTokens) ? state.controlPlane.agentAccessTokens : []
  for (const row of rows) {
    if (!row || typeof row !== 'object') continue
    if (String(row.status || '').toLowerCase() !== 'active') continue
    const expiresAtMs = Date.parse(String(row.expiresAt || ''))
    if (!Number.isFinite(expiresAtMs)) continue
    if (expiresAtMs > nowMs) continue
    row.status = 'expired'
    row.updatedAt = nowIso()
  }
}

function cleanupExpiredDashboardSessions() {
  const nowMs = Date.now()
  const rows = Array.isArray(state?.controlPlane?.dashboardSessions) ? state.controlPlane.dashboardSessions : []
  for (const row of rows) {
    if (!row || typeof row !== 'object') continue
    if (String(row.status || '').toLowerCase() !== 'active') continue
    const expiresAtMs = Date.parse(String(row.expiresAt || ''))
    if (!Number.isFinite(expiresAtMs)) continue
    if (expiresAtMs > nowMs) continue
    row.status = 'expired'
    row.updatedAt = nowIso()
  }
}

function findDashboardUserByEmail(email) {
  const normalizedEmail = normalizeEmail(email)
  if (!normalizedEmail) return null
  const rows = Array.isArray(state?.controlPlane?.dashboardUsers) ? state.controlPlane.dashboardUsers : []
  return rows.find((item) => normalizeEmail(item?.email) === normalizedEmail) || null
}

function findDashboardUserById(userId) {
  const normalizedUserId = String(userId || '').trim()
  if (!normalizedUserId) return null
  const rows = Array.isArray(state?.controlPlane?.dashboardUsers) ? state.controlPlane.dashboardUsers : []
  return rows.find((item) => String(item?.userId || '') === normalizedUserId) || null
}

function findDashboardSessionByTokenHashFromState(tokenHash = '') {
  const normalizedTokenHash = String(tokenHash || '').trim()
  if (!normalizedTokenHash) return null
  const rows = Array.isArray(state?.controlPlane?.dashboardSessions) ? state.controlPlane.dashboardSessions : []
  return rows.find((item) => String(item?.tokenHash || '') === normalizedTokenHash) || null
}

async function findDashboardSessionByPlaintext(accessToken) {
  const token = String(accessToken || '').trim()
  if (!token) return null
  const tokenHash = hashToken(token)
  const current = findDashboardSessionByTokenHashFromState(tokenHash)
  if (current || !isSupabaseSettlementStore()) return current

  await refreshControlPlaneStateFromStore({ force: true }).catch(() => {})
  return findDashboardSessionByTokenHashFromState(tokenHash)
}

function findLatestAppForAccount(accountId) {
  const normalizedAccountId = normalizeControlPlaneAccountId(accountId, '')
  if (!normalizedAccountId) return null
  const rows = Array.isArray(state?.controlPlane?.apps) ? state.controlPlane.apps : []
  const matched = rows.filter((item) => normalizeControlPlaneAccountId(item?.accountId || item?.organizationId, '') === normalizedAccountId)
  matched.sort((a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')))
  return matched[0] || null
}

function appBelongsToAccount(appId, accountId) {
  const normalizedAppId = String(appId || '').trim()
  const normalizedAccountId = normalizeControlPlaneAccountId(accountId, '')
  if (!normalizedAppId || !normalizedAccountId) return false
  const app = resolveControlPlaneAppRecord(normalizedAppId)
  if (!app) return false
  return normalizeControlPlaneAccountId(app.accountId || app.organizationId, '') === normalizedAccountId
}

async function appBelongsToAccountReadThrough(appId, accountId) {
  if (appBelongsToAccount(appId, accountId)) return true
  if (!isSupabaseSettlementStore()) return false
  await refreshControlPlaneStateFromStore({ force: true }).catch(() => {})
  return appBelongsToAccount(appId, accountId)
}

function listDashboardUsersByAccount(accountId) {
  const normalizedAccountId = normalizeControlPlaneAccountId(accountId, '')
  if (!normalizedAccountId) return []
  const rows = Array.isArray(state?.controlPlane?.dashboardUsers) ? state.controlPlane.dashboardUsers : []
  return rows.filter((item) => normalizeControlPlaneAccountId(item?.accountId, '') === normalizedAccountId)
}

function hasNonBootstrapAccountResources(accountId) {
  const normalizedAccountId = normalizeControlPlaneAccountId(accountId, '')
  if (!normalizedAccountId) return false

  const controlPlane = state?.controlPlane && typeof state.controlPlane === 'object'
    ? state.controlPlane
    : createInitialControlPlaneState()
  const apps = Array.isArray(controlPlane.apps) ? controlPlane.apps : []
  const appEnvironments = Array.isArray(controlPlane.appEnvironments) ? controlPlane.appEnvironments : []
  const apiKeys = Array.isArray(controlPlane.apiKeys) ? controlPlane.apiKeys : []
  const integrationTokens = Array.isArray(controlPlane.integrationTokens) ? controlPlane.integrationTokens : []
  const agentAccessTokens = Array.isArray(controlPlane.agentAccessTokens) ? controlPlane.agentAccessTokens : []

  const hasAppResource = apps.some((item) => {
    const rowAccountId = normalizeControlPlaneAccountId(item?.accountId || item?.organizationId, '')
    return rowAccountId === normalizedAccountId
  })
  if (hasAppResource) return true

  const hasEnvironmentResource = appEnvironments.some((item) => {
    const rowAccountId = normalizeControlPlaneAccountId(item?.accountId, '')
    return rowAccountId === normalizedAccountId
  })
  if (hasEnvironmentResource) return true

  const hasApiKeyResource = apiKeys.some((item) => {
    const rowAccountId = normalizeControlPlaneAccountId(item?.accountId || resolveAccountIdForApp(item?.appId), '')
    return rowAccountId === normalizedAccountId
  })
  if (hasApiKeyResource) return true

  const hasIntegrationTokenResource = integrationTokens.some((item) => (
    normalizeControlPlaneAccountId(item?.accountId || resolveAccountIdForApp(item?.appId), '') === normalizedAccountId
  ))
  if (hasIntegrationTokenResource) return true

  const hasAgentTokenResource = agentAccessTokens.some((item) => (
    normalizeControlPlaneAccountId(item?.accountId || resolveAccountIdForApp(item?.appId), '') === normalizedAccountId
  ))
  if (hasAgentTokenResource) return true

  const hasRuntimeOrAuditRows = (
    Array.isArray(state?.decisionLogs) ? state.decisionLogs : []
  ).some((item) => normalizeControlPlaneAccountId(item?.accountId || resolveAccountIdForApp(item?.appId), '') === normalizedAccountId)
    || (
      Array.isArray(state?.eventLogs) ? state.eventLogs : []
    ).some((item) => normalizeControlPlaneAccountId(item?.accountId || resolveAccountIdForApp(item?.appId), '') === normalizedAccountId)
    || (
      Array.isArray(state?.controlPlaneAuditLogs) ? state.controlPlaneAuditLogs : []
    ).some((item) => normalizeControlPlaneAccountId(item?.accountId || resolveAccountIdForApp(item?.appId), '') === normalizedAccountId)
    || (
      Array.isArray(state?.placementAuditLogs) ? state.placementAuditLogs : []
    ).some((item) => normalizeControlPlaneAccountId(item?.accountId || resolveAccountIdForApp(item?.appId), '') === normalizedAccountId)
    || (
      Array.isArray(state?.networkFlowLogs) ? state.networkFlowLogs : []
    ).some((item) => normalizeControlPlaneAccountId(item?.accountId || resolveAccountIdForApp(item?.appId), '') === normalizedAccountId)

  return hasRuntimeOrAuditRows
}

async function resolveDashboardRegisterOwnershipProof(req, accountId = '') {
  const normalizedAccountId = normalizeControlPlaneAccountId(accountId, '')
  if (!normalizedAccountId) return { ok: false, mode: 'none' }

  const token = parseBearerToken(req)
  if (!token) return { ok: false, mode: 'none' }

  if (token.startsWith(DASHBOARD_SESSION_PREFIX)) {
    const sessionAuth = await resolveDashboardSession(req)
    if (sessionAuth.kind === 'dashboard_session') {
      const scopedAccountId = normalizeControlPlaneAccountId(
        sessionAuth.user?.accountId || sessionAuth.session?.accountId,
        '',
      )
      if (scopedAccountId === normalizedAccountId) {
        return {
          ok: true,
          mode: 'dashboard_session',
          user: sessionAuth.user,
          session: sessionAuth.session,
        }
      }
    }
    return { ok: false, mode: 'dashboard_session_invalid' }
  }

  const apiKey = await findActiveApiKeyBySecret(token)
  if (!apiKey) return { ok: false, mode: 'none' }
  const apiKeyAccountId = normalizeControlPlaneAccountId(apiKey.accountId || resolveAccountIdForApp(apiKey.appId), '')
  if (apiKeyAccountId !== normalizedAccountId) {
    return { ok: false, mode: 'api_key_account_mismatch' }
  }
  return {
    ok: true,
    mode: 'api_key',
    apiKey,
  }
}

async function validateDashboardRegisterOwnership(req, accountId = '') {
  const normalizedAccountId = normalizeControlPlaneAccountId(accountId, '')
  if (!normalizedAccountId) {
    return {
      ok: false,
      status: 400,
      error: {
        code: 'INVALID_REQUEST',
        message: 'accountId is required.',
      },
    }
  }

  const existingUsers = listDashboardUsersByAccount(normalizedAccountId)
  const hasExistingUsers = existingUsers.length > 0
  const hasProtectedResources = hasExistingUsers || hasNonBootstrapAccountResources(normalizedAccountId)
  if (!hasProtectedResources) {
    return {
      ok: true,
      proofMode: 'none',
    }
  }

  const proof = await resolveDashboardRegisterOwnershipProof(req, normalizedAccountId)
  if (proof.ok) {
    return {
      ok: true,
      proofMode: proof.mode,
    }
  }

  return {
    ok: false,
    status: 403,
    error: {
      code: 'DASHBOARD_ACCOUNT_OWNERSHIP_REQUIRED',
      message: hasExistingUsers
        ? `accountId ${normalizedAccountId} is already claimed. Sign in with an existing account user to add members.`
        : `accountId ${normalizedAccountId} already has provisioned resources. Provide an active account credential (dashboard session or API key).`,
    },
  }
}

async function resolveDashboardSession(req) {
  cleanupExpiredDashboardSessions()
  const token = parseBearerToken(req)
  if (!token) return { kind: 'none' }
  if (!token.startsWith(DASHBOARD_SESSION_PREFIX)) {
    return {
      kind: 'invalid',
      status: 401,
      code: 'DASHBOARD_TOKEN_INVALID',
      message: 'Dashboard access token is invalid.',
    }
  }
  const session = await findDashboardSessionByPlaintext(token)
  if (!session) {
    return {
      kind: 'invalid',
      status: 401,
      code: 'DASHBOARD_TOKEN_INVALID',
      message: 'Dashboard access token is invalid.',
    }
  }

  const status = String(session.status || '').trim().toLowerCase()
  if (status !== 'active') {
    return {
      kind: 'invalid',
      status: 401,
      code: status === 'expired' ? 'DASHBOARD_TOKEN_EXPIRED' : 'DASHBOARD_TOKEN_INACTIVE',
      message: status === 'expired'
        ? 'Dashboard access token has expired.'
        : `Dashboard access token is not active (${status || 'unknown'}).`,
      session,
    }
  }

  const expiresAtMs = Date.parse(String(session.expiresAt || ''))
  if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
    session.status = 'expired'
    session.updatedAt = nowIso()
    persistState(state)
    return {
      kind: 'invalid',
      status: 401,
      code: 'DASHBOARD_TOKEN_EXPIRED',
      message: 'Dashboard access token has expired.',
      session,
    }
  }

  const user = findDashboardUserById(session.userId)
  if (!user || String(user.status || '').toLowerCase() !== 'active') {
    session.status = 'revoked'
    session.updatedAt = nowIso()
    persistState(state)
    return {
      kind: 'invalid',
      status: 401,
      code: 'DASHBOARD_USER_INVALID',
      message: 'Dashboard user is invalid or disabled.',
      session,
    }
  }

  return {
    kind: 'dashboard_session',
    accessToken: token,
    session,
    user,
  }
}

async function authorizeDashboardScope(req, searchParams, options = {}) {
  const option = options && typeof options === 'object' ? options : {}
  const requireAuth = option.requireAuth === true || DASHBOARD_AUTH_REQUIRED
  const requestedScope = parseScopeFiltersFromSearchParams(searchParams)
  const resolved = await resolveDashboardSession(req)

  if (resolved.kind === 'none') {
    if (requireAuth) {
      return {
        ok: false,
        status: 401,
        error: {
          code: 'DASHBOARD_AUTH_REQUIRED',
          message: 'Dashboard authentication is required.',
        },
      }
    }
    return {
      ok: true,
      scope: requestedScope,
      authMode: 'anonymous',
      session: null,
      user: null,
    }
  }

  if (resolved.kind === 'invalid') {
    return {
      ok: false,
      status: resolved.status,
      error: {
        code: resolved.code,
        message: resolved.message,
      },
    }
  }

  const session = resolved.session
  const user = resolved.user
  const enforcedAccountId = normalizeControlPlaneAccountId(user.accountId || session.accountId, '')
  let enforcedAppId = String(requestedScope.appId || '').trim()
  if (enforcedAppId && !(await appBelongsToAccountReadThrough(enforcedAppId, enforcedAccountId))) {
    return {
      ok: false,
      status: 403,
      error: {
        code: 'DASHBOARD_SCOPE_VIOLATION',
        message: `appId ${enforcedAppId} does not belong to your account.`,
      },
    }
  }

  session.updatedAt = nowIso()
  session.metadata = session.metadata && typeof session.metadata === 'object' ? session.metadata : {}
  session.metadata.lastUsedAt = session.updatedAt

  user.lastLoginAt = user.lastLoginAt || session.updatedAt
  user.updatedAt = session.updatedAt

  persistState(state)
  return {
    ok: true,
    scope: {
      accountId: enforcedAccountId,
      appId: enforcedAppId,
    },
    authMode: 'dashboard_session',
    session,
    user,
  }
}

function resolveAuthorizedDashboardAccount(auth) {
  const accountId = normalizeControlPlaneAccountId(
    auth?.scope?.accountId || auth?.user?.accountId || auth?.session?.accountId || '',
    '',
  )
  return accountId
}

function validateDashboardAccountOwnership(requestedAccountId, authorizedAccountId) {
  const requested = normalizeControlPlaneAccountId(requestedAccountId, '')
  const authorized = normalizeControlPlaneAccountId(authorizedAccountId, '')
  if (!requested || !authorized) return { ok: true }
  if (requested === authorized) return { ok: true }
  return {
    ok: false,
    status: 403,
    error: {
      code: 'DASHBOARD_SCOPE_VIOLATION',
      message: `accountId ${requested} does not belong to your dashboard scope.`,
    },
  }
}

async function validateDashboardAppOwnership(appId, authorizedAccountId) {
  const normalizedAppId = String(appId || '').trim()
  if (!normalizedAppId) return { ok: true }
  let app = resolveControlPlaneAppRecord(normalizedAppId)
  if (!app && isSupabaseSettlementStore()) {
    await refreshControlPlaneStateFromStore({ force: true }).catch(() => {})
    app = resolveControlPlaneAppRecord(normalizedAppId)
  }
  if (!app) return { ok: true }
  const appAccountId = normalizeControlPlaneAccountId(app.accountId || app.organizationId, '')
  const scopedAccountId = normalizeControlPlaneAccountId(authorizedAccountId, '')
  if (!appAccountId || !scopedAccountId || appAccountId === scopedAccountId) return { ok: true }
  return {
    ok: false,
    status: 403,
    error: {
      code: 'DASHBOARD_SCOPE_VIOLATION',
      message: `appId ${normalizedAppId} does not belong to your account.`,
    },
  }
}

function findIntegrationTokenByHashFromState(tokenHash = '') {
  const normalizedTokenHash = String(tokenHash || '').trim()
  if (!normalizedTokenHash) return null
  const rows = Array.isArray(state?.controlPlane?.integrationTokens) ? state.controlPlane.integrationTokens : []
  return rows.find((item) => String(item?.tokenHash || '') === normalizedTokenHash) || null
}

async function findIntegrationTokenByPlaintext(integrationToken) {
  const token = String(integrationToken || '').trim()
  if (!token) return null
  const tokenHash = hashToken(token)
  const current = findIntegrationTokenByHashFromState(tokenHash)
  if (current || !isSupabaseSettlementStore()) return current

  await refreshControlPlaneStateFromStore({ force: true }).catch(() => {})
  return findIntegrationTokenByHashFromState(tokenHash)
}

function findAgentAccessTokenByHashFromState(tokenHash = '') {
  const normalizedTokenHash = String(tokenHash || '').trim()
  if (!normalizedTokenHash) return null
  const rows = Array.isArray(state?.controlPlane?.agentAccessTokens) ? state.controlPlane.agentAccessTokens : []
  return rows.find((item) => String(item?.tokenHash || '') === normalizedTokenHash) || null
}

async function findAgentAccessTokenByPlaintext(accessToken) {
  const token = String(accessToken || '').trim()
  if (!token) return null
  const tokenHash = hashToken(token)
  const current = findAgentAccessTokenByHashFromState(tokenHash)
  if (current || !isSupabaseSettlementStore()) return current

  await refreshControlPlaneStateFromStore({ force: true }).catch(() => {})
  return findAgentAccessTokenByHashFromState(tokenHash)
}

function parseBearerToken(req) {
  if (!req || !req.headers) return ''
  const authorization = String(req.headers.authorization || '').trim()
  if (!authorization) return ''
  const matched = authorization.match(/^bearer\s+(.+)$/i)
  if (matched) return String(matched[1] || '').trim()
  if (authorization.includes(' ')) return ''
  return authorization
}

function resolveControlPlaneAppRecord(appId = '') {
  const normalizedAppId = String(appId || '').trim()
  if (!normalizedAppId) return null
  const apps = Array.isArray(state?.controlPlane?.apps) ? state.controlPlane.apps : []
  return apps.find((item) => String(item?.appId || '').trim() === normalizedAppId) || null
}

function resolveAccountIdForApp(appId = '') {
  const app = resolveControlPlaneAppRecord(appId)
  if (!app) return ''
  return normalizeControlPlaneAccountId(app.accountId || app.organizationId)
}

function normalizeScopeFilters(input = {}) {
  const source = input && typeof input === 'object' ? input : {}
  return {
    appId: String(source.appId || source.app_id || '').trim(),
    accountId: normalizeControlPlaneAccountId(
      source.accountId || source.account_id || source.organizationId || source.organization_id,
      '',
    ),
  }
}

function scopeHasFilters(scope = {}) {
  return Boolean(String(scope?.appId || '').trim() || String(scope?.accountId || '').trim())
}

function appMatchesScope(app, scope = {}) {
  if (!app || typeof app !== 'object') return false
  const appId = String(app.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(app.accountId || app.organizationId, '')
  if (scope.appId && scope.appId !== appId) return false
  if (scope.accountId && scope.accountId !== accountId) return false
  return Boolean(appId)
}

function getScopedApps(scope = {}) {
  const apps = Array.isArray(state?.controlPlane?.apps) ? state.controlPlane.apps : []
  return apps.filter((item) => appMatchesScope(item, scope))
}

function recordMatchesScope(record, scope = {}) {
  if (!record || typeof record !== 'object') return false
  const appId = String(record.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(record.accountId || resolveAccountIdForApp(appId), '')
  if (scope.appId && scope.appId !== appId) return false
  if (scope.accountId && scope.accountId !== accountId) return false
  return true
}

function parseScopeFiltersFromSearchParams(searchParams) {
  return normalizeScopeFilters({
    appId: searchParams.get('appId') || searchParams.get('app_id') || '',
    accountId: searchParams.get('accountId') || searchParams.get('account_id') || '',
  })
}

function appendQueryParams(rawUrl, params = {}) {
  const text = String(rawUrl || '').trim()
  if (!text) return ''
  let url
  try {
    url = new URL(text)
  } catch {
    return text
  }

  for (const [key, value] of Object.entries(params || {})) {
    const normalizedKey = String(key || '').trim()
    const normalizedValue = String(value || '').trim()
    if (!normalizedKey || !normalizedValue) continue
    url.searchParams.set(normalizedKey, normalizedValue)
  }
  return url.toString()
}

function injectTrackingScopeIntoAd(ad, scope = {}) {
  if (!ad || typeof ad !== 'object') return ad
  const params = {
    [TRACKING_ACCOUNT_QUERY_PARAM]: String(scope.accountId || '').trim(),
  }

  const tracking = ad.tracking && typeof ad.tracking === 'object' ? { ...ad.tracking } : {}
  const clickUrl = String(tracking.clickUrl || tracking.click_url || ad.targetUrl || '').trim()
  if (clickUrl) {
    const scopedClickUrl = appendQueryParams(clickUrl, params)
    tracking.clickUrl = scopedClickUrl
    tracking.click_url = scopedClickUrl
  }

  return {
    ...ad,
    tracking,
  }
}

function injectTrackingScopeIntoBid(bid, scope = {}) {
  if (!bid || typeof bid !== 'object') return bid

  const params = {
    [TRACKING_ACCOUNT_QUERY_PARAM]: String(scope.accountId || '').trim(),
  }
  const scopedBid = {
    ...bid,
  }

  const rawUrl = String(bid.url || '').trim()
  if (rawUrl) {
    scopedBid.url = appendQueryParams(rawUrl, params)
  }

  const rawTargetUrl = String(bid.targetUrl || '').trim()
  if (rawTargetUrl) {
    scopedBid.targetUrl = appendQueryParams(rawTargetUrl, params)
  }

  const rawTrackingUrl = String(bid.trackingUrl || '').trim()
  if (rawTrackingUrl) {
    scopedBid.trackingUrl = appendQueryParams(rawTrackingUrl, params)
  }

  return scopedBid
}

function injectTrackingScopeIntoAds(ads, scope = {}) {
  if (!Array.isArray(ads)) return []
  return ads.map((item) => injectTrackingScopeIntoAd(item, scope))
}

function createInitialControlPlaneState() {
  return {
    apps: [],
    appEnvironments: [],
    apiKeys: [],
    integrationTokens: [],
    agentAccessTokens: [],
    dashboardUsers: [],
    dashboardSessions: [],
  }
}

function ensureControlPlaneState(raw) {
  const fallback = createInitialControlPlaneState()
  if (!raw || typeof raw !== 'object') return fallback

  const appRows = Array.isArray(raw.apps) ? raw.apps : []
  const apps = appRows
    .map((item) => buildControlPlaneAppRecord(item))
    .filter(Boolean)

  const appIdSet = new Set(apps.map((item) => item.appId))

  const environmentRows = Array.isArray(raw.appEnvironments || raw.environments)
    ? (raw.appEnvironments || raw.environments)
    : []
  const accountByAppId = new Map(apps.map((item) => [item.appId, normalizeControlPlaneAccountId(item.accountId)]))
  const appEnvironments = []
  const envDedup = new Set()

  for (const row of environmentRows) {
    const normalized = buildControlPlaneEnvironmentRecord(row)
    if (!normalized) continue
    if (!appIdSet.has(normalized.appId)) continue
    normalized.accountId = normalizeControlPlaneAccountId(
      normalized.accountId || accountByAppId.get(normalized.appId),
    )
    const dedupKey = `${normalized.appId}::${normalized.environment}`
    if (envDedup.has(dedupKey)) continue
    envDedup.add(dedupKey)
    appEnvironments.push(normalized)
  }

  for (const app of apps) {
    for (const environment of CONTROL_PLANE_ENVIRONMENTS) {
      const dedupKey = `${app.appId}::${environment}`
      if (envDedup.has(dedupKey)) continue
      envDedup.add(dedupKey)
      const environmentRecord = buildControlPlaneEnvironmentRecord({
        appId: app.appId,
        accountId: accountByAppId.get(app.appId),
        environment,
      })
      if (environmentRecord) {
        appEnvironments.push(environmentRecord)
      }
    }
  }

  const keyRows = Array.isArray(raw.apiKeys || raw.keys) ? (raw.apiKeys || raw.keys) : []
  let apiKeys = keyRows
    .map((item) => normalizeControlPlaneKeyRecord(item))
    .filter((item) => item && appIdSet.has(item.appId))
    .map((item) => ({
      ...item,
      accountId: normalizeControlPlaneAccountId(item.accountId || accountByAppId.get(item.appId)),
    }))
  const tokenRows = Array.isArray(raw.integrationTokens || raw.tokens)
    ? (raw.integrationTokens || raw.tokens)
    : []
  const integrationTokens = tokenRows
    .map((item) => normalizeIntegrationTokenRecord(item))
    .filter((item) => item && appIdSet.has(item.appId))
    .map((item) => ({
      ...item,
      accountId: normalizeControlPlaneAccountId(item.accountId || accountByAppId.get(item.appId)),
    }))
    .slice(0, MAX_INTEGRATION_TOKENS)

  const agentTokenRows = Array.isArray(raw.agentAccessTokens || raw.accessTokens)
    ? (raw.agentAccessTokens || raw.accessTokens)
    : []
  const agentAccessTokens = agentTokenRows
    .map((item) => normalizeAgentAccessTokenRecord(item))
    .filter((item) => item && appIdSet.has(item.appId))
    .map((item) => ({
      ...item,
      accountId: normalizeControlPlaneAccountId(item.accountId || accountByAppId.get(item.appId)),
    }))
    .slice(0, MAX_AGENT_ACCESS_TOKENS)

  const dashboardUserRows = Array.isArray(raw.dashboardUsers || raw.users)
    ? (raw.dashboardUsers || raw.users)
    : []
  const dashboardUsers = dashboardUserRows
    .map((item) => normalizeDashboardUserRecord(item))
    .filter((item) => item && Boolean(item.accountId))
    .slice(0, MAX_DASHBOARD_USERS)

  const knownUserIds = new Set(dashboardUsers.map((item) => item.userId))
  const dashboardSessionRows = Array.isArray(raw.dashboardSessions || raw.sessions)
    ? (raw.dashboardSessions || raw.sessions)
    : []
  const dashboardSessions = dashboardSessionRows
    .map((item) => normalizeDashboardSessionRecord(item))
    .filter((item) => item && knownUserIds.has(item.userId))
    .slice(0, MAX_DASHBOARD_SESSIONS)

  return {
    apps,
    appEnvironments,
    apiKeys,
    integrationTokens,
    agentAccessTokens,
    dashboardUsers,
    dashboardSessions,
  }
}

async function ensureControlPlaneAppAndEnvironment(appId, environment, accountId = '') {
  const normalizedAppId = String(appId || '').trim()
  if (!normalizedAppId) {
    throw new Error('appId is required.')
  }
  const normalizedEnvironment = normalizeControlPlaneEnvironment(environment)
  const requestedAccountId = normalizeControlPlaneAccountId(accountId, '')
  if (!requestedAccountId) {
    throw new Error('accountId is required.')
  }
  const controlPlane = state.controlPlane
  const now = nowIso()

  const existingApp = controlPlane.apps.find((item) => item.appId === normalizedAppId)
  let appRecord = null
  if (!existingApp) {
    appRecord = buildControlPlaneAppRecord({
      appId: normalizedAppId,
      accountId: requestedAccountId,
      displayName: normalizedAppId,
      organizationId: requestedAccountId,
      createdAt: now,
      updatedAt: now,
    })
    if (!appRecord) {
      throw new Error('failed to create control plane app.')
    }
  } else {
    const existingAccountId = normalizeControlPlaneAccountId(existingApp.accountId || existingApp.organizationId)
    if (requestedAccountId && requestedAccountId !== existingAccountId) {
      throw new Error(`appId ${normalizedAppId} is already bound to accountId ${existingAccountId}.`)
    }
    appRecord = buildControlPlaneAppRecord({
      ...existingApp,
      accountId: existingAccountId,
      organizationId: existingAccountId,
      updatedAt: now,
    })
  }
  const effectiveAccountId = normalizeControlPlaneAccountId(appRecord?.accountId || appRecord?.organizationId)
  if (!effectiveAccountId) {
    throw new Error('accountId is required.')
  }

  const dedupKey = `${normalizedAppId}::${normalizedEnvironment}`
  const existingEnvironment = controlPlane.appEnvironments.find((item) => (
    `${item.appId}::${item.environment}` === dedupKey
  ))
  let environmentRecord = null
  if (!existingEnvironment) {
    environmentRecord = buildControlPlaneEnvironmentRecord({
      appId: normalizedAppId,
      accountId: effectiveAccountId,
      environment: normalizedEnvironment,
      createdAt: now,
      updatedAt: now,
    })
  } else {
    environmentRecord = buildControlPlaneEnvironmentRecord({
      ...existingEnvironment,
      accountId: effectiveAccountId,
      appId: normalizedAppId,
      environment: normalizedEnvironment,
      updatedAt: now,
    })
  }

  if (isSupabaseSettlementStore()) {
    await upsertControlPlaneAppToSupabase(appRecord)
    if (environmentRecord) {
      await upsertControlPlaneEnvironmentToSupabase(environmentRecord)
    }
  }

  upsertControlPlaneStateRecord('apps', 'appId', appRecord)
  if (environmentRecord) {
    upsertControlPlaneEnvironmentStateRecord(environmentRecord)
  }

  getPlacementConfigForApp(normalizedAppId, effectiveAccountId, { createIfMissing: true })
  if (normalizedAppId === DEFAULT_CONTROL_PLANE_APP_ID) {
    syncLegacyPlacementSnapshot()
  }

  return {
    appId: normalizedAppId,
    accountId: effectiveAccountId,
    environment: normalizedEnvironment,
  }
}

function createDecision(result, reasonDetail, intentScore) {
  const normalizedResult = DECISION_REASON_ENUM.has(result) ? result : 'error'
  const detail = String(reasonDetail || '').trim() || normalizedResult
  return {
    result: normalizedResult,
    reason: normalizedResult,
    reasonDetail: detail,
    intentScore,
  }
}

function createInitialNetworkFlowStats() {
  return {
    totalRuntimeEvaluations: 0,
    degradedRuntimeEvaluations: 0,
    resilientServes: 0,
    servedWithNetworkErrors: 0,
    noFillWithNetworkErrors: 0,
    runtimeErrors: 0,
    circuitOpenEvaluations: 0,
  }
}

function normalizeNetworkFlowStats(raw) {
  const fallback = createInitialNetworkFlowStats()
  const value = raw && typeof raw === 'object' ? raw : {}
  return {
    totalRuntimeEvaluations: toPositiveInteger(value.totalRuntimeEvaluations, fallback.totalRuntimeEvaluations),
    degradedRuntimeEvaluations: toPositiveInteger(value.degradedRuntimeEvaluations, fallback.degradedRuntimeEvaluations),
    resilientServes: toPositiveInteger(value.resilientServes, fallback.resilientServes),
    servedWithNetworkErrors: toPositiveInteger(value.servedWithNetworkErrors, fallback.servedWithNetworkErrors),
    noFillWithNetworkErrors: toPositiveInteger(value.noFillWithNetworkErrors, fallback.noFillWithNetworkErrors),
    runtimeErrors: toPositiveInteger(value.runtimeErrors, fallback.runtimeErrors),
    circuitOpenEvaluations: toPositiveInteger(value.circuitOpenEvaluations, fallback.circuitOpenEvaluations),
  }
}

function summarizeNetworkHealthMap(networkHealth = {}) {
  const items = Object.values(networkHealth || {})
  let healthy = 0
  let degraded = 0
  let open = 0

  for (const item of items) {
    const status = String(item?.status || '').toLowerCase()
    if (status === 'healthy') healthy += 1
    else if (status === 'degraded') degraded += 1
    else if (status === 'open') open += 1
  }

  return {
    totalNetworks: items.length,
    healthy,
    degraded,
    open,
  }
}

function validateNoExtraFields(payload, allowedFields, routeName) {
  const keys = Object.keys(payload)
  const extras = keys.filter((key) => !allowedFields.has(key))
  if (extras.length > 0) {
    throw new Error(`${routeName} contains unsupported fields: ${extras.join(', ')}`)
  }
}

function requiredNonEmptyString(value, fieldName) {
  const text = String(value || '').trim()
  if (!text) {
    throw new Error(`${fieldName} is required.`)
  }
  return text
}

function normalizePlacementIdWithMigration(value, fallback = '') {
  const placementId = String(value || '').trim() || String(fallback || '').trim()
  if (!placementId) return ''
  return String(LEGACY_PLACEMENT_ID_MAP[placementId] || placementId).trim()
}

function createPlacementIdRenamedError(placementId, fieldName = 'placementId') {
  const normalizedPlacementId = String(placementId || '').trim()
  const replacementPlacementId = String(LEGACY_PLACEMENT_ID_MAP[normalizedPlacementId] || '').trim()
  const error = new Error(
    `${fieldName} "${normalizedPlacementId}" has been renamed to "${replacementPlacementId}". Use "${replacementPlacementId}" instead.`,
  )
  error.code = 'PLACEMENT_ID_RENAMED'
  error.statusCode = 400
  error.fieldName = fieldName
  error.placementId = normalizedPlacementId
  error.replacementPlacementId = replacementPlacementId
  return error
}

function assertPlacementIdNotRenamed(value, fieldName = 'placementId') {
  const placementId = String(value || '').trim()
  if (!placementId) return placementId
  if (Object.prototype.hasOwnProperty.call(LEGACY_PLACEMENT_ID_MAP, placementId)) {
    throw createPlacementIdRenamedError(placementId, fieldName)
  }
  return placementId
}

function normalizeV2BidMessages(value) {
  if (!Array.isArray(value)) {
    throw new Error('messages must be an array.')
  }

  const diagnostics = {
    roleCoercions: [],
    droppedTimestamps: [],
  }
  const messages = value
    .map((item, index) => {
      if (!item || typeof item !== 'object') {
        throw new Error(`messages[${index}] must be an object.`)
      }
      const roleInput = String(item.role || '').trim().toLowerCase()
      let role = roleInput
      if (!V2_BID_MESSAGE_ROLES.has(role)) {
        if (
          roleInput.includes('assistant')
          || roleInput.includes('bot')
          || roleInput.includes('agent')
          || roleInput.includes('model')
          || roleInput.includes('ai')
          || roleInput.includes('system')
          || roleInput === 'sys'
        ) {
          role = 'assistant'
        } else {
          role = 'user'
        }
        diagnostics.roleCoercions.push({
          index,
          from: roleInput || '(empty)',
          to: role,
        })
      }
      const content = requiredNonEmptyString(item.content, `messages[${index}].content`)
      const timestamp = String(item.timestamp || '').trim()
      let normalizedTimestamp = ''
      if (timestamp) {
        const parsed = Date.parse(timestamp)
        if (Number.isFinite(parsed)) {
          normalizedTimestamp = new Date(parsed).toISOString()
        } else {
          diagnostics.droppedTimestamps.push({
            index,
            value: timestamp,
          })
        }
      }
      return {
        role,
        content,
        ...(normalizedTimestamp ? { timestamp: normalizedTimestamp } : {}),
      }
    })
    .filter(Boolean)

  if (messages.length === 0) {
    throw new Error('messages must contain at least one valid message.')
  }

  return {
    messages,
    diagnostics,
  }
}

function normalizeV2BidPayload(payload, routeName) {
  const input = payload && typeof payload === 'object' ? payload : {}
  const rawMessages = Array.isArray(input.messages) ? input.messages : null
  let synthesizedMessageSources = []
  let messagesInput = rawMessages
  if (!messagesInput || messagesInput.length === 0) {
    const query = String(input.query || input.prompt || '').trim()
    const answerText = String(input.answerText || input.answer || '').trim()
    messagesInput = []
    if (query) {
      messagesInput.push({ role: 'user', content: query })
      synthesizedMessageSources.push('query_or_prompt')
    }
    if (answerText) {
      messagesInput.push({ role: 'assistant', content: answerText })
      synthesizedMessageSources.push('answer')
    }
  }
  const { messages, diagnostics: messageDiagnostics } = normalizeV2BidMessages(messagesInput)

  const rawUserId = String(input.userId || input.user_id || '').trim()
  const rawChatId = String(input.chatId || input.chat_id || input.sessionId || input.session_id || '').trim()
  let userId = rawUserId
  let chatId = rawChatId
  if (!userId) {
    if (chatId) {
      const suffix = createHash('sha256').update(chatId).digest('hex').slice(0, 12)
      userId = `anon_${suffix}`
    } else {
      const stableSeed = JSON.stringify({
        messages: messages.map((item) => `${item.role}:${item.content}`).slice(0, 6),
        query: String(input.query || input.prompt || '').trim(),
        answer: String(input.answerText || input.answer || '').trim(),
      })
      const suffix = createHash('sha256').update(stableSeed).digest('hex').slice(0, 12)
      userId = `anon_${suffix}`
    }
  }
  if (!chatId) {
    chatId = userId
  }

  const rawPlacementId = String(input.placementId || input.placement_id || '').trim()
  if (rawPlacementId) {
    const error = new Error('placementId is no longer accepted in /api/v2/bid. Configure placement in Dashboard.')
    error.code = 'V2_BID_PLACEMENT_ID_NOT_ALLOWED'
    error.statusCode = 400
    throw error
  }
  const placementId = ''

  return {
    userId,
    chatId,
    placementId,
    messages,
    inputDiagnostics: {
      routeName: String(routeName || '').trim(),
      defaultsApplied: {
        userIdGenerated: !rawUserId,
        chatIdDefaultedToUserId: !rawChatId,
        placementIdDefaulted: !rawPlacementId,
        placementIdResolvedFromDashboardDefault: false,
        placementIdFallbackApplied: false,
      },
      placementMigration: null,
      messagesSynthesized: !rawMessages || rawMessages.length === 0,
      messageSources: synthesizedMessageSources,
      roleCoercions: messageDiagnostics.roleCoercions,
      droppedTimestamps: messageDiagnostics.droppedTimestamps,
    },
  }
}

function normalizeAttachEventKind(value) {
  const kind = String(value || '').trim().toLowerCase()
  if (!kind) return 'impression'
  if (kind === 'impression' || kind === 'click') return kind
  throw new Error('kind must be impression or click.')
}

function normalizeNextStepEventKind(value) {
  const kind = String(value || '').trim().toLowerCase()
  if (!kind) return 'impression'
  if (kind === 'impression' || kind === 'click' || kind === 'dismiss') return kind
  throw new Error('kind must be impression, click, or dismiss.')
}

function normalizePostbackType(value) {
  const type = String(value || '').trim().toLowerCase()
  if (!type) return 'conversion'
  if (POSTBACK_TYPES.has(type)) return type
  throw new Error('postbackType must be conversion.')
}

function normalizePostbackStatus(value) {
  const status = String(value || '').trim().toLowerCase()
  if (!status) return 'success'
  if (POSTBACK_STATUS.has(status)) return status
  throw new Error('postbackStatus must be pending, success, or failed.')
}

function normalizeIsoTimestamp(value, fallback = nowIso(), fieldName = '') {
  const text = String(value || '').trim()
  if (!text) return fallback
  const parsed = Date.parse(text)
  if (!Number.isFinite(parsed)) {
    if (fieldName) {
      throw new Error(`${fieldName} must be a valid ISO-8601 datetime.`)
    }
    return fallback
  }
  return new Date(parsed).toISOString()
}

function isPostbackConversionPayload(payload) {
  if (!payload || typeof payload !== 'object') return false
  const eventType = String(payload.eventType || payload.event || '').trim().toLowerCase()
  if (POSTBACK_EVENT_TYPES.has(eventType)) return true
  if (payload.postbackType !== undefined) return true
  if (payload.postbackStatus !== undefined) return true
  if (payload.conversionId !== undefined || payload.conversion_id !== undefined) return true
  return false
}

function normalizePostbackConversionPayload(payload, routeName) {
  const input = payload && typeof payload === 'object' ? payload : {}
  validateNoExtraFields(input, POSTBACK_CONVERSION_ALLOWED_FIELDS, routeName)

  const requestId = requiredNonEmptyString(input.requestId, 'requestId')
  const appId = String(input.appId || input.app_id || '').trim()
  const accountId = normalizeControlPlaneAccountId(
    input.accountId || input.account_id || (appId ? resolveAccountIdForApp(appId) : ''),
    '',
  )
  const eventType = String(input.eventType || input.event || 'postback').trim().toLowerCase()
  if (!POSTBACK_EVENT_TYPES.has(eventType)) {
    throw new Error('eventType must be postback.')
  }

  const postbackType = normalizePostbackType(input.postbackType || input.kind || 'conversion')
  const postbackStatus = normalizePostbackStatus(input.postbackStatus || 'success')
  const cpaUsd = clampNumber(input.cpaUsd ?? input.cpa_usd ?? input.payoutUsd ?? input.payout_usd, 0, Number.MAX_SAFE_INTEGER, NaN)
  if (postbackStatus === 'success' && !Number.isFinite(cpaUsd)) {
    throw new Error('cpaUsd is required for successful postback conversion.')
  }

  const currency = String(input.currency || 'USD').trim().toUpperCase()
  if (currency !== 'USD') {
    throw new Error('currency must be USD for CPA MVP.')
  }

  const rawPlacementId = String(input.placementId || '').trim()
  if (rawPlacementId) {
    assertPlacementIdNotRenamed(rawPlacementId, 'placementId')
  }

  return {
    eventType: 'postback',
    requestId,
    appId,
    accountId,
    sessionId: String(input.sessionId || '').trim(),
    turnId: String(input.turnId || '').trim(),
    userId: String(input.userId || '').trim(),
    placementId: normalizePlacementIdWithMigration(rawPlacementId),
    placementKey: String(input.placementKey || '').trim(),
    adId: String(input.adId || '').trim(),
    postbackType,
    postbackStatus,
    conversionId: String(input.conversionId || input.conversion_id || '').trim(),
    eventSeq: String(input.eventSeq || '').trim(),
    occurredAt: normalizeIsoTimestamp(input.eventAt || input.event_at, nowIso(), 'eventAt'),
    cpaUsd: Number.isFinite(cpaUsd) ? round(cpaUsd, 4) : 0,
    currency,
  }
}

function normalizeDashboardRegisterPayload(payload, routeName) {
  const input = payload && typeof payload === 'object' ? payload : {}
  validateNoExtraFields(input, DASHBOARD_REGISTER_ALLOWED_FIELDS, routeName)
  const email = normalizeEmail(requiredNonEmptyString(input.email, 'email'))
  const password = requiredNonEmptyString(input.password, 'password')
  if (password.length < 8) {
    throw new Error('password must contain at least 8 characters.')
  }
  const accountId = normalizeControlPlaneAccountId(
    requiredNonEmptyString(input.accountId || input.account_id, 'accountId'),
    '',
  )
  const appId = String(input.appId || input.app_id || '').trim()
  const displayName = String(input.displayName || input.display_name || '').trim()
  return {
    email,
    password,
    accountId,
    appId,
    displayName,
  }
}

function normalizeDashboardLoginPayload(payload, routeName) {
  const input = payload && typeof payload === 'object' ? payload : {}
  validateNoExtraFields(input, DASHBOARD_LOGIN_ALLOWED_FIELDS, routeName)
  const email = normalizeEmail(requiredNonEmptyString(input.email, 'email'))
  const password = requiredNonEmptyString(input.password, 'password')
  return {
    email,
    password,
  }
}

function normalizeAttachMvpPayload(payload, routeName) {
  const input = payload && typeof payload === 'object' ? payload : {}
  validateNoExtraFields(input, ATTACH_MVP_ALLOWED_FIELDS, routeName)

  const requestId = String(input.requestId || '').trim()
  const appId = String(input.appId || input.app_id || '').trim()
  const accountId = normalizeControlPlaneAccountId(
    input.accountId || input.account_id || (appId ? resolveAccountIdForApp(appId) : ''),
    '',
  )
  const sessionId = requiredNonEmptyString(input.sessionId, 'sessionId')
  const turnId = requiredNonEmptyString(input.turnId, 'turnId')
  const query = requiredNonEmptyString(input.query, 'query')
  const answerText = requiredNonEmptyString(input.answerText, 'answerText')
  const locale = requiredNonEmptyString(input.locale, 'locale')
  const intentScore = clampNumber(input.intentScore, 0, 1, NaN)
  const kind = normalizeAttachEventKind(input.kind)
  const adId = String(input.adId || '').trim()
  const placementId = assertPlacementIdNotRenamed(
    String(input.placementId || '').trim() || PLACEMENT_ID_FROM_ANSWER,
    'placementId',
  )

  if (!Number.isFinite(intentScore)) {
    throw new Error('intentScore is required and must be a number between 0 and 1.')
  }

  return {
    requestId,
    appId,
    accountId,
    sessionId,
    turnId,
    query,
    answerText,
    intentScore,
    locale,
    kind,
    adId,
    placementId,
  }
}

function isNextStepIntentCardPayload(payload) {
  if (!payload || typeof payload !== 'object') return false

  const placementKey = String(payload.placementKey || '').trim()
  if (placementKey === NEXT_STEP_INTENT_CARD_PLACEMENT_KEY) return true

  const event = String(payload.event || '').trim().toLowerCase()
  if (NEXT_STEP_INTENT_CARD_EVENTS.has(event)) return true

  const context = payload.context && typeof payload.context === 'object' ? payload.context : null
  if (!context) return false

  return (
    Object.prototype.hasOwnProperty.call(context, 'intent_class') ||
    Object.prototype.hasOwnProperty.call(context, 'intent_score') ||
    Object.prototype.hasOwnProperty.call(context, 'preference_facets')
  )
}

function normalizeNextStepRecentTurns(value) {
  if (!Array.isArray(value)) return []
  return value
    .map((item) => {
      if (!item || typeof item !== 'object') return null
      const role = String(item.role || '').trim().toLowerCase()
      const content = String(item.content || '').trim()
      if (!role || !content) return null
      return { role, content }
    })
    .filter(Boolean)
    .slice(-8)
}

function normalizeNextStepPreferenceFacets(value) {
  if (!Array.isArray(value)) return []

  return value
    .map((item) => {
      if (!item || typeof item !== 'object') return null
      const facetKey = String(item.facet_key || item.facetKey || '').trim()
      const facetValue = String(item.facet_value || item.facetValue || '').trim()
      if (!facetKey || !facetValue) return null

      const confidence = clampNumber(item.confidence, 0, 1, NaN)
      const source = String(item.source || '').trim()

      return {
        facetKey,
        facetValue,
        confidence: Number.isFinite(confidence) ? confidence : null,
        source: source || '',
      }
    })
    .filter(Boolean)
}

function normalizeNextStepConstraints(value) {
  if (!value || typeof value !== 'object') return null
  const mustInclude = Array.isArray(value.must_include || value.mustInclude)
    ? (value.must_include || value.mustInclude).map((item) => String(item || '').trim()).filter(Boolean)
    : []
  const mustExclude = Array.isArray(value.must_exclude || value.mustExclude)
    ? (value.must_exclude || value.mustExclude).map((item) => String(item || '').trim()).filter(Boolean)
    : []

  if (mustInclude.length === 0 && mustExclude.length === 0) return null
  return {
    mustInclude,
    mustExclude,
  }
}

function normalizeNextStepIntentCardPayload(payload, routeName) {
  const input = payload && typeof payload === 'object' ? payload : {}
  validateNoExtraFields(input, NEXT_STEP_INTENT_CARD_ALLOWED_FIELDS, routeName)

  const requestId = String(input.requestId || '').trim()
  const appId = String(input.appId || input.app_id || '').trim()
  const accountId = normalizeControlPlaneAccountId(
    input.accountId || input.account_id || (appId ? resolveAccountIdForApp(appId) : ''),
    '',
  )
  const sessionId = requiredNonEmptyString(input.sessionId, 'sessionId')
  const turnId = requiredNonEmptyString(input.turnId, 'turnId')
  const placementId = assertPlacementIdNotRenamed(
    requiredNonEmptyString(input.placementId, 'placementId'),
    'placementId',
  )
  const placementKey = requiredNonEmptyString(input.placementKey, 'placementKey')
  const event = String(input.event || '').trim().toLowerCase()
  const userId = String(input.userId || '').trim()
  const kind = normalizeNextStepEventKind(input.kind)
  const adId = String(input.adId || '').trim()

  if (placementKey !== NEXT_STEP_INTENT_CARD_PLACEMENT_KEY) {
    throw new Error(`placementKey must be ${NEXT_STEP_INTENT_CARD_PLACEMENT_KEY}.`)
  }

  if (!NEXT_STEP_INTENT_CARD_EVENTS.has(event)) {
    throw new Error('event must be followup_generation or follow_up_generation.')
  }

  const rawContext = input.context && typeof input.context === 'object' ? input.context : null
  if (!rawContext) {
    throw new Error('context is required.')
  }
  validateNoExtraFields(rawContext, NEXT_STEP_INTENT_CARD_CONTEXT_ALLOWED_FIELDS, `${routeName}.context`)

  const query = requiredNonEmptyString(rawContext.query, 'context.query')
  const locale = requiredNonEmptyString(rawContext.locale, 'context.locale')
  const rawIntentClass = String(rawContext.intent_class || rawContext.intentClass || '').trim().toLowerCase()
  const rawIntentScore = clampNumber(rawContext.intent_score ?? rawContext.intentScore, 0, 1, NaN)
  const rawPreferenceFacets = normalizeNextStepPreferenceFacets(
    rawContext.preference_facets ?? rawContext.preferenceFacets,
  )

  const expectedRevenue = clampNumber(rawContext.expected_revenue, 0, Number.MAX_SAFE_INTEGER, NaN)

  return {
    requestId,
    appId,
    accountId,
    sessionId,
    turnId,
    userId,
    event,
    kind,
    adId,
    placementId,
    placementKey,
    context: {
      query,
      answerText: String(rawContext.answerText || '').trim(),
      recentTurns: normalizeNextStepRecentTurns(rawContext.recent_turns),
      locale,
      intentClass: '',
      intentScore: 0,
      preferenceFacets: [],
      intentHints: {
        ...(rawIntentClass ? { intent_class: rawIntentClass } : {}),
        ...(Number.isFinite(rawIntentScore) ? { intent_score: rawIntentScore } : {}),
        ...(rawPreferenceFacets.length > 0 ? { preference_facets: rawPreferenceFacets.map((facet) => ({
          facet_key: facet.facetKey,
          facet_value: facet.facetValue,
          ...(Number.isFinite(facet.confidence) ? { confidence: facet.confidence } : {}),
          ...(facet.source ? { source: facet.source } : {}),
        })) } : {}),
      },
      constraints: normalizeNextStepConstraints(rawContext.constraints),
      blockedTopics: normalizeStringList(rawContext.blocked_topics),
      expectedRevenue: Number.isFinite(expectedRevenue) ? expectedRevenue : undefined,
    },
  }
}

function normalizeIntentCardRetrievePayload(payload, routeName) {
  const input = payload && typeof payload === 'object' ? payload : {}
  validateNoExtraFields(input, INTENT_CARD_RETRIEVE_ALLOWED_FIELDS, routeName)

  const query = requiredNonEmptyString(input.query, 'query')
  const facets = normalizeNextStepPreferenceFacets(input.facets)
  const topK = toPositiveInteger(input.topK, 3) || 3
  const minScore = clampNumber(input.minScore, 0, 1, 0)
  const catalog = normalizeIntentCardCatalogItems(input.catalog)

  if (!Array.isArray(input.catalog)) {
    throw new Error('catalog must be an array.')
  }
  if (catalog.length === 0) {
    throw new Error('catalog must contain at least one valid item.')
  }

  return {
    query,
    facets,
    topK: Math.min(20, topK),
    minScore,
    catalog,
  }
}

function mapInferenceFacetsToInternal(value) {
  if (!Array.isArray(value)) return []
  return value
    .map((facet) => {
      if (!facet || typeof facet !== 'object') return null
      const facetKey = String(facet.facet_key || '').trim()
      const facetValue = String(facet.facet_value || '').trim()
      if (!facetKey || !facetValue) return null
      const confidence = clampNumber(facet.confidence, 0, 1, NaN)
      const source = String(facet.source || '').trim()

      return {
        facetKey,
        facetValue,
        confidence: Number.isFinite(confidence) ? confidence : null,
        source: source || '',
      }
    })
    .filter(Boolean)
}

function mapInternalFacetsToInference(value) {
  if (!Array.isArray(value)) return []
  return value
    .map((facet) => {
      if (!facet || typeof facet !== 'object') return null
      const facetKey = String(facet.facetKey || '').trim()
      const facetValue = String(facet.facetValue || '').trim()
      if (!facetKey || !facetValue) return null
      const confidence = clampNumber(facet.confidence, 0, 1, NaN)
      const source = String(facet.source || '').trim()
      return {
        facet_key: facetKey,
        facet_value: facetValue,
        ...(Number.isFinite(confidence) ? { confidence } : {}),
        ...(source ? { source } : {}),
      }
    })
    .filter(Boolean)
}

function normalizeHintIntentClass(value) {
  const text = String(value || '').trim().toLowerCase()
  if (!text) return ''
  if (!NEXT_STEP_INTENT_CLASSES.has(text)) return ''
  return text
}

function mergeUniqueStrings(...values) {
  const set = new Set()
  for (const value of values) {
    if (!Array.isArray(value)) continue
    for (const item of value) {
      const normalized = String(item || '').trim()
      if (!normalized) continue
      set.add(normalized)
    }
  }
  return Array.from(set)
}

function mergeConstraints(primary, secondary) {
  const normalizedPrimary = normalizeNextStepConstraints(primary)
  const normalizedSecondary = normalizeNextStepConstraints(secondary)

  if (!normalizedPrimary && !normalizedSecondary) return null
  return {
    mustInclude: mergeUniqueStrings(normalizedPrimary?.mustInclude, normalizedSecondary?.mustInclude),
    mustExclude: mergeUniqueStrings(normalizedPrimary?.mustExclude, normalizedSecondary?.mustExclude),
  }
}

async function resolveIntentInferenceForNextStep(request) {
  const context = request?.context && typeof request.context === 'object' ? request.context : {}
  const hints = context.intentHints && typeof context.intentHints === 'object'
    ? context.intentHints
    : {}

  const inference = await inferIntentWithLlm({
    query: context.query || '',
    answerText: context.answerText || '',
    locale: context.locale || 'en-US',
    recentTurns: Array.isArray(context.recentTurns) ? context.recentTurns : [],
    hints,
  })

  const hintIntentClass = normalizeHintIntentClass(hints?.intent_class)
  const hintIntentScore = clampNumber(hints?.intent_score, 0, 1, NaN)
  const hintPreferenceFacets = normalizeNextStepPreferenceFacets(hints?.preference_facets)
  const fallbackUseClientHints = Boolean(inference?.fallbackUsed) && Boolean(hintIntentClass)

  const resolvedIntentClass = fallbackUseClientHints
    ? hintIntentClass
    : String(inference?.intent_class || 'non_commercial').trim().toLowerCase()
  const resolvedIntentScore = fallbackUseClientHints
    ? (Number.isFinite(hintIntentScore) ? hintIntentScore : 0)
    : (Number.isFinite(inference?.intent_score)
      ? clampNumber(inference.intent_score, 0, 1, 0)
      : 0)
  const resolvedPreferenceFacets = fallbackUseClientHints
    ? hintPreferenceFacets
    : mapInferenceFacetsToInternal(inference?.preference_facets)
  const resolvedConstraints = mergeConstraints(context.constraints, inference?.constraints)
  const effectiveInference = fallbackUseClientHints
    ? {
        ...inference,
        intent_class: resolvedIntentClass,
        intent_score: resolvedIntentScore,
        preference_facets: mapInternalFacetsToInference(resolvedPreferenceFacets),
        inference_trace: [
          ...(Array.isArray(inference?.inference_trace) ? inference.inference_trace : []),
          'fallback:client_hints_applied',
        ].slice(0, 10),
      }
    : inference

  return {
    inference: effectiveInference,
    resolvedContext: {
      ...context,
      intentClass: resolvedIntentClass || 'non_commercial',
      intentScore: resolvedIntentScore,
      preferenceFacets: resolvedPreferenceFacets,
      constraints: resolvedConstraints,
    },
  }
}

function toHttpUrl(value) {
  const raw = String(value || '').trim()
  if (!raw) return ''
  try {
    const parsed = new URL(raw)
    const protocol = String(parsed.protocol || '').toLowerCase()
    if (protocol !== 'http:' && protocol !== 'https:') return ''
    return parsed.toString()
  } catch {
    return ''
  }
}

function mapRuntimeAdToNextStepCardItem(ad, index) {
  if (!ad || typeof ad !== 'object') return null
  const title = String(ad.title || '').trim()
  const targetUrl = String(ad.targetUrl || '').trim()
  if (!title || !targetUrl) return null

  const itemId = String(ad.offerId || ad.adId || ad.entityCanonicalId || `next_step_item_${index}`).trim()
  const merchantOrNetwork = String(ad.sourceNetwork || ad.networkId || 'affiliate').trim() || 'affiliate'
  const primaryReason = String(ad.reason || '').trim() || 'semantic_match'
  const tracking = ad.tracking && typeof ad.tracking === 'object' ? ad.tracking : {}
  const normalizedTracking = {}

  if (typeof tracking.impressionUrl === 'string' && tracking.impressionUrl.trim()) {
    normalizedTracking.impression_url = tracking.impressionUrl.trim()
  }
  if (typeof tracking.clickUrl === 'string' && tracking.clickUrl.trim()) {
    normalizedTracking.click_url = tracking.clickUrl.trim()
  }
  if (typeof tracking.dismissUrl === 'string' && tracking.dismissUrl.trim()) {
    normalizedTracking.dismiss_url = tracking.dismissUrl.trim()
  }
  const imageUrl = toHttpUrl(
    ad.image_url
    || ad.imageUrl
    || ad.icon_url
    || ad.iconUrl,
  )

  const cardItem = {
    item_id: itemId,
    title,
    target_url: targetUrl,
    merchant_or_network: merchantOrNetwork,
    match_reasons: [primaryReason],
    disclosure: normalizeDisclosure(ad.disclosure),
  }

  if (typeof ad.description === 'string' && ad.description.trim()) {
    cardItem.snippet = ad.description.trim()
  }
  if (typeof ad.priceHint === 'string' && ad.priceHint.trim()) {
    cardItem.price_hint = ad.priceHint.trim()
  }
  if (typeof ad.relevanceScore === 'number' && Number.isFinite(ad.relevanceScore)) {
    cardItem.relevance_score = clampNumber(ad.relevanceScore, 0, 1, 0)
  }
  if (imageUrl) {
    cardItem.image_url = imageUrl
  }
  if (Object.keys(normalizedTracking).length > 0) {
    cardItem.tracking = normalizedTracking
  }

  return cardItem
}

function buildNextStepIntentCardResponse(result, request, inference) {
  const ads = Array.isArray(result?.ads)
    ? result.ads.map((item, index) => mapRuntimeAdToNextStepCardItem(item, index)).filter(Boolean)
    : []
  const intentScore = Number.isFinite(inference?.intent_score)
    ? inference.intent_score
    : Number.isFinite(request?.context?.intentScore)
      ? request.context.intentScore
      : 0
  const decision = result?.decision && typeof result.decision === 'object' ? result.decision : {}
  const constraints = inference?.constraints || request?.context?.constraints

  const response = {
    requestId: result?.requestId || createId('adreq'),
    placementId: result?.placementId || request?.placementId || '',
    placementKey: NEXT_STEP_INTENT_CARD_PLACEMENT_KEY,
    decision: {
      result: DECISION_REASON_ENUM.has(decision.result) ? decision.result : 'error',
      reason: DECISION_REASON_ENUM.has(decision.reason) ? decision.reason : 'error',
      reasonDetail: String(decision.reasonDetail || decision.result || 'error'),
      intent_score: Number.isFinite(decision.intentScore) ? decision.intentScore : intentScore,
    },
    intent_inference: {
      intent_class: String(inference?.intent_class || request?.context?.intentClass || 'non_commercial'),
      intent_score: intentScore,
      preference_facets: Array.isArray(inference?.preference_facets) ? inference.preference_facets : [],
    },
    ads,
    meta: {
      selected_count: ads.length,
      model_version: String(inference?.model || ''),
      inference_fallback: Boolean(inference?.fallbackUsed),
      inference_fallback_reason: String(inference?.fallbackReason || ''),
    },
  }

  if (constraints) {
    response.intent_inference.constraints = {
      ...(constraints.mustInclude?.length ? { must_include: constraints.mustInclude } : {}),
      ...(constraints.mustExclude?.length ? { must_exclude: constraints.mustExclude } : {}),
      ...(constraints.must_include?.length ? { must_include: constraints.must_include } : {}),
      ...(constraints.must_exclude?.length ? { must_exclude: constraints.must_exclude } : {}),
    }
  }

  if (Array.isArray(inference?.inference_trace) && inference.inference_trace.length > 0) {
    response.intent_inference.inference_trace = inference.inference_trace.slice(0, 8)
  }

  return response
}

function toPositiveInteger(value, fallback) {
  const n = Number(value)
  if (!Number.isFinite(n) || n < 0) return fallback
  return Math.floor(n)
}

function layerFromPlacementKey(placementKey = '') {
  if (placementKey.startsWith('attach.')) return 'attach'
  if (placementKey.startsWith('next_step.')) return 'next_step'
  if (placementKey.startsWith('intervention.')) return 'intervention'
  if (placementKey.startsWith('takeover.')) return 'takeover'
  return 'unknown'
}

function normalizeBidderConfig(value) {
  const input = value && typeof value === 'object' ? value : {}
  const networkId = String(input.networkId || input.network_id || '').trim().toLowerCase()
  if (!networkId) return null

  return {
    networkId,
    endpoint: String(input.endpoint || '').trim(),
    timeoutMs: toPositiveInteger(input.timeoutMs ?? input.timeout_ms, 800),
    enabled: input.enabled !== false,
    policyWeight: clampNumber(input.policyWeight ?? input.policy_weight, -1000, 1000, 0),
  }
}

function normalizePlacementBidders(value = []) {
  const rows = Array.isArray(value) ? value : []
  const dedupe = new Set()
  const normalized = []

  for (const row of rows) {
    const bidder = normalizeBidderConfig(row)
    if (!bidder) continue
    if (dedupe.has(bidder.networkId)) continue
    dedupe.add(bidder.networkId)
    normalized.push(bidder)
  }

  if (normalized.length > 0) return normalized

  return [
    {
      networkId: 'partnerstack',
      endpoint: '',
      timeoutMs: 800,
      enabled: true,
      policyWeight: 0,
    },
    {
      networkId: 'cj',
      endpoint: '',
      timeoutMs: 800,
      enabled: true,
      policyWeight: 0,
    },
  ]
}

function normalizePlacementFallback(value) {
  const input = value && typeof value === 'object' ? value : {}
  const store = input.store && typeof input.store === 'object' ? input.store : {}
  return {
    store: {
      enabled: store.enabled === true,
      floorPrice: clampNumber(store.floorPrice, 0, Number.MAX_SAFE_INTEGER, 0),
    },
  }
}

function normalizePlacementRelevancePolicy(raw = {}) {
  const input = raw && typeof raw === 'object' ? raw : {}
  const strictThreshold = clampNumber(
    input.strictThreshold ?? input.strict,
    0,
    1,
    NaN,
  )
  const relaxedThresholdRaw = clampNumber(
    input.relaxedThreshold ?? input.relaxed,
    0,
    1,
    NaN,
  )
  const hasStrict = Number.isFinite(strictThreshold)
  const hasRelaxed = Number.isFinite(relaxedThresholdRaw)
  const relaxedThreshold = hasStrict && hasRelaxed
    ? Math.min(strictThreshold, relaxedThresholdRaw)
    : (hasRelaxed ? relaxedThresholdRaw : NaN)
  const rolloutPercentRaw = toPositiveInteger(input.rolloutPercent, NaN)
  const rolloutPercent = Number.isFinite(rolloutPercentRaw)
    ? Math.max(1, Math.min(100, rolloutPercentRaw))
    : 100

  return {
    enabled: parseFeatureSwitch(input.enabled, true),
    mode: parseRelevancePolicyMode(input.mode, 'enforce'),
    strictThreshold: hasStrict ? strictThreshold : null,
    relaxedThreshold: Number.isFinite(relaxedThreshold) ? relaxedThreshold : null,
    sameVerticalFallbackEnabled: parseFeatureSwitch(input.sameVerticalFallbackEnabled, true),
    rolloutPercent,
  }
}

function normalizePlacement(raw) {
  const placementId = normalizePlacementIdWithMigration(String(raw?.placementId || '').trim())
  const placementKey = String(raw?.placementKey || PLACEMENT_KEY_BY_ID[placementId] || '').trim()

  return {
    placementId,
    placementKey,
    configVersion: toPositiveInteger(raw?.configVersion, 1),
    enabled: raw?.enabled !== false,
    disclosure: normalizeDisclosure(raw?.disclosure),
    priority: toPositiveInteger(raw?.priority, 100),
    routingMode: MANAGED_ROUTING_MODE,
    surface: String(raw?.surface || 'CHAT_INLINE'),
    format: String(raw?.format || 'CARD'),
    trigger: {
      intentThreshold: clampNumber(raw?.trigger?.intentThreshold, 0, 1, 0.6),
      cooldownSeconds: toPositiveInteger(raw?.trigger?.cooldownSeconds, 0),
      minExpectedRevenue: clampNumber(raw?.trigger?.minExpectedRevenue, 0, Number.MAX_SAFE_INTEGER, 0),
      blockedTopics: normalizeStringList(raw?.trigger?.blockedTopics),
    },
    frequencyCap: {
      maxPerSession: toPositiveInteger(raw?.frequencyCap?.maxPerSession, 0),
      maxPerUserPerDay: toPositiveInteger(raw?.frequencyCap?.maxPerUserPerDay, 0),
    },
    bidders: normalizePlacementBidders(raw?.bidders),
    fallback: normalizePlacementFallback(raw?.fallback),
    relevancePolicyV2: normalizePlacementRelevancePolicy(raw?.relevancePolicyV2),
    maxFanout: toPositiveInteger(raw?.maxFanout, 3),
    globalTimeoutMs: toPositiveInteger(raw?.globalTimeoutMs, 1200),
  }
}

function buildDefaultPlacementList() {
  return defaultPlacements.map((item) => normalizePlacement(item))
}

function normalizePlacementConfigRecord(raw = {}, fallback = {}) {
  const source = raw && typeof raw === 'object' ? raw : {}
  const seed = fallback && typeof fallback === 'object' ? fallback : {}
  const appId = String(source.appId || source.app_id || seed.appId || DEFAULT_CONTROL_PLANE_APP_ID).trim()
    || DEFAULT_CONTROL_PLANE_APP_ID
  const accountId = normalizeControlPlaneAccountId(
    source.accountId || source.account_id || source.organizationId || source.organization_id
      || seed.accountId || seed.organizationId || DEFAULT_CONTROL_PLANE_ORG_ID,
    '',
  )
  const placementSource = (
    Array.isArray(source.placements) && source.placements.length > 0
      ? source.placements
      : (
        Array.isArray(seed.placements) && seed.placements.length > 0
          ? seed.placements
          : buildDefaultPlacementList()
      )
  )
  const placements = placementSource.map((item) => normalizePlacement(item))
  const derivedVersion = Math.max(1, ...placements.map((placement) => placement.configVersion || 1))
  const placementConfigVersion = Math.max(
    toPositiveInteger(source.placementConfigVersion ?? source.configVersion ?? seed.placementConfigVersion, 1),
    derivedVersion,
  )

  return {
    appId,
    accountId,
    placementConfigVersion,
    placements,
    updatedAt: String(source.updatedAt || seed.updatedAt || nowIso()),
  }
}

function getTodayKey(timestamp = Date.now()) {
  const d = new Date(timestamp)
  const yyyy = d.getFullYear()
  const mm = String(d.getMonth() + 1).padStart(2, '0')
  const dd = String(d.getDate()).padStart(2, '0')
  return `${yyyy}-${mm}-${dd}`
}

function createDailyMetricsSeed(days = 7) {
  const rows = []
  const today = new Date()
  for (let i = days - 1; i >= 0; i -= 1) {
    const date = new Date(today)
    date.setDate(today.getDate() - i)
    rows.push({
      date: getTodayKey(date.getTime()),
      impressions: 0,
      clicks: 0,
      revenueUsd: 0,
    })
  }
  return rows
}

function ensureDailyMetricsWindow(dailyMetrics = []) {
  const rows = Array.isArray(dailyMetrics) ? [...dailyMetrics] : []
  const known = new Set(rows.map((row) => row.date))
  const seed = createDailyMetricsSeed(7)

  for (const item of seed) {
    if (!known.has(item.date)) rows.push(item)
  }

  rows.sort((a, b) => a.date.localeCompare(b.date))
  return rows.slice(-7)
}

function initialPlacementStats(placements) {
  const stats = {}
  for (const placement of placements) {
    stats[placement.placementId] = {
      requests: 0,
      served: 0,
      impressions: 0,
      clicks: 0,
      revenueUsd: 0,
    }
  }
  return stats
}

function normalizePlacementStatsSnapshot(rawStats = {}, placements = []) {
  const source = rawStats && typeof rawStats === 'object' ? rawStats : {}
  const stats = {}
  for (const [rawPlacementId, row] of Object.entries(source)) {
    const placementId = normalizePlacementIdWithMigration(rawPlacementId)
    if (!placementId) continue
    const target = stats[placementId] || {
      requests: 0,
      served: 0,
      impressions: 0,
      clicks: 0,
      revenueUsd: 0,
    }
    target.requests += toPositiveInteger(row?.requests, 0)
    target.served += toPositiveInteger(row?.served, 0)
    target.impressions += toPositiveInteger(row?.impressions, 0)
    target.clicks += toPositiveInteger(row?.clicks, 0)
    target.revenueUsd = round(target.revenueUsd + clampNumber(row?.revenueUsd, 0, Number.MAX_SAFE_INTEGER, 0), 4)
    stats[placementId] = target
  }
  for (const placement of Array.isArray(placements) ? placements : []) {
    const placementId = String(placement?.placementId || '').trim()
    if (!placementId || stats[placementId]) continue
    stats[placementId] = {
      requests: 0,
      served: 0,
      impressions: 0,
      clicks: 0,
      revenueUsd: 0,
    }
  }
  return stats
}

function normalizeConversionFact(raw) {
  const item = raw && typeof raw === 'object' ? raw : {}
  const appId = String(item.appId || '').trim()
  const requestId = String(item.requestId || '').trim()
  const conversionId = String(item.conversionId || '').trim()
  const createdAt = normalizeIsoTimestamp(item.createdAt, nowIso())
  const occurredAt = normalizeIsoTimestamp(item.occurredAt || item.eventAt, createdAt)

  const typeRaw = String(item.postbackType || '').trim().toLowerCase()
  const statusRaw = String(item.postbackStatus || '').trim().toLowerCase()
  const postbackType = POSTBACK_TYPES.has(typeRaw) ? typeRaw : 'conversion'
  const postbackStatus = POSTBACK_STATUS.has(statusRaw) ? statusRaw : 'success'
  const factTypeRaw = String(item.factType || '').trim().toLowerCase()
  const factType = factTypeRaw === CONVERSION_FACT_TYPES.CPC
    ? CONVERSION_FACT_TYPES.CPC
    : CONVERSION_FACT_TYPES.CPA
  const cpaUsd = round(clampNumber(item.cpaUsd ?? item.revenueUsd, 0, Number.MAX_SAFE_INTEGER, 0), 4)
  const revenueUsd = round(clampNumber(item.revenueUsd ?? item.cpaUsd, 0, Number.MAX_SAFE_INTEGER, cpaUsd), 4)
  const fallbackFactId = `fact_${createHash('sha1').update(`${appId}|${requestId}|${conversionId}|${createdAt}`).digest('hex').slice(0, 16)}`

  const placementId = normalizePlacementIdWithMigration(String(item.placementId || '').trim())
  const placementKey = String(item.placementKey || '').trim() || resolvePlacementKeyById(placementId, appId)

  return {
    factId: String(item.factId || '').trim() || fallbackFactId,
    factType,
    appId,
    accountId: normalizeControlPlaneAccountId(item.accountId || resolveAccountIdForApp(appId), ''),
    requestId,
    sessionId: String(item.sessionId || '').trim(),
    turnId: String(item.turnId || '').trim(),
    userId: String(item.userId || '').trim(),
    placementId,
    placementKey,
    adId: String(item.adId || '').trim(),
    postbackType,
    postbackStatus,
    conversionId,
    eventSeq: String(item.eventSeq || '').trim(),
    occurredAt,
    createdAt,
    cpaUsd,
    revenueUsd: postbackStatus === 'success' ? revenueUsd : 0,
    currency: 'USD',
    idempotencyKey: String(item.idempotencyKey || '').trim(),
  }
}

function mapPostgresRowToConversionFact(row) {
  if (!row || typeof row !== 'object') return null
  return normalizeConversionFact({
    factId: row.fact_id,
    factType: row.fact_type,
    appId: row.app_id,
    accountId: row.account_id,
    requestId: row.request_id,
    sessionId: row.session_id,
    turnId: row.turn_id,
    userId: row.user_id,
    placementId: row.placement_id,
    placementKey: row.placement_key,
    adId: row.ad_id,
    postbackType: row.postback_type,
    postbackStatus: row.postback_status,
    conversionId: row.conversion_id,
    eventSeq: row.event_seq,
    occurredAt: row.occurred_at,
    createdAt: row.created_at,
    cpaUsd: row.cpa_usd,
    revenueUsd: row.revenue_usd,
    currency: row.currency,
    idempotencyKey: row.idempotency_key,
  })
}

async function ensureSettlementFactTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${SETTLEMENT_FACT_TABLE} (
      fact_id TEXT PRIMARY KEY,
      fact_type TEXT NOT NULL,
      app_id TEXT NOT NULL,
      account_id TEXT NOT NULL,
      request_id TEXT NOT NULL,
      session_id TEXT NOT NULL DEFAULT '',
      turn_id TEXT NOT NULL DEFAULT '',
      user_id TEXT NOT NULL DEFAULT '',
      placement_id TEXT NOT NULL DEFAULT '',
      placement_key TEXT NOT NULL DEFAULT '',
      ad_id TEXT NOT NULL DEFAULT '',
      postback_type TEXT NOT NULL,
      postback_status TEXT NOT NULL,
      conversion_id TEXT NOT NULL,
      event_seq TEXT NOT NULL DEFAULT '',
      occurred_at TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      cpa_usd NUMERIC(18, 4) NOT NULL DEFAULT 0,
      revenue_usd NUMERIC(18, 4) NOT NULL DEFAULT 0,
      currency TEXT NOT NULL DEFAULT 'USD',
      idempotency_key TEXT NOT NULL UNIQUE
    )
  `)
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${SETTLEMENT_FACT_TABLE}_account_app ON ${SETTLEMENT_FACT_TABLE} (account_id, app_id, occurred_at DESC)`)
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${SETTLEMENT_FACT_TABLE}_request ON ${SETTLEMENT_FACT_TABLE} (request_id)`)
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${SETTLEMENT_FACT_TABLE}_placement ON ${SETTLEMENT_FACT_TABLE} (placement_id, occurred_at DESC)`)
}

async function ensureRuntimeLogTables(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${RUNTIME_DECISION_LOG_TABLE} (
      id TEXT PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL,
      request_id TEXT NOT NULL DEFAULT '',
      app_id TEXT NOT NULL DEFAULT '',
      account_id TEXT NOT NULL DEFAULT '',
      session_id TEXT NOT NULL DEFAULT '',
      turn_id TEXT NOT NULL DEFAULT '',
      event TEXT NOT NULL DEFAULT '',
      placement_id TEXT NOT NULL DEFAULT '',
      placement_key TEXT NOT NULL DEFAULT '',
      result TEXT NOT NULL DEFAULT '',
      reason TEXT NOT NULL DEFAULT '',
      payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_${RUNTIME_DECISION_LOG_TABLE}_account_app
      ON ${RUNTIME_DECISION_LOG_TABLE} (account_id, app_id, created_at DESC)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_${RUNTIME_DECISION_LOG_TABLE}_request
      ON ${RUNTIME_DECISION_LOG_TABLE} (request_id)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_${RUNTIME_DECISION_LOG_TABLE}_placement
      ON ${RUNTIME_DECISION_LOG_TABLE} (placement_id, created_at DESC)
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${RUNTIME_EVENT_LOG_TABLE} (
      id TEXT PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL,
      event_type TEXT NOT NULL DEFAULT '',
      event TEXT NOT NULL DEFAULT '',
      kind TEXT NOT NULL DEFAULT '',
      request_id TEXT NOT NULL DEFAULT '',
      app_id TEXT NOT NULL DEFAULT '',
      account_id TEXT NOT NULL DEFAULT '',
      session_id TEXT NOT NULL DEFAULT '',
      turn_id TEXT NOT NULL DEFAULT '',
      placement_id TEXT NOT NULL DEFAULT '',
      placement_key TEXT NOT NULL DEFAULT '',
      result TEXT NOT NULL DEFAULT '',
      payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_${RUNTIME_EVENT_LOG_TABLE}_account_app
      ON ${RUNTIME_EVENT_LOG_TABLE} (account_id, app_id, created_at DESC)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_${RUNTIME_EVENT_LOG_TABLE}_request
      ON ${RUNTIME_EVENT_LOG_TABLE} (request_id)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_${RUNTIME_EVENT_LOG_TABLE}_placement
      ON ${RUNTIME_EVENT_LOG_TABLE} (placement_id, created_at DESC)
  `)
}

async function ensureControlPlaneTables(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${CONTROL_PLANE_APPS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      app_id TEXT NOT NULL UNIQUE,
      organization_id TEXT NOT NULL,
      display_name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (status IN ('active', 'disabled', 'archived'))
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_control_plane_apps_org_status
      ON ${CONTROL_PLANE_APPS_TABLE} (organization_id, status, created_at DESC)
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${CONTROL_PLANE_APP_ENVIRONMENTS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      environment_id TEXT NOT NULL UNIQUE,
      app_id TEXT NOT NULL REFERENCES ${CONTROL_PLANE_APPS_TABLE}(app_id) ON DELETE CASCADE ON UPDATE CASCADE,
      environment TEXT NOT NULL,
      api_base_url TEXT,
      status TEXT NOT NULL DEFAULT 'active',
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (app_id, environment),
      CHECK (environment IN ('prod')),
      CHECK (status IN ('active', 'disabled'))
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_control_plane_env_app_env
      ON ${CONTROL_PLANE_APP_ENVIRONMENTS_TABLE} (app_id, environment)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_control_plane_env_status
      ON ${CONTROL_PLANE_APP_ENVIRONMENTS_TABLE} (status, created_at DESC)
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${CONTROL_PLANE_API_KEYS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      key_id TEXT NOT NULL UNIQUE,
      app_id TEXT NOT NULL REFERENCES ${CONTROL_PLANE_APPS_TABLE}(app_id) ON DELETE CASCADE ON UPDATE CASCADE,
      environment TEXT NOT NULL,
      key_name TEXT NOT NULL,
      key_prefix TEXT NOT NULL,
      secret_hash TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL DEFAULT 'active',
      revoked_at TIMESTAMPTZ,
      last_used_at TIMESTAMPTZ,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (environment IN ('prod')),
      CHECK (status IN ('active', 'revoked'))
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_control_plane_api_keys_app_env_status
      ON ${CONTROL_PLANE_API_KEYS_TABLE} (app_id, environment, status, created_at DESC)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_control_plane_api_keys_prefix
      ON ${CONTROL_PLANE_API_KEYS_TABLE} (key_prefix)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_control_plane_api_keys_last_used
      ON ${CONTROL_PLANE_API_KEYS_TABLE} (last_used_at DESC)
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${CONTROL_PLANE_DASHBOARD_USERS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      user_id TEXT NOT NULL UNIQUE,
      email TEXT NOT NULL UNIQUE,
      display_name TEXT NOT NULL,
      account_id TEXT NOT NULL,
      app_id TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'active',
      password_hash TEXT NOT NULL,
      password_salt TEXT NOT NULL,
      last_login_at TIMESTAMPTZ,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (status IN ('active', 'disabled'))
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cp_dashboard_users_account
      ON ${CONTROL_PLANE_DASHBOARD_USERS_TABLE} (account_id, created_at DESC)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cp_dashboard_users_app
      ON ${CONTROL_PLANE_DASHBOARD_USERS_TABLE} (app_id, created_at DESC)
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${CONTROL_PLANE_DASHBOARD_SESSIONS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      session_id TEXT NOT NULL UNIQUE,
      token_hash TEXT NOT NULL UNIQUE,
      user_id TEXT NOT NULL REFERENCES ${CONTROL_PLANE_DASHBOARD_USERS_TABLE}(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
      email TEXT NOT NULL,
      account_id TEXT NOT NULL,
      app_id TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'active',
      issued_at TIMESTAMPTZ NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      revoked_at TIMESTAMPTZ,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (status IN ('active', 'expired', 'revoked'))
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cp_dashboard_sessions_user
      ON ${CONTROL_PLANE_DASHBOARD_SESSIONS_TABLE} (user_id, issued_at DESC)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cp_dashboard_sessions_account
      ON ${CONTROL_PLANE_DASHBOARD_SESSIONS_TABLE} (account_id, issued_at DESC)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cp_dashboard_sessions_status
      ON ${CONTROL_PLANE_DASHBOARD_SESSIONS_TABLE} (status, expires_at DESC)
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${CONTROL_PLANE_INTEGRATION_TOKENS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      token_id TEXT NOT NULL UNIQUE,
      app_id TEXT NOT NULL REFERENCES ${CONTROL_PLANE_APPS_TABLE}(app_id) ON DELETE CASCADE ON UPDATE CASCADE,
      account_id TEXT NOT NULL,
      environment TEXT NOT NULL,
      placement_id TEXT NOT NULL DEFAULT '',
      token_hash TEXT NOT NULL UNIQUE,
      token_type TEXT NOT NULL DEFAULT 'integration_token',
      one_time BOOLEAN NOT NULL DEFAULT TRUE,
      status TEXT NOT NULL DEFAULT 'active',
      scope JSONB NOT NULL DEFAULT '{}'::jsonb,
      issued_at TIMESTAMPTZ NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      used_at TIMESTAMPTZ,
      revoked_at TIMESTAMPTZ,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (environment IN ('prod')),
      CHECK (status IN ('active', 'used', 'expired', 'revoked'))
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cp_integration_tokens_account
      ON ${CONTROL_PLANE_INTEGRATION_TOKENS_TABLE} (account_id, app_id, issued_at DESC)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cp_integration_tokens_status
      ON ${CONTROL_PLANE_INTEGRATION_TOKENS_TABLE} (status, expires_at DESC)
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${CONTROL_PLANE_AGENT_ACCESS_TOKENS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      token_id TEXT NOT NULL UNIQUE,
      app_id TEXT NOT NULL REFERENCES ${CONTROL_PLANE_APPS_TABLE}(app_id) ON DELETE CASCADE ON UPDATE CASCADE,
      account_id TEXT NOT NULL,
      environment TEXT NOT NULL,
      placement_id TEXT NOT NULL DEFAULT '',
      source_token_id TEXT NOT NULL DEFAULT '',
      token_hash TEXT NOT NULL UNIQUE,
      token_type TEXT NOT NULL DEFAULT 'agent_access_token',
      status TEXT NOT NULL DEFAULT 'active',
      scope JSONB NOT NULL DEFAULT '{}'::jsonb,
      issued_at TIMESTAMPTZ NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (environment IN ('prod')),
      CHECK (status IN ('active', 'expired', 'revoked'))
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cp_agent_access_tokens_account
      ON ${CONTROL_PLANE_AGENT_ACCESS_TOKENS_TABLE} (account_id, app_id, issued_at DESC)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cp_agent_access_tokens_status
      ON ${CONTROL_PLANE_AGENT_ACCESS_TOKENS_TABLE} (status, expires_at DESC)
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${CONTROL_PLANE_ALLOWED_ORIGINS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      origin TEXT NOT NULL UNIQUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cp_allowed_origins_updated
      ON ${CONTROL_PLANE_ALLOWED_ORIGINS_TABLE} (updated_at DESC)
  `)
}

async function ensureCampaignBudgetTables(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${CAMPAIGNS_TABLE} (
      campaign_id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL DEFAULT '',
      app_id TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'active',
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (status IN ('active', 'paused', 'archived'))
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_campaigns_account_app
      ON ${CAMPAIGNS_TABLE} (account_id, app_id, updated_at DESC)
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${CAMPAIGN_BUDGET_LIMITS_TABLE} (
      campaign_id TEXT PRIMARY KEY REFERENCES ${CAMPAIGNS_TABLE}(campaign_id) ON DELETE CASCADE ON UPDATE CASCADE,
      daily_budget_usd NUMERIC(18, 4),
      lifetime_budget_usd NUMERIC(18, 4) NOT NULL,
      currency TEXT NOT NULL DEFAULT 'USD',
      timezone TEXT NOT NULL DEFAULT 'UTC',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (lifetime_budget_usd > 0),
      CHECK (daily_budget_usd IS NULL OR daily_budget_usd > 0)
    )
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${BUDGET_RESERVATIONS_TABLE} (
      reservation_id TEXT PRIMARY KEY,
      campaign_id TEXT NOT NULL REFERENCES ${CAMPAIGNS_TABLE}(campaign_id) ON DELETE CASCADE ON UPDATE CASCADE,
      account_id TEXT NOT NULL DEFAULT '',
      app_id TEXT NOT NULL DEFAULT '',
      request_id TEXT NOT NULL,
      ad_id TEXT NOT NULL DEFAULT '',
      reserved_cpc_usd NUMERIC(18, 4) NOT NULL,
      pricing_semantics_version TEXT NOT NULL DEFAULT 'cpc_v1',
      status TEXT NOT NULL DEFAULT 'reserved',
      reason_code TEXT NOT NULL DEFAULT '',
      expires_at TIMESTAMPTZ NOT NULL,
      settled_fact_id TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (status IN ('reserved', 'settled', 'released', 'expired')),
      CHECK (reserved_cpc_usd > 0)
    )
  `)
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_budget_reservation_request_campaign_ad
      ON ${BUDGET_RESERVATIONS_TABLE} (request_id, campaign_id, ad_id)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_budget_reservation_campaign_status
      ON ${BUDGET_RESERVATIONS_TABLE} (campaign_id, status, expires_at DESC)
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${BUDGET_LEDGER_TABLE} (
      ledger_id TEXT PRIMARY KEY,
      campaign_id TEXT NOT NULL REFERENCES ${CAMPAIGNS_TABLE}(campaign_id) ON DELETE CASCADE ON UPDATE CASCADE,
      reservation_id TEXT NOT NULL DEFAULT '',
      request_id TEXT NOT NULL DEFAULT '',
      fact_id TEXT NOT NULL DEFAULT '',
      entry_type TEXT NOT NULL,
      amount_usd NUMERIC(18, 4) NOT NULL,
      currency TEXT NOT NULL DEFAULT 'USD',
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (entry_type IN ('reserve', 'release', 'settle')),
      CHECK (amount_usd >= 0)
    )
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_budget_ledger_campaign_created
      ON ${BUDGET_LEDGER_TABLE} (campaign_id, created_at DESC)
  `)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_budget_ledger_request
      ON ${BUDGET_LEDGER_TABLE} (request_id, created_at DESC)
  `)
}

async function migrateLegacyPlacementIdsInSupabase(pool) {
  const db = pool || settlementStore.pool
  if (!db) {
    return {
      executed: false,
      totalUpdatedRows: 0,
      updatedRowsByTable: {},
    }
  }

  const updatedRowsByTable = {}
  let totalUpdatedRows = 0
  const migrationEntries = Object.entries(LEGACY_PLACEMENT_ID_MAP)

  for (const target of PLACEMENT_ID_MIGRATION_TABLES) {
    const table = String(target?.table || '').trim()
    const column = String(target?.column || '').trim()
    if (!table || !column) continue

    let tableUpdatedRows = 0
    const byLegacy = {}

    for (const [legacyPlacementId, replacementPlacementId] of migrationEntries) {
      const result = await db.query(
        `UPDATE ${table} SET ${column} = $1 WHERE ${column} = $2`,
        [replacementPlacementId, legacyPlacementId],
      )
      const rowCount = Number(result?.rowCount)
      const affectedRows = Number.isFinite(rowCount) && rowCount > 0 ? Math.floor(rowCount) : 0
      byLegacy[`${legacyPlacementId}->${replacementPlacementId}`] = affectedRows
      tableUpdatedRows += affectedRows
      totalUpdatedRows += affectedRows
    }

    updatedRowsByTable[table] = {
      column,
      updatedRows: tableUpdatedRows,
      byLegacy,
    }
  }

  return {
    executed: true,
    totalUpdatedRows,
    updatedRowsByTable,
  }
}

function upsertControlPlaneStateRecord(collectionKey, recordKey, record, max = 0) {
  if (!state?.controlPlane || typeof state.controlPlane !== 'object') {
    state.controlPlane = createInitialControlPlaneState()
  }
  const rows = Array.isArray(state.controlPlane[collectionKey]) ? state.controlPlane[collectionKey] : []
  const key = String(record?.[recordKey] || '').trim()
  if (!key) return
  const nextRows = [record, ...rows.filter((item) => String(item?.[recordKey] || '').trim() !== key)]
  state.controlPlane[collectionKey] = max > 0 ? nextRows.slice(0, max) : nextRows
}

function upsertControlPlaneEnvironmentStateRecord(record) {
  if (!record || typeof record !== 'object') return
  if (!state?.controlPlane || typeof state.controlPlane !== 'object') {
    state.controlPlane = createInitialControlPlaneState()
  }
  const rows = Array.isArray(state.controlPlane.appEnvironments) ? state.controlPlane.appEnvironments : []
  const appId = String(record.appId || '').trim()
  const environment = normalizeControlPlaneEnvironment(record.environment)
  if (!appId) return
  const dedupKey = `${appId}::${environment}`
  state.controlPlane.appEnvironments = [
    {
      ...record,
      appId,
      environment,
    },
    ...rows.filter((item) => `${String(item?.appId || '').trim()}::${normalizeControlPlaneEnvironment(item?.environment)}` !== dedupKey),
  ]
}

async function loadControlPlaneStateFromSupabase(pool) {
  const db = pool || settlementStore.pool
  if (!db) return createInitialControlPlaneState()

  const [
    appsResult,
    environmentsResult,
    keysResult,
    usersResult,
    sessionsResult,
    integrationTokensResult,
    agentTokensResult,
  ] = await Promise.all([
    db.query(`
      SELECT
        app_id,
        organization_id AS account_id,
        display_name,
        status,
        metadata,
        created_at,
        updated_at
      FROM ${CONTROL_PLANE_APPS_TABLE}
      ORDER BY updated_at DESC
    `),
    db.query(`
      SELECT
        env.environment_id,
        env.app_id,
        apps.organization_id AS account_id,
        env.environment,
        env.api_base_url,
        env.status,
        env.metadata,
        env.created_at,
        env.updated_at
      FROM ${CONTROL_PLANE_APP_ENVIRONMENTS_TABLE} AS env
      LEFT JOIN ${CONTROL_PLANE_APPS_TABLE} AS apps
        ON apps.app_id = env.app_id
      ORDER BY env.updated_at DESC
    `),
    db.query(`
      SELECT
        keys.key_id,
        keys.app_id,
        apps.organization_id AS account_id,
        keys.environment,
        keys.key_name,
        keys.key_prefix,
        keys.secret_hash,
        keys.status,
        keys.revoked_at,
        keys.last_used_at,
        keys.metadata,
        keys.created_at,
        keys.updated_at
      FROM ${CONTROL_PLANE_API_KEYS_TABLE} AS keys
      LEFT JOIN ${CONTROL_PLANE_APPS_TABLE} AS apps
        ON apps.app_id = keys.app_id
      ORDER BY keys.updated_at DESC
    `),
    db.query(`
      SELECT
        user_id,
        email,
        display_name,
        account_id,
        app_id,
        status,
        password_hash,
        password_salt,
        last_login_at,
        metadata,
        created_at,
        updated_at
      FROM ${CONTROL_PLANE_DASHBOARD_USERS_TABLE}
      ORDER BY updated_at DESC
    `),
    db.query(`
      SELECT
        session_id,
        token_hash,
        user_id,
        email,
        account_id,
        app_id,
        status,
        issued_at,
        expires_at,
        revoked_at,
        metadata,
        updated_at
      FROM ${CONTROL_PLANE_DASHBOARD_SESSIONS_TABLE}
      ORDER BY issued_at DESC
    `),
    db.query(`
      SELECT
        token_id,
        app_id,
        account_id,
        environment,
        placement_id,
        token_hash,
        token_type,
        one_time,
        status,
        scope,
        issued_at,
        expires_at,
        used_at,
        revoked_at,
        metadata,
        updated_at
      FROM ${CONTROL_PLANE_INTEGRATION_TOKENS_TABLE}
      ORDER BY issued_at DESC
    `),
    db.query(`
      SELECT
        token_id,
        app_id,
        account_id,
        environment,
        placement_id,
        source_token_id,
        token_hash,
        token_type,
        status,
        scope,
        issued_at,
        expires_at,
        metadata,
        updated_at
      FROM ${CONTROL_PLANE_AGENT_ACCESS_TOKENS_TABLE}
      ORDER BY issued_at DESC
    `),
  ])

  const loaded = ensureControlPlaneState({
    apps: Array.isArray(appsResult.rows)
      ? appsResult.rows.map((row) => ({
        appId: row.app_id,
        accountId: row.account_id,
        displayName: row.display_name,
        status: row.status,
        metadata: toDbJsonObject(row.metadata, {}),
        createdAt: normalizeDbTimestamp(row.created_at, nowIso()),
        updatedAt: normalizeDbTimestamp(row.updated_at, nowIso()),
      }))
      : [],
    appEnvironments: Array.isArray(environmentsResult.rows)
      ? environmentsResult.rows.map((row) => ({
        environmentId: row.environment_id,
        appId: row.app_id,
        accountId: row.account_id,
        environment: row.environment,
        apiBaseUrl: row.api_base_url,
        status: row.status,
        metadata: toDbJsonObject(row.metadata, {}),
        createdAt: normalizeDbTimestamp(row.created_at, nowIso()),
        updatedAt: normalizeDbTimestamp(row.updated_at, nowIso()),
      }))
      : [],
    apiKeys: Array.isArray(keysResult.rows)
      ? keysResult.rows.map((row) => ({
        keyId: row.key_id,
        appId: row.app_id,
        accountId: row.account_id,
        environment: row.environment,
        keyName: row.key_name,
        keyPrefix: row.key_prefix,
        secretHash: row.secret_hash,
        status: row.status,
        revokedAt: normalizeDbTimestamp(row.revoked_at, ''),
        lastUsedAt: normalizeDbTimestamp(row.last_used_at, ''),
        metadata: toDbJsonObject(row.metadata, {}),
        createdAt: normalizeDbTimestamp(row.created_at, nowIso()),
        updatedAt: normalizeDbTimestamp(row.updated_at, nowIso()),
      }))
      : [],
    dashboardUsers: Array.isArray(usersResult.rows)
      ? usersResult.rows.map((row) => ({
        userId: row.user_id,
        email: row.email,
        displayName: row.display_name,
        accountId: row.account_id,
        appId: row.app_id,
        status: row.status,
        passwordHash: row.password_hash,
        passwordSalt: row.password_salt,
        lastLoginAt: normalizeDbTimestamp(row.last_login_at, ''),
        metadata: toDbJsonObject(row.metadata, {}),
        createdAt: normalizeDbTimestamp(row.created_at, nowIso()),
        updatedAt: normalizeDbTimestamp(row.updated_at, nowIso()),
      }))
      : [],
    dashboardSessions: Array.isArray(sessionsResult.rows)
      ? sessionsResult.rows.map((row) => ({
        sessionId: row.session_id,
        tokenHash: row.token_hash,
        userId: row.user_id,
        email: row.email,
        accountId: row.account_id,
        appId: row.app_id,
        status: row.status,
        issuedAt: normalizeDbTimestamp(row.issued_at, nowIso()),
        expiresAt: normalizeDbTimestamp(row.expires_at, nowIso()),
        revokedAt: normalizeDbTimestamp(row.revoked_at, ''),
        metadata: toDbJsonObject(row.metadata, {}),
        updatedAt: normalizeDbTimestamp(row.updated_at, nowIso()),
      }))
      : [],
    integrationTokens: Array.isArray(integrationTokensResult.rows)
      ? integrationTokensResult.rows.map((row) => ({
        tokenId: row.token_id,
        appId: row.app_id,
        accountId: row.account_id,
        environment: row.environment,
        placementId: row.placement_id,
        tokenHash: row.token_hash,
        tokenType: row.token_type || 'integration_token',
        oneTime: row.one_time !== false,
        status: row.status,
        scope: toDbJsonObject(row.scope, createMinimalAgentScope()),
        issuedAt: normalizeDbTimestamp(row.issued_at, nowIso()),
        expiresAt: normalizeDbTimestamp(row.expires_at, nowIso()),
        usedAt: normalizeDbTimestamp(row.used_at, ''),
        revokedAt: normalizeDbTimestamp(row.revoked_at, ''),
        metadata: toDbJsonObject(row.metadata, {}),
        updatedAt: normalizeDbTimestamp(row.updated_at, nowIso()),
      }))
      : [],
    agentAccessTokens: Array.isArray(agentTokensResult.rows)
      ? agentTokensResult.rows.map((row) => ({
        tokenId: row.token_id,
        appId: row.app_id,
        accountId: row.account_id,
        environment: row.environment,
        placementId: row.placement_id,
        sourceTokenId: row.source_token_id,
        tokenHash: row.token_hash,
        tokenType: row.token_type || 'agent_access_token',
        status: row.status,
        scope: toDbJsonObject(row.scope, createMinimalAgentScope()),
        issuedAt: normalizeDbTimestamp(row.issued_at, nowIso()),
        expiresAt: normalizeDbTimestamp(row.expires_at, nowIso()),
        metadata: toDbJsonObject(row.metadata, {}),
        updatedAt: normalizeDbTimestamp(row.updated_at, nowIso()),
      }))
      : [],
  })

  state.controlPlane = loaded

  for (const app of loaded.apps) {
    const appId = String(app?.appId || '').trim()
    const accountId = normalizeControlPlaneAccountId(app?.accountId || app?.organizationId, '')
    if (!appId || !accountId) continue
    getPlacementConfigForApp(appId, accountId, { createIfMissing: true })
  }
  if (DEFAULT_CONTROL_PLANE_APP_ID) {
    syncLegacyPlacementSnapshot()
  }

  controlPlaneRefreshState.lastLoadedAt = Date.now()
  return loaded
}

async function upsertControlPlaneAppToSupabase(recordInput, pool = null) {
  const record = buildControlPlaneAppRecord(recordInput)
  if (!record) {
    throw new Error('control plane app record is invalid.')
  }
  const db = pool || settlementStore.pool
  if (!db) return record

  await db.query(
    `
      INSERT INTO ${CONTROL_PLANE_APPS_TABLE} (
        app_id,
        organization_id,
        display_name,
        status,
        metadata,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5::jsonb, $6::timestamptz, $7::timestamptz)
      ON CONFLICT (app_id) DO UPDATE SET
        organization_id = EXCLUDED.organization_id,
        display_name = EXCLUDED.display_name,
        status = EXCLUDED.status,
        metadata = EXCLUDED.metadata,
        updated_at = EXCLUDED.updated_at
    `,
    [
      record.appId,
      record.accountId,
      record.displayName,
      record.status,
      JSON.stringify(toDbJsonObject(record.metadata, {})),
      normalizeDbTimestamp(record.createdAt, nowIso()),
      normalizeDbTimestamp(record.updatedAt, nowIso()),
    ],
  )
  return record
}

async function upsertControlPlaneEnvironmentToSupabase(recordInput, pool = null) {
  const record = buildControlPlaneEnvironmentRecord(recordInput)
  if (!record) {
    throw new Error('control plane app environment record is invalid.')
  }
  const db = pool || settlementStore.pool
  if (!db) return record

  await db.query(
    `
      INSERT INTO ${CONTROL_PLANE_APP_ENVIRONMENTS_TABLE} (
        environment_id,
        app_id,
        environment,
        api_base_url,
        status,
        metadata,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::timestamptz, $8::timestamptz)
      ON CONFLICT (app_id, environment) DO UPDATE SET
        environment_id = EXCLUDED.environment_id,
        api_base_url = EXCLUDED.api_base_url,
        status = EXCLUDED.status,
        metadata = EXCLUDED.metadata,
        updated_at = EXCLUDED.updated_at
    `,
    [
      record.environmentId,
      record.appId,
      record.environment,
      record.apiBaseUrl,
      record.status,
      JSON.stringify(toDbJsonObject(record.metadata, {})),
      normalizeDbTimestamp(record.createdAt, nowIso()),
      normalizeDbTimestamp(record.updatedAt, nowIso()),
    ],
  )
  return record
}

async function upsertControlPlaneKeyToSupabase(recordInput, pool = null) {
  const record = normalizeControlPlaneKeyRecord(recordInput)
  if (!record) {
    throw new Error('control plane api key record is invalid.')
  }
  const db = pool || settlementStore.pool
  if (!db) return record

  await db.query(
    `
      INSERT INTO ${CONTROL_PLANE_API_KEYS_TABLE} (
        key_id,
        app_id,
        environment,
        key_name,
        key_prefix,
        secret_hash,
        status,
        revoked_at,
        last_used_at,
        metadata,
        created_at,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7,
        $8::timestamptz, $9::timestamptz, $10::jsonb, $11::timestamptz, $12::timestamptz
      )
      ON CONFLICT (key_id) DO UPDATE SET
        app_id = EXCLUDED.app_id,
        environment = EXCLUDED.environment,
        key_name = EXCLUDED.key_name,
        key_prefix = EXCLUDED.key_prefix,
        secret_hash = EXCLUDED.secret_hash,
        status = EXCLUDED.status,
        revoked_at = EXCLUDED.revoked_at,
        last_used_at = EXCLUDED.last_used_at,
        metadata = EXCLUDED.metadata,
        updated_at = EXCLUDED.updated_at
    `,
    [
      record.keyId,
      record.appId,
      record.environment,
      record.keyName,
      record.keyPrefix,
      record.secretHash,
      record.status,
      toDbNullableTimestamptz(record.revokedAt),
      toDbNullableTimestamptz(record.lastUsedAt),
      JSON.stringify(toDbJsonObject(record.metadata, {})),
      normalizeDbTimestamp(record.createdAt, nowIso()),
      normalizeDbTimestamp(record.updatedAt, nowIso()),
    ],
  )
  return record
}

async function upsertDashboardUserToSupabase(recordInput, pool = null) {
  const record = normalizeDashboardUserRecord(recordInput)
  if (!record) {
    throw new Error('dashboard user record is invalid.')
  }
  const db = pool || settlementStore.pool
  if (!db) return record

  await db.query(
    `
      INSERT INTO ${CONTROL_PLANE_DASHBOARD_USERS_TABLE} (
        user_id,
        email,
        display_name,
        account_id,
        app_id,
        status,
        password_hash,
        password_salt,
        last_login_at,
        metadata,
        created_at,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8,
        $9::timestamptz, $10::jsonb, $11::timestamptz, $12::timestamptz
      )
      ON CONFLICT (user_id) DO UPDATE SET
        email = EXCLUDED.email,
        display_name = EXCLUDED.display_name,
        account_id = EXCLUDED.account_id,
        app_id = EXCLUDED.app_id,
        status = EXCLUDED.status,
        password_hash = EXCLUDED.password_hash,
        password_salt = EXCLUDED.password_salt,
        last_login_at = EXCLUDED.last_login_at,
        metadata = EXCLUDED.metadata,
        updated_at = EXCLUDED.updated_at
    `,
    [
      record.userId,
      record.email,
      record.displayName,
      record.accountId,
      record.appId,
      record.status,
      record.passwordHash,
      record.passwordSalt,
      toDbNullableTimestamptz(record.lastLoginAt),
      JSON.stringify(toDbJsonObject(record.metadata, {})),
      normalizeDbTimestamp(record.createdAt, nowIso()),
      normalizeDbTimestamp(record.updatedAt, nowIso()),
    ],
  )
  return record
}

async function upsertDashboardSessionToSupabase(recordInput, pool = null) {
  const record = normalizeDashboardSessionRecord(recordInput)
  if (!record) {
    throw new Error('dashboard session record is invalid.')
  }
  const db = pool || settlementStore.pool
  if (!db) return record

  await db.query(
    `
      INSERT INTO ${CONTROL_PLANE_DASHBOARD_SESSIONS_TABLE} (
        session_id,
        token_hash,
        user_id,
        email,
        account_id,
        app_id,
        status,
        issued_at,
        expires_at,
        revoked_at,
        metadata,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7,
        $8::timestamptz, $9::timestamptz, $10::timestamptz,
        $11::jsonb, $12::timestamptz
      )
      ON CONFLICT (session_id) DO UPDATE SET
        token_hash = EXCLUDED.token_hash,
        user_id = EXCLUDED.user_id,
        email = EXCLUDED.email,
        account_id = EXCLUDED.account_id,
        app_id = EXCLUDED.app_id,
        status = EXCLUDED.status,
        issued_at = EXCLUDED.issued_at,
        expires_at = EXCLUDED.expires_at,
        revoked_at = EXCLUDED.revoked_at,
        metadata = EXCLUDED.metadata,
        updated_at = EXCLUDED.updated_at
    `,
    [
      record.sessionId,
      record.tokenHash,
      record.userId,
      record.email,
      record.accountId,
      record.appId,
      record.status,
      normalizeDbTimestamp(record.issuedAt, nowIso()),
      normalizeDbTimestamp(record.expiresAt, nowIso()),
      toDbNullableTimestamptz(record.revokedAt),
      JSON.stringify(toDbJsonObject(record.metadata, {})),
      normalizeDbTimestamp(record.updatedAt, nowIso()),
    ],
  )
  return record
}

async function upsertIntegrationTokenToSupabase(recordInput, pool = null) {
  const record = normalizeIntegrationTokenRecord(recordInput)
  if (!record) {
    throw new Error('integration token record is invalid.')
  }
  const db = pool || settlementStore.pool
  if (!db) return record

  await db.query(
    `
      INSERT INTO ${CONTROL_PLANE_INTEGRATION_TOKENS_TABLE} (
        token_id,
        app_id,
        account_id,
        environment,
        placement_id,
        token_hash,
        token_type,
        one_time,
        status,
        scope,
        issued_at,
        expires_at,
        used_at,
        revoked_at,
        metadata,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        $10::jsonb, $11::timestamptz, $12::timestamptz,
        $13::timestamptz, $14::timestamptz, $15::jsonb, $16::timestamptz
      )
      ON CONFLICT (token_id) DO UPDATE SET
        app_id = EXCLUDED.app_id,
        account_id = EXCLUDED.account_id,
        environment = EXCLUDED.environment,
        placement_id = EXCLUDED.placement_id,
        token_hash = EXCLUDED.token_hash,
        token_type = EXCLUDED.token_type,
        one_time = EXCLUDED.one_time,
        status = EXCLUDED.status,
        scope = EXCLUDED.scope,
        issued_at = EXCLUDED.issued_at,
        expires_at = EXCLUDED.expires_at,
        used_at = EXCLUDED.used_at,
        revoked_at = EXCLUDED.revoked_at,
        metadata = EXCLUDED.metadata,
        updated_at = EXCLUDED.updated_at
    `,
    [
      record.tokenId,
      record.appId,
      record.accountId,
      record.environment,
      record.placementId,
      record.tokenHash,
      record.tokenType || 'integration_token',
      record.oneTime !== false,
      record.status,
      JSON.stringify(toDbJsonObject(record.scope, createMinimalAgentScope())),
      normalizeDbTimestamp(record.issuedAt, nowIso()),
      normalizeDbTimestamp(record.expiresAt, nowIso()),
      toDbNullableTimestamptz(record.usedAt),
      toDbNullableTimestamptz(record.revokedAt),
      JSON.stringify(toDbJsonObject(record.metadata, {})),
      normalizeDbTimestamp(record.updatedAt, nowIso()),
    ],
  )
  return record
}

async function upsertAgentAccessTokenToSupabase(recordInput, pool = null) {
  const record = normalizeAgentAccessTokenRecord(recordInput)
  if (!record) {
    throw new Error('agent access token record is invalid.')
  }
  const db = pool || settlementStore.pool
  if (!db) return record

  await db.query(
    `
      INSERT INTO ${CONTROL_PLANE_AGENT_ACCESS_TOKENS_TABLE} (
        token_id,
        app_id,
        account_id,
        environment,
        placement_id,
        source_token_id,
        token_hash,
        token_type,
        status,
        scope,
        issued_at,
        expires_at,
        metadata,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        $10::jsonb, $11::timestamptz, $12::timestamptz, $13::jsonb, $14::timestamptz
      )
      ON CONFLICT (token_id) DO UPDATE SET
        app_id = EXCLUDED.app_id,
        account_id = EXCLUDED.account_id,
        environment = EXCLUDED.environment,
        placement_id = EXCLUDED.placement_id,
        source_token_id = EXCLUDED.source_token_id,
        token_hash = EXCLUDED.token_hash,
        token_type = EXCLUDED.token_type,
        status = EXCLUDED.status,
        scope = EXCLUDED.scope,
        issued_at = EXCLUDED.issued_at,
        expires_at = EXCLUDED.expires_at,
        metadata = EXCLUDED.metadata,
        updated_at = EXCLUDED.updated_at
    `,
    [
      record.tokenId,
      record.appId,
      record.accountId,
      record.environment,
      record.placementId,
      record.sourceTokenId,
      record.tokenHash,
      record.tokenType || 'agent_access_token',
      record.status,
      JSON.stringify(toDbJsonObject(record.scope, createMinimalAgentScope())),
      normalizeDbTimestamp(record.issuedAt, nowIso()),
      normalizeDbTimestamp(record.expiresAt, nowIso()),
      JSON.stringify(toDbJsonObject(record.metadata, {})),
      normalizeDbTimestamp(record.updatedAt, nowIso()),
    ],
  )
  return record
}

async function upsertConversionFactToPostgres(fact, pool = null) {
  const db = pool || settlementStore.pool
  if (!db) return null
  const result = await db.query(
    `
      INSERT INTO ${SETTLEMENT_FACT_TABLE} (
        fact_id,
        fact_type,
        app_id,
        account_id,
        request_id,
        session_id,
        turn_id,
        user_id,
        placement_id,
        placement_key,
        ad_id,
        postback_type,
        postback_status,
        conversion_id,
        event_seq,
        occurred_at,
        created_at,
        cpa_usd,
        revenue_usd,
        currency,
        idempotency_key
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
        $12, $13, $14, $15, $16::timestamptz, $17::timestamptz,
        $18, $19, $20, $21
      )
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING *
    `,
    [
      fact.factId,
      fact.factType,
      fact.appId,
      fact.accountId,
      fact.requestId,
      fact.sessionId,
      fact.turnId,
      fact.userId,
      fact.placementId,
      fact.placementKey,
      fact.adId,
      fact.postbackType,
      fact.postbackStatus,
      fact.conversionId,
      fact.eventSeq,
      fact.occurredAt,
      fact.createdAt,
      fact.cpaUsd,
      fact.revenueUsd,
      fact.currency,
      fact.idempotencyKey,
    ],
  )
  if (!Array.isArray(result.rows) || result.rows.length === 0) return null
  return mapPostgresRowToConversionFact(result.rows[0])
}

async function findConversionFactByIdempotencyKeyFromPostgres(idempotencyKey) {
  const db = settlementStore.pool
  if (!db) return null
  const result = await db.query(
    `SELECT * FROM ${SETTLEMENT_FACT_TABLE} WHERE idempotency_key = $1 LIMIT 1`,
    [idempotencyKey],
  )
  if (!Array.isArray(result.rows) || result.rows.length === 0) return null
  return mapPostgresRowToConversionFact(result.rows[0])
}

async function ensureSettlementStoreReady() {
  if (settlementStore.initPromise) {
    await settlementStore.initPromise
    return
  }

  settlementStore.initPromise = (async () => {
    if (!SETTLEMENT_DB_URL) {
      throw new Error('supabase persistence is required, but SUPABASE_DB_URL is missing.')
    }

    try {
      const { Pool } = await import('pg')
      const pool = new Pool({
        connectionString: SETTLEMENT_DB_URL,
        max: DB_POOL_MAX,
        idleTimeoutMillis: DB_POOL_IDLE_TIMEOUT_MS,
        connectionTimeoutMillis: DB_POOL_CONNECTION_TIMEOUT_MS,
        allowExitOnIdle: true,
        ssl: SETTLEMENT_DB_URL.includes('supabase.co')
          ? { rejectUnauthorized: false }
          : undefined,
      })
      await pool.query('SELECT 1')
      await ensureSettlementFactTable(pool)
      await ensureRuntimeLogTables(pool)
      await ensureControlPlaneTables(pool)
      await ensureCampaignBudgetTables(pool)
      await seedDefaultCampaignBudgets(pool)
      const migration = await migrateLegacyPlacementIdsInSupabase(pool)
      if (migration.executed) {
        const rowsByTable = migration.updatedRowsByTable && typeof migration.updatedRowsByTable === 'object'
          ? migration.updatedRowsByTable
          : {}
        for (const target of PLACEMENT_ID_MIGRATION_TABLES) {
          const table = String(target?.table || '').trim()
          const row = rowsByTable[table] && typeof rowsByTable[table] === 'object'
            ? rowsByTable[table]
            : { column: String(target?.column || '').trim(), updatedRows: 0 }
          console.info(
            `[mediation-gateway] placement-id migration ${table}.${row.column}: ${toPositiveInteger(row.updatedRows, 0)} rows updated`,
          )
        }
        recordControlPlaneAudit({
          action: 'placement_id_migration',
          actor: 'system',
          environment: 'prod',
          resourceType: 'gateway_migration',
          resourceId: 'legacy_placement_id',
          metadata: {
            totalUpdatedRows: toPositiveInteger(migration.totalUpdatedRows, 0),
            updatedRowsByTable: rowsByTable,
          },
        })
        persistState(state)
      }
      settlementStore.pool = pool
    } catch (error) {
      throw new Error(
        `supabase persistence init failed: ${error instanceof Error ? error.message : String(error)}`,
      )
    }
  })()

  await settlementStore.initPromise
}

async function listConversionFacts(scopeInput = {}) {
  const scope = normalizeScopeFilters(scopeInput)
  await ensureSettlementStoreReady()

  if (!isPostgresSettlementStore()) {
    const rows = Array.isArray(state?.conversionFacts) ? state.conversionFacts : []
    if (!scopeHasFilters(scope)) return rows
    return filterRowsByScope(rows, scope)
  }

  const clauses = []
  const values = []
  let cursor = 1

  if (scope.accountId) {
    clauses.push(`account_id = $${cursor}`)
    values.push(scope.accountId)
    cursor += 1
  }
  if (scope.appId) {
    clauses.push(`app_id = $${cursor}`)
    values.push(scope.appId)
    cursor += 1
  }

  const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : ''
  const result = await settlementStore.pool.query(
    `SELECT * FROM ${SETTLEMENT_FACT_TABLE} ${whereClause} ORDER BY created_at DESC`,
    values,
  )
  return Array.isArray(result.rows)
    ? result.rows.map((item) => mapPostgresRowToConversionFact(item)).filter(Boolean)
    : []
}

async function writeConversionFact(fact) {
  await ensureSettlementStoreReady()
  if (!isPostgresSettlementStore()) {
    const existingFact = state.conversionFacts.find((item) => String(item?.idempotencyKey || '') === fact.idempotencyKey)
    if (existingFact) {
      return {
        duplicate: true,
        fact: existingFact,
      }
    }
    state.conversionFacts = [fact, ...state.conversionFacts]
    return {
      duplicate: false,
      fact,
    }
  }

  const inserted = await upsertConversionFactToPostgres(fact)
  if (inserted) {
    return {
      duplicate: false,
      fact: inserted,
    }
  }

  const existingFact = await findConversionFactByIdempotencyKeyFromPostgres(fact.idempotencyKey)
  return {
    duplicate: true,
    fact: existingFact || fact,
  }
}

function createInitialState() {
  const placements = buildDefaultPlacementList()
  const placementConfigVersion = Math.max(1, ...placements.map((placement) => placement.configVersion || 1))
  const placementConfigs = []

  return {
    version: 6,
    updatedAt: nowIso(),
    placementConfigVersion,
    placements,
    placementConfigs,
    placementAuditLogs: [],
    controlPlaneAuditLogs: [],
    networkFlowStats: createInitialNetworkFlowStats(),
    networkFlowLogs: [],
    decisionLogs: [],
    eventLogs: [],
    conversionFacts: [],
    globalStats: {
      requests: 0,
      served: 0,
      impressions: 0,
      clicks: 0,
      revenueUsd: 0,
    },
    placementStats: initialPlacementStats(placements),
    dailyMetrics: createDailyMetricsSeed(7),
    controlPlane: createInitialControlPlaneState(),
  }
}

function loadState() {
  return createInitialState()
}

function persistState(state) {
  if (!state || typeof state !== 'object') return
  state.updatedAt = nowIso()
}

let state = loadState()

function syncLegacyPlacementSnapshot() {
  const configs = Array.isArray(state?.placementConfigs) ? state.placementConfigs : []
  const defaultConfig = (
    (DEFAULT_CONTROL_PLANE_APP_ID
      ? configs.find((item) => String(item?.appId || '').trim() === DEFAULT_CONTROL_PLANE_APP_ID)
      : null)
    || configs[0]
  )
  if (defaultConfig && Array.isArray(defaultConfig.placements)) {
    state.placements = defaultConfig.placements.map((item) => normalizePlacement(item))
  } else {
    state.placements = []
  }
  const maxConfigVersion = Math.max(
    1,
    toPositiveInteger(state.placementConfigVersion, 1),
    ...configs.map((item) => toPositiveInteger(item?.placementConfigVersion, 1)),
  )
  state.placementConfigVersion = maxConfigVersion
}

function findPlacementConfigByAppId(appId = '') {
  const normalizedAppId = String(appId || '').trim()
  if (!normalizedAppId) return null
  const rows = Array.isArray(state?.placementConfigs) ? state.placementConfigs : []
  return rows.find((item) => String(item?.appId || '').trim() === normalizedAppId) || null
}

function getPlacementConfigForApp(appId = '', accountId = '', options = {}) {
  const opts = options && typeof options === 'object' ? options : {}
  const createIfMissing = opts.createIfMissing === true
  const normalizedAppId = String(appId || '').trim()
  if (!normalizedAppId) return null
  const providedAccountId = normalizeControlPlaneAccountId(accountId, '')
  const app = resolveControlPlaneAppRecord(normalizedAppId)
  const resolvedAccountId = normalizeControlPlaneAccountId(
    providedAccountId || app?.accountId || app?.organizationId,
    '',
  )

  if (!Array.isArray(state.placementConfigs)) {
    state.placementConfigs = []
  }

  let config = findPlacementConfigByAppId(normalizedAppId)
  if (!config && createIfMissing && resolvedAccountId) {
    config = normalizePlacementConfigRecord({
      appId: normalizedAppId,
      accountId: resolvedAccountId,
      placementConfigVersion: 1,
      placements: buildDefaultPlacementList(),
      updatedAt: nowIso(),
    })
    state.placementConfigs.push(config)
    state.placementConfigVersion = Math.max(
      toPositiveInteger(state.placementConfigVersion, 1),
      toPositiveInteger(config.placementConfigVersion, 1),
    )
    if (normalizedAppId === DEFAULT_CONTROL_PLANE_APP_ID) {
      syncLegacyPlacementSnapshot()
    }
  }
  if (!config) return null
  if (resolvedAccountId) {
    config.accountId = normalizeControlPlaneAccountId(config.accountId || resolvedAccountId, '')
  }
  return config
}

function getPlacementsForApp(appId = '', accountId = '', options = {}) {
  const opts = options && typeof options === 'object' ? options : {}
  const clone = opts.clone === true
  const config = getPlacementConfigForApp(appId, accountId, {
    createIfMissing: opts.createIfMissing === true,
  })
  const rows = config && Array.isArray(config.placements) ? config.placements : []
  return clone ? rows.map((item) => normalizePlacement(item)) : rows
}

function resolvePlacementScopeAppId(scope = {}, fallbackAppId = '') {
  const normalizedScope = normalizeScopeFilters(scope)
  const normalizedAccountId = normalizeControlPlaneAccountId(normalizedScope.accountId, '')
  const requestedAppId = String(normalizedScope.appId || '').trim()
  if (requestedAppId) return requestedAppId

  const fallback = String(fallbackAppId || '').trim()
  if (fallback) {
    if (!normalizedAccountId || appBelongsToAccount(fallback, normalizedAccountId)) {
      return fallback
    }
  }

  if (normalizedAccountId) {
    const latest = findLatestAppForAccount(normalizedAccountId)
    if (latest?.appId) return String(latest.appId).trim()
  }
  return ''
}

function resolvePlacementConfigVersionForScope(scope = {}, fallbackAppId = '') {
  const normalizedScope = normalizeScopeFilters(scope)
  const resolvedAppId = resolvePlacementScopeAppId(normalizedScope, fallbackAppId)
  if (resolvedAppId) {
    const config = getPlacementConfigForApp(resolvedAppId, normalizedScope.accountId, {
      createIfMissing: false,
    })
    if (config) return toPositiveInteger(config.placementConfigVersion, 1)
  }

  if (normalizedScope.accountId) {
    const configs = Array.isArray(state?.placementConfigs) ? state.placementConfigs : []
    const scoped = configs.filter((item) => (
      normalizeControlPlaneAccountId(item?.accountId || resolveAccountIdForApp(item?.appId), '') === normalizedScope.accountId
    ))
    if (scoped.length > 0) {
      return Math.max(1, ...scoped.map((item) => toPositiveInteger(item?.placementConfigVersion, 1)))
    }
  }

  return Math.max(1, toPositiveInteger(state.placementConfigVersion, 1))
}

function getPlacementsForScope(scope = {}, options = {}) {
  const opts = options && typeof options === 'object' ? options : {}
  const normalizedScope = normalizeScopeFilters(scope)
  const resolvedAppId = resolvePlacementScopeAppId(normalizedScope, opts.fallbackAppId || '')
  const rows = getPlacementsForApp(
    resolvedAppId,
    normalizedScope.accountId,
    { createIfMissing: opts.createIfMissing === true, clone: opts.clone === true },
  )
  return {
    appId: resolvedAppId,
    placements: rows,
  }
}

function mergePlacementRowsWithObserved(baseRows = [], observedPlacementIds = [], appId = '') {
  const map = new Map()
  for (const row of Array.isArray(baseRows) ? baseRows : []) {
    const placementId = String(row?.placementId || '').trim()
    if (!placementId || map.has(placementId)) continue
    map.set(placementId, normalizePlacement(row))
  }
  for (const placementId of observedPlacementIds) {
    const normalizedPlacementId = String(placementId || '').trim()
    if (!normalizedPlacementId || map.has(normalizedPlacementId)) continue
    map.set(normalizedPlacementId, normalizePlacement({
      placementId: normalizedPlacementId,
      placementKey: resolvePlacementKeyById(normalizedPlacementId, appId),
    }))
  }
  return Array.from(map.values())
}

syncLegacyPlacementSnapshot()

function applyCorsOrigin(req, res) {
  const requestOrigin = normalizeCorsOrigin(req?.headers?.origin || '')
  if (!requestOrigin) return { ok: true, requestOrigin: '' }

  if (corsOriginState.originSet.has(requestOrigin)) {
    res.setHeader('Access-Control-Allow-Origin', requestOrigin)
    res.setHeader('Vary', 'Origin')
    return { ok: true, requestOrigin }
  }

  return { ok: false, requestOrigin }
}

function withCors(res) {
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization,x-dashboard-actor,x-user-id')
}

function sendJson(res, statusCode, payload) {
  withCors(res)
  res.statusCode = statusCode
  res.setHeader('Content-Type', 'application/json; charset=utf-8')
  res.end(JSON.stringify(payload))
}

function sendNotFound(res) {
  sendJson(res, 404, {
    error: {
      code: 'NOT_FOUND',
      message: 'Endpoint not found.',
    },
  })
}

function sendCorsForbidden(res, origin) {
  sendJson(res, 403, {
    error: {
      code: 'CORS_ORIGIN_FORBIDDEN',
      message: `Origin is not allowed: ${String(origin || '').trim() || '<empty>'}`,
    },
  })
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = ''
    req.on('data', (chunk) => {
      raw += chunk
      if (raw.length > 1024 * 1024) {
        reject(new Error('Payload too large'))
        req.destroy()
      }
    })
    req.on('end', () => {
      if (!raw.trim()) {
        resolve({})
        return
      }
      try {
        resolve(JSON.parse(raw))
      } catch {
        reject(new Error('Invalid JSON payload'))
      }
    })
    req.on('error', reject)
  })
}

function appendDailyMetric({ impressions = 0, clicks = 0 }) {
  state.dailyMetrics = ensureDailyMetricsWindow(state.dailyMetrics)
  const today = getTodayKey()
  const row = state.dailyMetrics.find((item) => item.date === today)
  if (!row) return

  row.impressions += Math.max(0, impressions)
  row.clicks += Math.max(0, clicks)
}

function ensurePlacementStats(placementId) {
  const normalizedPlacementId = normalizePlacementIdWithMigration(
    String(placementId || '').trim(),
    PLACEMENT_ID_FROM_ANSWER,
  )
  if (!normalizedPlacementId) {
    return {
      requests: 0,
      served: 0,
      impressions: 0,
      clicks: 0,
      revenueUsd: 0,
    }
  }
  if (!state.placementStats[normalizedPlacementId]) {
    state.placementStats[normalizedPlacementId] = {
      requests: 0,
      served: 0,
      impressions: 0,
      clicks: 0,
      revenueUsd: 0,
    }
  }
  return state.placementStats[normalizedPlacementId]
}

function readJsonObject(value) {
  if (value && typeof value === 'object') return value
  if (typeof value !== 'string') return {}
  try {
    const parsed = JSON.parse(value)
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

function normalizeRuntimeDecisionLogRecord(payload = {}) {
  const source = payload && typeof payload === 'object' ? payload : {}
  const appId = String(source.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(source.accountId || resolveAccountIdForApp(appId), '')
  const result = DECISION_REASON_ENUM.has(String(source.result || '')) ? String(source.result) : 'error'
  const reason = DECISION_REASON_ENUM.has(String(source.reason || '')) ? String(source.reason) : result
  const intentScore = clampNumber(source.intentScore, 0, 1, 0)

  const placementId = normalizePlacementIdWithMigration(
    String(source.placementId || source.placement_id || '').trim(),
  )
  const placementKey = String(source.placementKey || source.placement_key || '').trim()
    || resolvePlacementKeyById(placementId, appId)

  return {
    ...(source && typeof source === 'object' ? source : {}),
    id: String(source.id || '').trim() || createId('decision'),
    createdAt: normalizeIsoTimestamp(source.createdAt || source.created_at, nowIso()),
    appId,
    accountId,
    requestId: String(source.requestId || source.request_id || '').trim(),
    sessionId: String(source.sessionId || source.session_id || '').trim(),
    turnId: String(source.turnId || source.turn_id || '').trim(),
    event: String(source.event || '').trim(),
    placementId,
    placementKey,
    result,
    reason,
    reasonDetail: String(source.reasonDetail || source.reason_detail || '').trim() || reason,
    intentScore,
  }
}

function normalizeRuntimeEventLogRecord(payload = {}) {
  const source = payload && typeof payload === 'object' ? payload : {}
  const appId = String(source.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(source.accountId || resolveAccountIdForApp(appId), '')
  const placementId = normalizePlacementIdWithMigration(
    String(source.placementId || source.placement_id || '').trim(),
  )
  const placementKey = String(source.placementKey || source.placement_key || '').trim()
    || resolvePlacementKeyById(placementId, appId)

  return {
    ...(source && typeof source === 'object' ? source : {}),
    id: String(source.id || '').trim() || createId('event'),
    createdAt: normalizeIsoTimestamp(source.createdAt || source.created_at, nowIso()),
    appId,
    accountId,
    requestId: String(source.requestId || source.request_id || '').trim(),
    sessionId: String(source.sessionId || source.session_id || '').trim(),
    turnId: String(source.turnId || source.turn_id || '').trim(),
    placementId,
    placementKey,
    eventType: String(source.eventType || source.event_type || '').trim(),
    event: String(source.event || '').trim(),
    kind: String(source.kind || '').trim(),
    result: String(source.result || '').trim(),
    reason: String(source.reason || '').trim(),
    reasonDetail: String(source.reasonDetail || source.reason_detail || '').trim(),
  }
}

function mapPostgresRowToRuntimeDecisionLog(row) {
  const payload = readJsonObject(row?.payload_json)
  return normalizeRuntimeDecisionLogRecord({
    ...payload,
    id: String(row?.id || '').trim(),
    createdAt: row?.created_at,
    requestId: row?.request_id,
    appId: row?.app_id,
    accountId: row?.account_id,
    sessionId: row?.session_id,
    turnId: row?.turn_id,
    event: row?.event,
    placementId: row?.placement_id,
    placementKey: row?.placement_key,
    result: row?.result,
    reason: row?.reason,
  })
}

function mapPostgresRowToRuntimeEventLog(row) {
  const payload = readJsonObject(row?.payload_json)
  return normalizeRuntimeEventLogRecord({
    ...payload,
    id: String(row?.id || '').trim(),
    createdAt: row?.created_at,
    eventType: row?.event_type,
    event: row?.event,
    kind: row?.kind,
    requestId: row?.request_id,
    appId: row?.app_id,
    accountId: row?.account_id,
    sessionId: row?.session_id,
    turnId: row?.turn_id,
    placementId: row?.placement_id,
    placementKey: row?.placement_key,
    result: row?.result,
  })
}

async function upsertDecisionLogToPostgres(log, pool = null) {
  const db = pool || settlementStore.pool
  if (!db) return false
  const result = await db.query(
    `
      INSERT INTO ${RUNTIME_DECISION_LOG_TABLE} (
        id,
        created_at,
        request_id,
        app_id,
        account_id,
        session_id,
        turn_id,
        event,
        placement_id,
        placement_key,
        result,
        reason,
        payload_json
      )
      VALUES ($1, $2::timestamptz, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)
      ON CONFLICT (id) DO NOTHING
      RETURNING id
    `,
    [
      String(log?.id || '').trim(),
      String(log?.createdAt || nowIso()),
      String(log?.requestId || '').trim(),
      String(log?.appId || '').trim(),
      normalizeControlPlaneAccountId(log?.accountId || resolveAccountIdForApp(log?.appId), ''),
      String(log?.sessionId || '').trim(),
      String(log?.turnId || '').trim(),
      String(log?.event || '').trim(),
      String(log?.placementId || '').trim(),
      String(log?.placementKey || '').trim(),
      String(log?.result || '').trim(),
      String(log?.reason || '').trim(),
      JSON.stringify(log || {}),
    ],
  )
  return Array.isArray(result.rows) && result.rows.length > 0
}

async function upsertEventLogToPostgres(log, pool = null) {
  const db = pool || settlementStore.pool
  if (!db) return false
  const result = await db.query(
    `
      INSERT INTO ${RUNTIME_EVENT_LOG_TABLE} (
        id,
        created_at,
        event_type,
        event,
        kind,
        request_id,
        app_id,
        account_id,
        session_id,
        turn_id,
        placement_id,
        placement_key,
        result,
        payload_json
      )
      VALUES ($1, $2::timestamptz, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)
      ON CONFLICT (id) DO NOTHING
      RETURNING id
    `,
    [
      String(log?.id || '').trim(),
      String(log?.createdAt || nowIso()),
      String(log?.eventType || '').trim(),
      String(log?.event || '').trim(),
      String(log?.kind || '').trim(),
      String(log?.requestId || '').trim(),
      String(log?.appId || '').trim(),
      normalizeControlPlaneAccountId(log?.accountId || resolveAccountIdForApp(log?.appId), ''),
      String(log?.sessionId || '').trim(),
      String(log?.turnId || '').trim(),
      String(log?.placementId || '').trim(),
      String(log?.placementKey || '').trim(),
      String(log?.result || '').trim(),
      JSON.stringify(log || {}),
    ],
  )
  return Array.isArray(result.rows) && result.rows.length > 0
}

async function listDecisionLogs(scopeInput = {}) {
  const scope = normalizeScopeFilters(scopeInput)
  await ensureSettlementStoreReady()

  if (!isPostgresSettlementStore()) {
    const rows = Array.isArray(state?.decisionLogs) ? state.decisionLogs : []
    return scopeHasFilters(scope) ? filterRowsByScope(rows, scope) : rows
  }

  const clauses = []
  const values = []
  let cursor = 1
  if (scope.accountId) {
    clauses.push(`account_id = $${cursor}`)
    values.push(scope.accountId)
    cursor += 1
  }
  if (scope.appId) {
    clauses.push(`app_id = $${cursor}`)
    values.push(scope.appId)
    cursor += 1
  }
  const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : ''
  const result = await settlementStore.pool.query(
    `SELECT * FROM ${RUNTIME_DECISION_LOG_TABLE} ${whereClause} ORDER BY created_at DESC`,
    values,
  )
  return Array.isArray(result.rows)
    ? result.rows.map((row) => mapPostgresRowToRuntimeDecisionLog(row)).filter(Boolean)
    : []
}

async function listEventLogs(scopeInput = {}) {
  const scope = normalizeScopeFilters(scopeInput)
  await ensureSettlementStoreReady()

  if (!isPostgresSettlementStore()) {
    const rows = Array.isArray(state?.eventLogs) ? state.eventLogs : []
    return scopeHasFilters(scope) ? filterRowsByScope(rows, scope) : rows
  }

  const clauses = []
  const values = []
  let cursor = 1
  if (scope.accountId) {
    clauses.push(`account_id = $${cursor}`)
    values.push(scope.accountId)
    cursor += 1
  }
  if (scope.appId) {
    clauses.push(`app_id = $${cursor}`)
    values.push(scope.appId)
    cursor += 1
  }
  const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : ''
  const result = await settlementStore.pool.query(
    `SELECT * FROM ${RUNTIME_EVENT_LOG_TABLE} ${whereClause} ORDER BY created_at DESC`,
    values,
  )
  return Array.isArray(result.rows)
    ? result.rows.map((row) => mapPostgresRowToRuntimeEventLog(row)).filter(Boolean)
    : []
}

async function recordDecision(payload) {
  const record = normalizeRuntimeDecisionLogRecord(payload)
  state.decisionLogs = applyCollectionLimit([
    record,
    ...state.decisionLogs,
  ], MAX_DECISION_LOGS)

  if (!isPostgresSettlementStore()) return record
  try {
    await upsertDecisionLogToPostgres(record)
  } catch (error) {
    if (REQUIRE_RUNTIME_LOG_DB_PERSISTENCE) {
      throw new Error(
        `decision log persistence failed: ${error instanceof Error ? error.message : String(error)}`,
      )
    }
    console.error(
      '[mediation-gateway] decision log persistence failed (fallback state only):',
      error instanceof Error ? error.message : String(error),
    )
  }
  return record
}

async function recordEvent(payload) {
  const record = normalizeRuntimeEventLogRecord(payload)
  state.eventLogs = applyCollectionLimit([
    record,
    ...state.eventLogs,
  ], MAX_EVENT_LOGS)

  if (!isPostgresSettlementStore()) return record
  try {
    await upsertEventLogToPostgres(record)
  } catch (error) {
    if (REQUIRE_RUNTIME_LOG_DB_PERSISTENCE) {
      throw new Error(
        `event log persistence failed: ${error instanceof Error ? error.message : String(error)}`,
      )
    }
    console.error(
      '[mediation-gateway] event log persistence failed (fallback state only):',
      error instanceof Error ? error.message : String(error),
    )
  }
  return record
}

async function findPlacementIdByRequestId(requestId) {
  const targetRequestId = String(requestId || '').trim()
  if (!targetRequestId) return ''
  for (const row of state.decisionLogs) {
    if (String(row?.requestId || '').trim() !== targetRequestId) continue
    return normalizePlacementIdWithMigration(String(row?.placementId || '').trim())
  }
  if (!isPostgresSettlementStore()) return ''
  try {
    const result = await settlementStore.pool.query(
      `
        SELECT placement_id
        FROM ${RUNTIME_DECISION_LOG_TABLE}
        WHERE request_id = $1
          AND placement_id IS NOT NULL
          AND placement_id <> ''
        ORDER BY created_at DESC
        LIMIT 1
      `,
      [targetRequestId],
    )
    const row = Array.isArray(result.rows) ? result.rows[0] : null
    return normalizePlacementIdWithMigration(String(row?.placement_id || '').trim())
  } catch (error) {
    console.error(
      '[mediation-gateway] failed to resolve placementId by requestId from runtime decision logs:',
      error instanceof Error ? error.message : String(error),
    )
  }
  return ''
}

async function findRuntimeDecisionByRequestId(requestId = '') {
  const targetRequestId = String(requestId || '').trim()
  if (!targetRequestId) return null

  for (const row of state.decisionLogs) {
    if (String(row?.requestId || '').trim() !== targetRequestId) continue
    return row
  }

  if (!isPostgresSettlementStore()) return null
  try {
    const result = await settlementStore.pool.query(
      `
        SELECT *
        FROM ${RUNTIME_DECISION_LOG_TABLE}
        WHERE request_id = $1
        ORDER BY created_at DESC
        LIMIT 1
      `,
      [targetRequestId],
    )
    const row = Array.isArray(result.rows) ? result.rows[0] : null
    return row ? mapPostgresRowToRuntimeDecisionLog(row) : null
  } catch (error) {
    console.error(
      '[mediation-gateway] failed to resolve runtime decision by requestId:',
      error instanceof Error ? error.message : String(error),
    )
  }
  return null
}

function normalizeDecisionAdSnapshotByRequest(decisionRecord = {}, adId = '') {
  const decision = decisionRecord && typeof decisionRecord === 'object' ? decisionRecord : {}
  const targetAdId = String(adId || '').trim()
  const ads = Array.isArray(decision.ads) ? decision.ads : []
  const normalizedAds = ads
    .map((item) => {
      const ad = item && typeof item === 'object' ? item : null
      if (!ad) return null
      return {
        adId: String(ad.adId || '').trim(),
        targetUrl: String(ad.targetUrl || '').trim(),
      }
    })
    .filter((item) => item && (item.adId || item.targetUrl))

  const exact = targetAdId
    ? normalizedAds.find((item) => item.adId === targetAdId)
    : null
  const fallback = normalizedAds[0] || null

  return {
    matched: exact || null,
    fallback,
    found: Boolean(exact || fallback),
  }
}

function resolvePlacementKeyById(placementId, appId = '') {
  const normalizedPlacementId = normalizePlacementIdWithMigration(String(placementId || '').trim())
  if (!normalizedPlacementId) return ''
  const placements = getPlacementsForApp(appId, '', { createIfMissing: false })
  const placement = placements.find((item) => item.placementId === normalizedPlacementId)
    || state.placements.find((item) => item.placementId === normalizedPlacementId)
  if (placement) {
    return String(placement.placementKey || '').trim()
  }
  return String(PLACEMENT_KEY_BY_ID[normalizedPlacementId] || '').trim()
}

function normalizeBidPricingSnapshot(raw = {}) {
  if (!raw || typeof raw !== 'object') return null
  const modelVersion = String(raw.modelVersion || '').trim()
  const pricingSemanticsVersion = String(raw.pricingSemanticsVersion || '').trim()
  const billingUnit = String(raw.billingUnit || '').trim().toLowerCase()
  const triggerType = String(raw.triggerType || '').trim()
  const targetRpmUsd = clampNumber(raw.targetRpmUsd, 0, Number.MAX_SAFE_INTEGER, NaN)
  const ecpmUsd = clampNumber(raw.ecpmUsd, 0, Number.MAX_SAFE_INTEGER, NaN)
  const cpcUsd = clampNumber(raw.cpcUsd, 0, Number.MAX_SAFE_INTEGER, NaN)
  const cpaUsd = clampNumber(raw.cpaUsd, 0, Number.MAX_SAFE_INTEGER, NaN)
  const pClick = clampNumber(raw.pClick, 0, 1, NaN)
  const pConv = clampNumber(raw.pConv, 0, 1, NaN)
  const network = String(raw.network || '').trim().toLowerCase()
  const rawSignal = raw.rawSignal && typeof raw.rawSignal === 'object'
    ? {
        rawBidValue: clampNumber(raw.rawSignal.rawBidValue, 0, Number.MAX_SAFE_INTEGER, 0),
        rawUnit: String(raw.rawSignal.rawUnit || '').trim(),
        normalizedFactor: clampNumber(raw.rawSignal.normalizedFactor, 0, Number.MAX_SAFE_INTEGER, 1),
      }
    : null

  if (!modelVersion && !Number.isFinite(cpaUsd) && !Number.isFinite(ecpmUsd) && !Number.isFinite(cpcUsd)) return null
  return {
    modelVersion,
    pricingSemanticsVersion,
    billingUnit: billingUnit === 'cpc' ? 'cpc' : '',
    triggerType,
    targetRpmUsd: Number.isFinite(targetRpmUsd) ? round(targetRpmUsd, 4) : 0,
    ecpmUsd: Number.isFinite(ecpmUsd) ? round(ecpmUsd, 4) : 0,
    cpcUsd: Number.isFinite(cpcUsd) ? round(cpcUsd, 4) : 0,
    cpaUsd: Number.isFinite(cpaUsd) ? round(cpaUsd, 4) : 0,
    pClick: Number.isFinite(pClick) ? round(pClick, 6) : 0,
    pConv: Number.isFinite(pConv) ? round(pConv, 6) : 0,
    network,
    rawSignal,
  }
}

function findPricingSnapshotByRequestId(requestId = '') {
  const targetRequestId = String(requestId || '').trim()
  if (!targetRequestId) return null

  for (const row of state.decisionLogs) {
    if (String(row?.requestId || '').trim() !== targetRequestId) continue
    const runtime = row?.runtime && typeof row.runtime === 'object' ? row.runtime : {}
    const winnerPricing = normalizeBidPricingSnapshot(runtime?.winnerBid?.pricing)
    if (winnerPricing) return winnerPricing
  }

  const deliveryRows = Array.isArray(state.deliveryRecords) ? state.deliveryRecords : []
  for (const row of deliveryRows) {
    if (String(row?.requestId || row?.responseReference || '').trim() !== targetRequestId) continue
    const payload = row?.payload && typeof row.payload === 'object' ? row.payload : {}
    const winnerPricing = normalizeBidPricingSnapshot(payload?.winnerBid?.pricing || payload?.pricingSnapshot)
    if (winnerPricing) return winnerPricing
  }

  return null
}

function normalizeCampaignId(value = '') {
  return String(value || '').trim()
}

function resolveCampaignIdFromMetadata(metadata = {}) {
  const source = metadata && typeof metadata === 'object' ? metadata : {}
  return normalizeCampaignId(
    source.campaignId
    || source.campaign_id
    || source.programId
    || source.program_id
    || source.advertiserId
    || source.advertiser_id,
  )
}

function resolveCampaignIdFromCandidate(candidate = {}) {
  return resolveCampaignIdFromMetadata(candidate?.metadata)
}

function resolveCampaignIdFromBid(bid = {}) {
  const source = bid && typeof bid === 'object' ? bid : {}
  const direct = normalizeCampaignId(source.campaignId || source.campaign_id)
  if (direct) return direct
  return resolveCampaignIdFromMetadata(source.metadata)
}

function isBudgetEnforced() {
  return BUDGET_ENFORCEMENT_MODE === 'on'
}

function isBudgetMonitorOnly() {
  return BUDGET_ENFORCEMENT_MODE === 'monitor_only'
}

function isRiskEnforced() {
  return RISK_ENFORCEMENT_MODE === 'on'
}

function isRiskMonitorOnly() {
  return RISK_ENFORCEMENT_MODE === 'monitor_only'
}

function trimRecentTimestamps(rows = [], windowMs = 0, nowMs = Date.now()) {
  const list = Array.isArray(rows) ? rows : []
  const minTs = nowMs - Math.max(0, Math.floor(windowMs))
  return list.filter((ts) => Number.isFinite(ts) && ts >= minTs)
}

function getRiskConfigSnapshot() {
  const config = runtimeMemory?.risk?.config && typeof runtimeMemory.risk.config === 'object'
    ? runtimeMemory.risk.config
    : {}
  return {
    clickBurstWindowSec: toPositiveInteger(config.clickBurstWindowSec, DEFAULT_RISK_RULES.clickBurstWindowSec),
    clickBurstLimit: toPositiveInteger(config.clickBurstLimit, DEFAULT_RISK_RULES.clickBurstLimit),
    duplicateClickWindowSec: toPositiveInteger(config.duplicateClickWindowSec, DEFAULT_RISK_RULES.duplicateClickWindowSec),
    ctrWarnThreshold: clampNumber(config.ctrWarnThreshold, 0, 1, DEFAULT_RISK_RULES.ctrWarnThreshold),
    ctrBlockThreshold: clampNumber(config.ctrBlockThreshold, 0, 1, DEFAULT_RISK_RULES.ctrBlockThreshold),
    ctrMinImpressions: toPositiveInteger(config.ctrMinImpressions, DEFAULT_RISK_RULES.ctrMinImpressions),
    degradeMultiplier: clampNumber(config.degradeMultiplier, 0.1, 1, DEFAULT_RISK_RULES.degradeMultiplier),
  }
}

function normalizeRiskConfigPayload(payload = {}) {
  const input = payload && typeof payload === 'object' ? payload : {}
  const existing = getRiskConfigSnapshot()
  return {
    clickBurstWindowSec: toPositiveInteger(input.clickBurstWindowSec, existing.clickBurstWindowSec),
    clickBurstLimit: toPositiveInteger(input.clickBurstLimit, existing.clickBurstLimit),
    duplicateClickWindowSec: toPositiveInteger(input.duplicateClickWindowSec, existing.duplicateClickWindowSec),
    ctrWarnThreshold: clampNumber(input.ctrWarnThreshold, 0, 1, existing.ctrWarnThreshold),
    ctrBlockThreshold: clampNumber(input.ctrBlockThreshold, 0, 1, existing.ctrBlockThreshold),
    ctrMinImpressions: toPositiveInteger(input.ctrMinImpressions, existing.ctrMinImpressions),
    degradeMultiplier: clampNumber(input.degradeMultiplier, 0.1, 1, existing.degradeMultiplier),
  }
}

function updateRiskConfig(payload = {}) {
  const next = normalizeRiskConfigPayload(payload)
  runtimeMemory.risk.config = { ...next }
  return getRiskConfigSnapshot()
}

function getCampaignPerfState(campaignId = '') {
  const normalizedCampaignId = normalizeCampaignId(campaignId)
  if (!normalizedCampaignId) return null
  if (!(runtimeMemory?.risk?.campaignPerfById instanceof Map)) {
    runtimeMemory.risk.campaignPerfById = new Map()
  }
  const map = runtimeMemory.risk.campaignPerfById
  if (!map.has(normalizedCampaignId)) {
    map.set(normalizedCampaignId, {
      impressionTs: [],
      clickTs: [],
    })
  }
  return map.get(normalizedCampaignId)
}

function getCampaignCtrSnapshot(campaignId = '', nowMs = Date.now()) {
  const perf = getCampaignPerfState(campaignId)
  if (!perf) {
    return {
      impressions: 0,
      clicks: 0,
      ctr: 0,
    }
  }
  const windowMs = 15 * 60 * 1000
  perf.impressionTs = trimRecentTimestamps(perf.impressionTs, windowMs, nowMs)
  perf.clickTs = trimRecentTimestamps(perf.clickTs, windowMs, nowMs)
  const impressions = perf.impressionTs.length
  const clicks = perf.clickTs.length
  return {
    impressions,
    clicks,
    ctr: impressions > 0 ? clicks / impressions : 0,
  }
}

function recordCampaignImpressionForRisk(campaignId = '', nowMs = Date.now()) {
  const perf = getCampaignPerfState(campaignId)
  if (!perf) return
  perf.impressionTs.push(nowMs)
  perf.impressionTs = trimRecentTimestamps(perf.impressionTs, 15 * 60 * 1000, nowMs)
}

function recordCampaignClickForRisk(campaignId = '', nowMs = Date.now()) {
  const perf = getCampaignPerfState(campaignId)
  if (!perf) return
  perf.clickTs.push(nowMs)
  perf.clickTs = trimRecentTimestamps(perf.clickTs, 15 * 60 * 1000, nowMs)
}

function buildRiskActorKey(input = {}) {
  const accountId = normalizeControlPlaneAccountId(input.accountId || '', '')
  const appId = String(input.appId || '').trim()
  const userId = String(input.userId || '').trim()
  const sessionId = String(input.sessionId || '').trim()
  return [accountId, appId, userId, sessionId].join('|')
}

function evaluateBidRisk(input = {}) {
  const campaignId = normalizeCampaignId(input.campaignId)
  const config = getRiskConfigSnapshot()
  if (!campaignId) {
    return {
      decision: 'allow',
      reasonCode: 'risk_campaign_missing',
      multiplier: 1,
      enforced: isRiskEnforced(),
      mode: RISK_ENFORCEMENT_MODE,
    }
  }
  const ctrSnapshot = getCampaignCtrSnapshot(campaignId, Date.now())
  if (ctrSnapshot.impressions >= config.ctrMinImpressions && ctrSnapshot.ctr >= config.ctrBlockThreshold) {
    return {
      decision: 'block',
      reasonCode: 'risk_ctr_spike_block',
      multiplier: 1,
      ctrSnapshot,
      enforced: isRiskEnforced(),
      mode: RISK_ENFORCEMENT_MODE,
    }
  }
  if (ctrSnapshot.impressions >= config.ctrMinImpressions && ctrSnapshot.ctr >= config.ctrWarnThreshold) {
    return {
      decision: 'degrade',
      reasonCode: 'risk_ctr_spike_degrade',
      multiplier: config.degradeMultiplier,
      ctrSnapshot,
      enforced: isRiskEnforced(),
      mode: RISK_ENFORCEMENT_MODE,
    }
  }
  return {
    decision: 'allow',
    reasonCode: 'risk_allow',
    multiplier: 1,
    ctrSnapshot,
    enforced: isRiskEnforced(),
    mode: RISK_ENFORCEMENT_MODE,
  }
}

function evaluateClickRisk(input = {}) {
  const nowMs = Date.now()
  const config = getRiskConfigSnapshot()
  const requestId = String(input.requestId || '').trim()
  const adId = String(input.adId || '').trim()
  const actorKey = buildRiskActorKey(input)
  const requestAdKey = requestId && adId ? `${requestId}|${adId}` : ''
  const campaignId = normalizeCampaignId(input.campaignId)

  if (!(runtimeMemory?.risk?.clickSeenByRequestAd instanceof Map)) {
    runtimeMemory.risk.clickSeenByRequestAd = new Map()
  }
  if (requestAdKey) {
    const lastSeenMs = toPositiveInteger(runtimeMemory.risk.clickSeenByRequestAd.get(requestAdKey), 0)
    if (lastSeenMs > 0 && (nowMs - lastSeenMs) <= (config.duplicateClickWindowSec * 1000)) {
      return {
        decision: 'block',
        reasonCode: 'risk_duplicate_click',
        multiplier: 1,
        mode: RISK_ENFORCEMENT_MODE,
        enforced: isRiskEnforced(),
      }
    }
    runtimeMemory.risk.clickSeenByRequestAd.set(requestAdKey, nowMs)
  }

  if (!(runtimeMemory?.risk?.clickBurstByActor instanceof Map)) {
    runtimeMemory.risk.clickBurstByActor = new Map()
  }
  const burstRows = runtimeMemory.risk.clickBurstByActor.get(actorKey) || []
  const nextRows = [...trimRecentTimestamps(burstRows, config.clickBurstWindowSec * 1000, nowMs), nowMs]
  runtimeMemory.risk.clickBurstByActor.set(actorKey, nextRows)
  if (nextRows.length > config.clickBurstLimit) {
    return {
      decision: 'block',
      reasonCode: 'risk_click_burst',
      multiplier: 1,
      burstCount: nextRows.length,
      mode: RISK_ENFORCEMENT_MODE,
      enforced: isRiskEnforced(),
    }
  }

  const ctrSnapshot = getCampaignCtrSnapshot(campaignId, nowMs)
  if (campaignId && ctrSnapshot.impressions >= config.ctrMinImpressions && ctrSnapshot.ctr >= config.ctrBlockThreshold) {
    return {
      decision: 'block',
      reasonCode: 'risk_ctr_spike_block',
      multiplier: 1,
      ctrSnapshot,
      mode: RISK_ENFORCEMENT_MODE,
      enforced: isRiskEnforced(),
    }
  }
  if (campaignId && ctrSnapshot.impressions >= config.ctrMinImpressions && ctrSnapshot.ctr >= config.ctrWarnThreshold) {
    return {
      decision: 'degrade',
      reasonCode: 'risk_ctr_spike_degrade',
      multiplier: config.degradeMultiplier,
      ctrSnapshot,
      mode: RISK_ENFORCEMENT_MODE,
      enforced: isRiskEnforced(),
    }
  }

  return {
    decision: 'allow',
    reasonCode: 'risk_allow',
    multiplier: 1,
    mode: RISK_ENFORCEMENT_MODE,
    enforced: isRiskEnforced(),
  }
}

async function cleanupExpiredBudgetReservations(options = {}) {
  const db = options.pool || settlementStore.pool
  if (!db) return 0
  const nowAt = normalizeIsoTimestamp(options.nowAt, nowIso())
  const result = await db.query(
    `
      WITH expired AS (
        UPDATE ${BUDGET_RESERVATIONS_TABLE}
        SET
          status = 'released',
          reason_code = CASE WHEN reason_code = '' THEN 'expired' ELSE reason_code END,
          updated_at = $1::timestamptz
        WHERE status = 'reserved'
          AND expires_at <= $1::timestamptz
        RETURNING reservation_id, campaign_id, request_id, reserved_cpc_usd
      )
      INSERT INTO ${BUDGET_LEDGER_TABLE} (
        ledger_id,
        campaign_id,
        reservation_id,
        request_id,
        entry_type,
        amount_usd,
        currency,
        metadata,
        created_at
      )
      SELECT
        'bled_' || md5(expired.reservation_id || '|release|' || $1::text),
        expired.campaign_id,
        expired.reservation_id,
        expired.request_id,
        'release',
        expired.reserved_cpc_usd,
        'USD',
        '{"reason":"expired"}'::jsonb,
        $1::timestamptz
      FROM expired
      ON CONFLICT (ledger_id) DO NOTHING
    `,
    [nowAt],
  )
  return toPositiveInteger(result.rowCount, 0)
}

async function getCampaignBudgetSnapshot(campaignId = '', options = {}) {
  const normalizedCampaignId = normalizeCampaignId(campaignId)
  if (!normalizedCampaignId) {
    return {
      configured: false,
      reasonCode: 'budget_unconfigured',
      campaignId: '',
    }
  }
  const db = options.pool || settlementStore.pool
  if (!db) {
    return {
      configured: false,
      reasonCode: 'budget_store_unavailable',
      campaignId: normalizedCampaignId,
    }
  }

  await cleanupExpiredBudgetReservations({ pool: db })
  const result = await db.query(
    `
      SELECT
        c.campaign_id,
        c.account_id,
        c.app_id,
        c.status,
        l.daily_budget_usd,
        l.lifetime_budget_usd,
        l.currency,
        l.timezone,
        COALESCE((
          SELECT SUM(entry.amount_usd)
          FROM ${BUDGET_LEDGER_TABLE} entry
          WHERE entry.campaign_id = c.campaign_id
            AND entry.entry_type = 'settle'
        ), 0) AS spent_lifetime_usd,
        COALESCE((
          SELECT SUM(entry.amount_usd)
          FROM ${BUDGET_LEDGER_TABLE} entry
          WHERE entry.campaign_id = c.campaign_id
            AND entry.entry_type = 'settle'
            AND entry.created_at >= date_trunc('day', NOW())
        ), 0) AS spent_daily_usd,
        COALESCE((
          SELECT SUM(reserved.reserved_cpc_usd)
          FROM ${BUDGET_RESERVATIONS_TABLE} reserved
          WHERE reserved.campaign_id = c.campaign_id
            AND reserved.status = 'reserved'
            AND reserved.expires_at > NOW()
        ), 0) AS reserved_open_usd
      FROM ${CAMPAIGNS_TABLE} c
      LEFT JOIN ${CAMPAIGN_BUDGET_LIMITS_TABLE} l
        ON l.campaign_id = c.campaign_id
      WHERE c.campaign_id = $1
      LIMIT 1
    `,
    [normalizedCampaignId],
  )
  const row = Array.isArray(result.rows) ? result.rows[0] : null
  if (!row) {
    return {
      configured: false,
      reasonCode: 'budget_unconfigured',
      campaignId: normalizedCampaignId,
    }
  }
  const lifetimeBudgetUsd = clampNumber(row.lifetime_budget_usd, 0, Number.MAX_SAFE_INTEGER, 0)
  if (!(lifetimeBudgetUsd > 0)) {
    return {
      configured: false,
      reasonCode: 'budget_unconfigured',
      campaignId: normalizedCampaignId,
    }
  }
  const dailyBudgetUsd = clampNumber(row.daily_budget_usd, 0, Number.MAX_SAFE_INTEGER, NaN)
  const spentLifetimeUsd = clampNumber(row.spent_lifetime_usd, 0, Number.MAX_SAFE_INTEGER, 0)
  const spentDailyUsd = clampNumber(row.spent_daily_usd, 0, Number.MAX_SAFE_INTEGER, 0)
  const reservedOpenUsd = clampNumber(row.reserved_open_usd, 0, Number.MAX_SAFE_INTEGER, 0)
  const remainingLifetimeUsd = Math.max(0, lifetimeBudgetUsd - spentLifetimeUsd - reservedOpenUsd)
  const remainingDailyUsd = Number.isFinite(dailyBudgetUsd)
    ? Math.max(0, dailyBudgetUsd - spentDailyUsd - reservedOpenUsd)
    : Number.MAX_SAFE_INTEGER
  const remainingUsd = Math.max(0, Math.min(remainingLifetimeUsd, remainingDailyUsd))
  const active = String(row.status || '').trim().toLowerCase() === 'active'

  return {
    configured: true,
    active,
    campaignId: normalizedCampaignId,
    accountId: String(row.account_id || '').trim(),
    appId: String(row.app_id || '').trim(),
    currency: String(row.currency || 'USD').trim() || 'USD',
    timezone: String(row.timezone || 'UTC').trim() || 'UTC',
    dailyBudgetUsd: Number.isFinite(dailyBudgetUsd) ? round(dailyBudgetUsd, 4) : null,
    lifetimeBudgetUsd: round(lifetimeBudgetUsd, 4),
    spentDailyUsd: round(spentDailyUsd, 4),
    spentLifetimeUsd: round(spentLifetimeUsd, 4),
    reservedOpenUsd: round(reservedOpenUsd, 4),
    remainingUsd: round(remainingUsd, 4),
    reasonCode: active ? (remainingUsd > 0 ? 'budget_ok' : 'budget_exhausted') : 'budget_campaign_inactive',
  }
}

async function tryReserveCampaignBudget(input = {}) {
  const campaignId = normalizeCampaignId(input.campaignId)
  const requestId = String(input.requestId || '').trim()
  const adId = String(input.adId || '').trim()
  const accountId = normalizeControlPlaneAccountId(input.accountId || '', '')
  const appId = String(input.appId || '').trim()
  const reserveUsd = round(clampNumber(input.reserveUsd, 0, Number.MAX_SAFE_INTEGER, 0), 4)
  if (!campaignId) {
    return {
      allowed: false,
      reserved: false,
      reasonCode: 'budget_unconfigured',
      reservation: null,
      budgetSnapshot: null,
    }
  }
  if (!(reserveUsd > 0)) {
    return {
      allowed: false,
      reserved: false,
      reasonCode: 'budget_invalid_amount',
      reservation: null,
      budgetSnapshot: null,
    }
  }
  const db = input.pool || settlementStore.pool
  if (!db) {
    return {
      allowed: false,
      reserved: false,
      reasonCode: 'budget_store_unavailable',
      reservation: null,
      budgetSnapshot: null,
    }
  }

  const client = typeof db.connect === 'function' ? await db.connect() : null
  const runner = client || db
  const nowAt = nowIso()
  const expiresAt = new Date(Date.now() + BUDGET_RESERVATION_TTL_MS).toISOString()
  const reservationId = createId('bres')
  const ledgerId = createId('bled')

  try {
    await runner.query('BEGIN')
    await cleanupExpiredBudgetReservations({ pool: runner, nowAt })

    const existingReservationResult = await runner.query(
      `
        SELECT *
        FROM ${BUDGET_RESERVATIONS_TABLE}
        WHERE request_id = $1
          AND campaign_id = $2
          AND ad_id = $3
        ORDER BY created_at DESC
        LIMIT 1
      `,
      [requestId, campaignId, adId],
    )
    const existingReservation = Array.isArray(existingReservationResult.rows)
      ? existingReservationResult.rows[0]
      : null
    if (
      existingReservation
      && String(existingReservation.status || '') === 'reserved'
      && Date.parse(String(existingReservation.expires_at || '')) > Date.now()
    ) {
      await runner.query('COMMIT')
      return {
        allowed: true,
        reserved: true,
        reasonCode: 'budget_already_reserved',
        reservation: {
          reservationId: String(existingReservation.reservation_id || '').trim(),
          campaignId,
          requestId,
          adId,
          reservedCpcUsd: round(clampNumber(existingReservation.reserved_cpc_usd, 0, Number.MAX_SAFE_INTEGER, 0), 4),
          expiresAt: normalizeIsoTimestamp(existingReservation.expires_at, expiresAt),
        },
        budgetSnapshot: await getCampaignBudgetSnapshot(campaignId, { pool: runner }),
      }
    }

    const snapshot = await getCampaignBudgetSnapshot(campaignId, { pool: runner })
    if (!snapshot.configured) {
      await runner.query('ROLLBACK')
      return {
        allowed: false,
        reserved: false,
        reasonCode: snapshot.reasonCode || 'budget_unconfigured',
        reservation: null,
        budgetSnapshot: snapshot,
      }
    }
    if (!snapshot.active) {
      await runner.query('ROLLBACK')
      return {
        allowed: false,
        reserved: false,
        reasonCode: 'budget_campaign_inactive',
        reservation: null,
        budgetSnapshot: snapshot,
      }
    }
    if (snapshot.remainingUsd < reserveUsd) {
      await runner.query('ROLLBACK')
      return {
        allowed: false,
        reserved: false,
        reasonCode: 'budget_exhausted',
        reservation: null,
        budgetSnapshot: snapshot,
      }
    }

    await runner.query(
      `
        INSERT INTO ${BUDGET_RESERVATIONS_TABLE} (
          reservation_id,
          campaign_id,
          account_id,
          app_id,
          request_id,
          ad_id,
          reserved_cpc_usd,
          pricing_semantics_version,
          status,
          reason_code,
          expires_at,
          settled_fact_id,
          created_at,
          updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'cpc_v1', 'reserved', '', $8::timestamptz, '', $9::timestamptz, $9::timestamptz)
      `,
      [
        reservationId,
        campaignId,
        accountId,
        appId,
        requestId,
        adId,
        reserveUsd,
        expiresAt,
        nowAt,
      ],
    )

    await runner.query(
      `
        INSERT INTO ${BUDGET_LEDGER_TABLE} (
          ledger_id,
          campaign_id,
          reservation_id,
          request_id,
          fact_id,
          entry_type,
          amount_usd,
          currency,
          metadata,
          created_at
        )
        VALUES ($1, $2, $3, $4, '', 'reserve', $5, 'USD', $6::jsonb, $7::timestamptz)
      `,
      [
        ledgerId,
        campaignId,
        reservationId,
        requestId,
        reserveUsd,
        JSON.stringify({
          adId,
          appId,
          accountId,
          source: String(input.source || 'v2_bid').trim() || 'v2_bid',
        }),
        nowAt,
      ],
    )
    await runner.query('COMMIT')

    return {
      allowed: true,
      reserved: true,
      reasonCode: 'budget_reserved',
      reservation: {
        reservationId,
        campaignId,
        requestId,
        adId,
        reservedCpcUsd: reserveUsd,
        expiresAt,
      },
      budgetSnapshot: await getCampaignBudgetSnapshot(campaignId, { pool: db }),
    }
  } catch (error) {
    await runner.query('ROLLBACK').catch(() => {})
    return {
      allowed: false,
      reserved: false,
      reasonCode: 'budget_reservation_error',
      reservation: null,
      budgetSnapshot: null,
      error: error instanceof Error ? error.message : 'budget_reservation_error',
    }
  } finally {
    if (client) client.release()
  }
}

async function settleCampaignBudgetReservation(input = {}) {
  const campaignId = normalizeCampaignId(input.campaignId)
  const requestId = String(input.requestId || '').trim()
  const adId = String(input.adId || '').trim()
  const factId = String(input.factId || '').trim()
  const amountUsd = round(clampNumber(input.amountUsd, 0, Number.MAX_SAFE_INTEGER, 0), 4)
  if (!campaignId || !requestId || !factId || !(amountUsd > 0)) {
    return {
      settled: false,
      reasonCode: 'budget_settlement_invalid',
    }
  }

  const db = input.pool || settlementStore.pool
  if (!db) {
    return {
      settled: false,
      reasonCode: 'budget_store_unavailable',
    }
  }

  const client = typeof db.connect === 'function' ? await db.connect() : null
  const runner = client || db
  const nowAt = nowIso()
  try {
    await runner.query('BEGIN')
    const reservationResult = await runner.query(
      `
        SELECT *
        FROM ${BUDGET_RESERVATIONS_TABLE}
        WHERE request_id = $1
          AND campaign_id = $2
          AND ad_id = $3
        ORDER BY created_at DESC
        LIMIT 1
        FOR UPDATE
      `,
      [requestId, campaignId, adId],
    )
    const reservation = Array.isArray(reservationResult.rows) ? reservationResult.rows[0] : null
    if (!reservation) {
      await runner.query('ROLLBACK')
      return {
        settled: false,
        reasonCode: 'budget_reservation_missing',
      }
    }

    const reservationId = String(reservation.reservation_id || '').trim()
    const status = String(reservation.status || '').trim()
    if (status === 'settled') {
      await runner.query('COMMIT')
      return {
        settled: true,
        reasonCode: 'budget_already_settled',
        reservationId,
      }
    }
    if (status !== 'reserved') {
      await runner.query('ROLLBACK')
      return {
        settled: false,
        reasonCode: 'budget_reservation_not_active',
        reservationId,
      }
    }

    await runner.query(
      `
        UPDATE ${BUDGET_RESERVATIONS_TABLE}
        SET
          status = 'settled',
          reason_code = 'settled',
          settled_fact_id = $2,
          updated_at = $3::timestamptz
        WHERE reservation_id = $1
      `,
      [reservationId, factId, nowAt],
    )

    await runner.query(
      `
        INSERT INTO ${BUDGET_LEDGER_TABLE} (
          ledger_id,
          campaign_id,
          reservation_id,
          request_id,
          fact_id,
          entry_type,
          amount_usd,
          currency,
          metadata,
          created_at
        )
        VALUES ($1, $2, $3, $4, $5, 'settle', $6, 'USD', $7::jsonb, $8::timestamptz)
      `,
      [
        createId('bled'),
        campaignId,
        reservationId,
        requestId,
        factId,
        amountUsd,
        JSON.stringify({
          adId,
          source: String(input.source || 'click').trim() || 'click',
        }),
        nowAt,
      ],
    )
    await runner.query('COMMIT')
    return {
      settled: true,
      reasonCode: 'budget_settled',
      reservationId,
    }
  } catch (error) {
    await runner.query('ROLLBACK').catch(() => {})
    return {
      settled: false,
      reasonCode: 'budget_settlement_error',
      error: error instanceof Error ? error.message : 'budget_settlement_error',
    }
  } finally {
    if (client) client.release()
  }
}

async function upsertCampaignBudgetConfig(input = {}) {
  const campaignId = normalizeCampaignId(input.campaignId)
  const appId = String(input.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(input.accountId || resolveAccountIdForApp(appId), '')
  const status = String(input.status || 'active').trim().toLowerCase()
  const normalizedStatus = ['active', 'paused', 'archived'].includes(status) ? status : 'active'
  const lifetimeBudgetUsd = round(clampNumber(input.lifetimeBudgetUsd, 0, Number.MAX_SAFE_INTEGER, 0), 4)
  const dailyBudgetValue = clampNumber(input.dailyBudgetUsd, 0, Number.MAX_SAFE_INTEGER, NaN)
  const dailyBudgetUsd = Number.isFinite(dailyBudgetValue) && dailyBudgetValue > 0
    ? round(dailyBudgetValue, 4)
    : null
  if (!campaignId) throw new Error('campaignId is required.')
  if (!(lifetimeBudgetUsd > 0)) throw new Error('lifetimeBudgetUsd must be greater than 0.')

  const db = settlementStore.pool
  if (!db) throw new Error('budget store unavailable.')

  const nowAt = nowIso()
  await db.query(
    `
      INSERT INTO ${CAMPAIGNS_TABLE} (
        campaign_id,
        account_id,
        app_id,
        status,
        metadata,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5::jsonb, $6::timestamptz, $6::timestamptz)
      ON CONFLICT (campaign_id) DO UPDATE
      SET
        account_id = EXCLUDED.account_id,
        app_id = EXCLUDED.app_id,
        status = EXCLUDED.status,
        metadata = EXCLUDED.metadata,
        updated_at = EXCLUDED.updated_at
    `,
    [
      campaignId,
      accountId,
      appId,
      normalizedStatus,
      JSON.stringify(input.metadata && typeof input.metadata === 'object' ? input.metadata : {}),
      nowAt,
    ],
  )
  await db.query(
    `
      INSERT INTO ${CAMPAIGN_BUDGET_LIMITS_TABLE} (
        campaign_id,
        daily_budget_usd,
        lifetime_budget_usd,
        currency,
        timezone,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, 'USD', $4, $5::timestamptz, $5::timestamptz)
      ON CONFLICT (campaign_id) DO UPDATE
      SET
        daily_budget_usd = EXCLUDED.daily_budget_usd,
        lifetime_budget_usd = EXCLUDED.lifetime_budget_usd,
        timezone = EXCLUDED.timezone,
        updated_at = EXCLUDED.updated_at
    `,
    [
      campaignId,
      dailyBudgetUsd,
      lifetimeBudgetUsd,
      String(input.timezone || 'UTC').trim() || 'UTC',
      nowAt,
    ],
  )

  return await getCampaignBudgetSnapshot(campaignId)
}

async function listCampaignBudgetStatuses(scope = {}, options = {}) {
  const db = settlementStore.pool
  if (!db) return []
  await cleanupExpiredBudgetReservations({ pool: db })

  const clauses = []
  const values = []
  let cursor = 1
  if (scope.accountId) {
    clauses.push(`c.account_id = $${cursor}`)
    values.push(scope.accountId)
    cursor += 1
  }
  if (scope.appId) {
    clauses.push(`c.app_id = $${cursor}`)
    values.push(scope.appId)
    cursor += 1
  }
  const campaignId = normalizeCampaignId(options.campaignId)
  if (campaignId) {
    clauses.push(`c.campaign_id = $${cursor}`)
    values.push(campaignId)
    cursor += 1
  }
  const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : ''

  const result = await db.query(
    `
      SELECT c.campaign_id
      FROM ${CAMPAIGNS_TABLE} c
      ${whereClause}
      ORDER BY c.updated_at DESC
      LIMIT 500
    `,
    values,
  )
  const campaignRows = Array.isArray(result.rows) ? result.rows : []
  const snapshots = []
  for (const row of campaignRows) {
    const snapshot = await getCampaignBudgetSnapshot(String(row.campaign_id || '').trim(), { pool: db })
    if (snapshot?.campaignId) snapshots.push(snapshot)
  }
  return snapshots
}

function buildConversionFactIdempotencyKey(payload = {}) {
  const appId = String(payload.appId || '').trim()
  const requestId = String(payload.requestId || '').trim()
  const conversionId = String(payload.conversionId || '').trim()
  const eventSeq = String(payload.eventSeq || '').trim()
  const postbackType = String(payload.postbackType || '').trim().toLowerCase()
  const postbackStatus = String(payload.postbackStatus || '').trim().toLowerCase()
  const adId = String(payload.adId || '').trim()
  const cpaUsd = round(clampNumber(payload.cpaUsd, 0, Number.MAX_SAFE_INTEGER, 0), 4)
  const fallback = `${adId}|${cpaUsd.toFixed(4)}`
  const semantic = [appId, requestId, conversionId, eventSeq, postbackType, postbackStatus, fallback].join('|')
  return `fact_${createHash('sha256').update(semantic).digest('hex').slice(0, 24)}`
}

function buildClickFactIdempotencyKey(payload = {}) {
  const appId = String(payload.appId || '').trim()
  const requestId = String(payload.requestId || '').trim()
  const placementId = normalizePlacementIdWithMigration(String(payload.placementId || '').trim())
  const adId = String(payload.adId || '').trim()
  const source = String(payload.source || '').trim().toLowerCase() || 'sdk'
  const clickId = String(payload.clickId || payload.eventSeq || '').trim()

  if (!requestId) {
    return `fact_${createHash('sha256').update(`${createId('clk')}|${Date.now()}`).digest('hex').slice(0, 24)}`
  }

  if (!clickId) {
    return `fact_${createHash('sha256').update(`${appId}|${requestId}|${placementId}|${adId}|${source}|${createId('clk')}`).digest('hex').slice(0, 24)}`
  }

  const semantic = [appId, requestId, placementId, adId, source, clickId].join('|')
  return `fact_${createHash('sha256').update(semantic).digest('hex').slice(0, 24)}`
}

async function recordConversionFact(payload) {
  const request = payload && typeof payload === 'object' ? payload : {}
  if (!Array.isArray(state.conversionFacts)) {
    state.conversionFacts = []
  }

  const placementId = normalizePlacementIdWithMigration(
    String(request.placementId || '').trim() || await findPlacementIdByRequestId(request.requestId),
  )
  const placementKey = String(request.placementKey || '').trim() || resolvePlacementKeyById(placementId, request.appId)
  const idempotencyKey = buildConversionFactIdempotencyKey({
    ...request,
    placementId,
  })

  const fact = normalizeConversionFact({
    ...request,
    placementId,
    placementKey,
    idempotencyKey,
    factId: createId('fact'),
    createdAt: nowIso(),
  })
  return writeConversionFact(fact)
}

async function recordClickRevenueFactFromBid(payload = {}) {
  const request = payload && typeof payload === 'object' ? payload : {}
  const requestId = String(request.requestId || '').trim()
  if (!requestId) {
    return {
      recorded: false,
      duplicate: false,
      fact: null,
      reason: 'missing_request_id',
      bidPriceUsd: 0,
      targetUrl: '',
      pricingSnapshot: null,
      placementId: '',
      placementKey: '',
      appId: '',
      accountId: '',
      campaignId: '',
      riskDecision: null,
      budgetDecision: null,
    }
  }

  const decision = await findRuntimeDecisionByRequestId(requestId)
  if (!decision) {
    return {
      recorded: false,
      duplicate: false,
      fact: null,
      reason: 'request_not_found',
      bidPriceUsd: 0,
      targetUrl: '',
      pricingSnapshot: null,
      placementId: '',
      placementKey: '',
      appId: '',
      accountId: '',
      campaignId: '',
      riskDecision: null,
      budgetDecision: null,
    }
  }

  const appId = String(decision.appId || request.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(
    decision.accountId || request.accountId || resolveAccountIdForApp(appId),
    '',
  )
  const placementId = normalizePlacementIdWithMigration(
    String(request.placementId || decision.placementId || '').trim(),
    PLACEMENT_ID_FROM_ANSWER,
  )
  const placementKey = String(request.placementKey || decision.placementKey || '').trim()
    || resolvePlacementKeyById(placementId, appId)
  const campaignId = normalizeCampaignId(
    request.campaignId
    || decision?.runtime?.winnerBid?.campaignId
    || resolveCampaignIdFromBid(decision?.runtime?.winnerBid),
  )
  const adLookup = normalizeDecisionAdSnapshotByRequest(decision, request.adId)
  const targetUrl = String(
    request.targetUrl
    || adLookup.matched?.targetUrl
    || adLookup.fallback?.targetUrl
    || '',
  ).trim()
  const requestedAdId = String(request.adId || '').trim()
  const adId = requestedAdId || String(adLookup.matched?.adId || adLookup.fallback?.adId || '').trim()
  const hasDecisionAds = adLookup.found
  if (requestedAdId && hasDecisionAds && !adLookup.matched) {
    return {
      recorded: false,
      duplicate: false,
      fact: null,
      reason: 'ad_not_matched',
      bidPriceUsd: 0,
      targetUrl,
      pricingSnapshot: null,
      placementId,
      placementKey,
      appId,
      accountId,
      campaignId,
      riskDecision: null,
      budgetDecision: null,
    }
  }

  const pricingSnapshot = normalizeBidPricingSnapshot(
    decision?.runtime?.pricingSnapshot || findPricingSnapshotByRequestId(requestId),
  )
  const rawBidPriceUsd = round(
    clampNumber(
      request.bidPriceUsd
      ?? request.bidPrice
      ?? pricingSnapshot?.cpcUsd,
      0,
      Number.MAX_SAFE_INTEGER,
      0,
    ),
    4,
  )
  const riskDecision = evaluateClickRisk({
    requestId,
    adId,
    campaignId,
    appId,
    accountId,
    userId: String(request.userId || decision.userId || '').trim(),
    sessionId: String(request.sessionId || decision.sessionId || '').trim(),
  })
  if (isRiskEnforced() && riskDecision.decision === 'block') {
    return {
      recorded: false,
      duplicate: false,
      fact: null,
      reason: riskDecision.reasonCode || 'risk_blocked',
      bidPriceUsd: 0,
      targetUrl,
      pricingSnapshot,
      placementId,
      placementKey,
      appId,
      accountId,
      campaignId,
      riskDecision,
      budgetDecision: null,
    }
  }
  const riskMultiplier = (
    isRiskEnforced()
    && riskDecision.decision === 'degrade'
    && riskDecision.multiplier > 0
  )
    ? riskDecision.multiplier
    : 1
  const bidPriceUsd = round(
    clampNumber(
      rawBidPriceUsd * riskMultiplier,
      0,
      Number.MAX_SAFE_INTEGER,
      0,
    ),
    4,
  )
  if (bidPriceUsd <= 0) {
    return {
      recorded: false,
      duplicate: false,
      fact: null,
      reason: 'missing_bid_price',
      bidPriceUsd,
      targetUrl,
      pricingSnapshot,
      placementId,
      placementKey,
      appId,
      accountId,
      campaignId,
      riskDecision,
      budgetDecision: null,
    }
  }

  if (!campaignId && isBudgetEnforced()) {
    return {
      recorded: false,
      duplicate: false,
      fact: null,
      reason: 'budget_unconfigured',
      bidPriceUsd,
      targetUrl,
      pricingSnapshot,
      placementId,
      placementKey,
      appId,
      accountId,
      campaignId: '',
      riskDecision,
      budgetDecision: {
        mode: BUDGET_ENFORCEMENT_MODE,
        decision: 'block',
        reasonCode: 'budget_unconfigured',
      },
    }
  }

  const source = String(request.source || 'sdk').trim().toLowerCase() || 'sdk'
  const occurredAt = normalizeIsoTimestamp(request.occurredAt || request.eventAt, nowIso())
  const eventSeq = String(request.eventSeq || '').trim()
  const clickId = String(request.clickId || '').trim()
  const idempotencyKey = buildClickFactIdempotencyKey({
    appId,
    requestId,
    placementId,
    adId,
    source,
    clickId,
    eventSeq,
  })

  const fact = normalizeConversionFact({
    factType: CONVERSION_FACT_TYPES.CPC,
    appId,
    accountId,
    requestId,
    sessionId: String(request.sessionId || decision.sessionId || '').trim(),
    turnId: String(request.turnId || decision.turnId || '').trim(),
    userId: String(request.userId || '').trim(),
    placementId,
    placementKey,
    adId,
    postbackType: 'conversion',
    postbackStatus: 'success',
    conversionId: clickId || `click_${source}_${createId('conv')}`,
    eventSeq,
    occurredAt,
    createdAt: nowIso(),
    cpaUsd: bidPriceUsd,
    revenueUsd: bidPriceUsd,
    currency: 'USD',
    idempotencyKey,
  })
  const { duplicate, fact: persistedFact } = await writeConversionFact(fact)
  let budgetDecision = null
  if (campaignId && !duplicate) {
    const settlement = await settleCampaignBudgetReservation({
      campaignId,
      requestId,
      adId,
      factId: String(persistedFact?.factId || fact.factId || '').trim(),
      amountUsd: bidPriceUsd,
      source,
    })
    budgetDecision = {
      mode: BUDGET_ENFORCEMENT_MODE,
      decision: settlement.settled ? 'settled' : 'blocked',
      reasonCode: settlement.reasonCode || (settlement.settled ? 'budget_settled' : 'budget_reservation_missing'),
      reservationId: settlement.reservationId || '',
    }
    if (isBudgetEnforced() && !settlement.settled) {
      return {
        recorded: false,
        duplicate,
        fact: persistedFact,
        reason: budgetDecision.reasonCode,
        bidPriceUsd,
        targetUrl,
        pricingSnapshot,
        placementId,
        placementKey,
        appId,
        accountId,
        campaignId,
        riskDecision,
        budgetDecision,
      }
    }
  }
  if (!duplicate && campaignId) {
    recordCampaignClickForRisk(campaignId)
  }

  return {
    recorded: !duplicate,
    duplicate,
    fact: persistedFact,
    reason: duplicate ? 'duplicate' : 'recorded',
    bidPriceUsd,
    targetUrl,
    pricingSnapshot,
    placementId,
    placementKey,
    appId,
    accountId,
    campaignId,
    riskDecision,
    budgetDecision,
  }
}

function recordPlacementAudit(payload) {
  state.placementAuditLogs = applyCollectionLimit([
    {
      id: createId('placement_audit'),
      createdAt: nowIso(),
      ...payload,
    },
    ...state.placementAuditLogs,
  ], MAX_PLACEMENT_AUDIT_LOGS)
}

function resolveAuditActor(req, fallback = 'dashboard') {
  if (!req || !req.headers) return fallback
  const actor = String(req.headers['x-dashboard-actor'] || req.headers['x-user-id'] || '').trim()
  return actor || fallback
}

function recordControlPlaneAudit(payload) {
  const appId = String(payload?.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(payload?.accountId || resolveAccountIdForApp(appId), '')
  state.controlPlaneAuditLogs = applyCollectionLimit([
    {
      id: createId('cp_audit'),
      createdAt: nowIso(),
      ...(payload && typeof payload === 'object' ? payload : {}),
      appId,
      accountId,
    },
    ...state.controlPlaneAuditLogs,
  ], MAX_CONTROL_PLANE_AUDIT_LOGS)
}

function queryControlPlaneAudits(searchParams) {
  const action = String(searchParams.get('action') || '').trim().toLowerCase()
  const appId = String(searchParams.get('appId') || '').trim()
  const accountId = normalizeControlPlaneAccountId(
    searchParams.get('accountId') || searchParams.get('account_id') || '',
    '',
  )
  const resourceType = String(searchParams.get('resourceType') || '').trim().toLowerCase()
  const resourceId = String(searchParams.get('resourceId') || '').trim()
  const environment = String(searchParams.get('environment') || '').trim().toLowerCase()
  const actor = String(searchParams.get('actor') || '').trim().toLowerCase()
  const limit = clampNumber(searchParams.get('limit'), 1, 500, 100)

  let rows = [...state.controlPlaneAuditLogs]
  if (action) {
    rows = rows.filter((row) => String(row?.action || '').toLowerCase() === action)
  }
  if (appId) {
    rows = rows.filter((row) => String(row?.appId || '') === appId)
  }
  if (accountId) {
    rows = rows.filter((row) => normalizeControlPlaneAccountId(row?.accountId || resolveAccountIdForApp(row?.appId), '') === accountId)
  }
  if (resourceType) {
    rows = rows.filter((row) => String(row?.resourceType || '').toLowerCase() === resourceType)
  }
  if (resourceId) {
    rows = rows.filter((row) => String(row?.resourceId || '') === resourceId)
  }
  if (environment) {
    rows = rows.filter((row) => String(row?.environment || '').toLowerCase() === environment)
  }
  if (actor) {
    rows = rows.filter((row) => String(row?.actor || '').toLowerCase() === actor)
  }

  return rows.slice(0, Math.floor(limit))
}

function recordNetworkFlowObservation(payload) {
  const appId = String(payload?.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(payload?.accountId || resolveAccountIdForApp(appId), '')
  state.networkFlowLogs = applyCollectionLimit([
    {
      id: createId('network_flow'),
      createdAt: nowIso(),
      ...(payload && typeof payload === 'object' ? payload : {}),
      appId,
      accountId,
    },
    ...state.networkFlowLogs,
  ], MAX_NETWORK_FLOW_LOGS)
}

function recordRuntimeNetworkStats(decisionResult, runtimeDebug, meta = {}) {
  const stats = state.networkFlowStats
  stats.totalRuntimeEvaluations += 1

  const networkErrors = Array.isArray(runtimeDebug?.networkErrors) ? runtimeDebug.networkErrors : []
  const snapshotUsage = runtimeDebug?.snapshotUsage && typeof runtimeDebug.snapshotUsage === 'object'
    ? runtimeDebug.snapshotUsage
    : {}
  const networkHealth = runtimeDebug?.networkHealth && typeof runtimeDebug.networkHealth === 'object'
    ? runtimeDebug.networkHealth
    : getAllNetworkHealth()

  const hasSnapshotFallback = Object.values(snapshotUsage).some(Boolean)
  const healthSummary = summarizeNetworkHealthMap(networkHealth)
  const hasNetworkError = networkErrors.length > 0
  const runtimeError = meta.runtimeError === true
  const failOpenApplied = meta.failOpenApplied === true || runtimeError
  const isDegraded =
    runtimeError || hasNetworkError || hasSnapshotFallback || healthSummary.degraded > 0 || healthSummary.open > 0

  if (isDegraded) {
    stats.degradedRuntimeEvaluations += 1
  }

  if (decisionResult === 'served' && isDegraded) {
    stats.resilientServes += 1
  }

  if (decisionResult === 'served' && hasNetworkError) {
    stats.servedWithNetworkErrors += 1
  }

  if (decisionResult === 'no_fill' && hasNetworkError) {
    stats.noFillWithNetworkErrors += 1
  }

  if (decisionResult === 'error' || runtimeError) {
    stats.runtimeErrors += 1
  }

  if (healthSummary.open > 0) {
    stats.circuitOpenEvaluations += 1
  }

  recordNetworkFlowObservation({
    requestId: meta.requestId || '',
    appId: String(meta.appId || '').trim(),
    accountId: normalizeControlPlaneAccountId(meta.accountId || resolveAccountIdForApp(meta.appId), ''),
    placementId: meta.placementId || '',
    decisionResult: decisionResult || '',
    runtimeError,
    failOpenApplied,
    networkErrors,
    snapshotUsage,
    networkHealthSummary: healthSummary,
  })
}

function isSettledConversionFact(row) {
  return String(row?.postbackStatus || '').trim().toLowerCase() === 'success'
}

function conversionFactRevenueUsd(row) {
  if (!isSettledConversionFact(row)) return 0
  return round(clampNumber(row?.revenueUsd ?? row?.cpaUsd, 0, Number.MAX_SAFE_INTEGER, 0), 4)
}

function computeRevenueBreakdownFromFacts(factRows = []) {
  const rows = Array.isArray(factRows) ? factRows : []
  let cpaRevenueUsd = 0
  let cpcRevenueUsd = 0

  for (const row of rows) {
    const revenue = conversionFactRevenueUsd(row)
    if (revenue <= 0) continue
    if (String(row?.factType || '').trim().toLowerCase() === CONVERSION_FACT_TYPES.CPC) {
      cpcRevenueUsd += revenue
      continue
    }
    cpaRevenueUsd += revenue
  }

  return {
    cpaRevenueUsd: round(cpaRevenueUsd, 4),
    cpcRevenueUsd: round(cpcRevenueUsd, 4),
  }
}

function isClickEventLogRow(row = {}) {
  const item = row && typeof row === 'object' ? row : {}
  const kind = String(item.kind || item.event || '').trim().toLowerCase()
  if (kind !== 'click') return false
  const eventType = String(item.eventType || '').trim().toLowerCase()
  return eventType === 'sdk_event' || eventType === 'redirect_click'
}

function isDismissEventLogRow(row = {}) {
  const item = row && typeof row === 'object' ? row : {}
  const kind = String(item.kind || item.event || '').trim().toLowerCase()
  if (kind !== 'dismiss') return false
  const eventType = String(item.eventType || '').trim().toLowerCase()
  return eventType === 'sdk_event'
}

function conversionFactDateKey(row) {
  const raw = String(row?.occurredAt || row?.createdAt || '').trim()
  if (!raw) return ''
  const parsed = Date.parse(raw)
  if (!Number.isFinite(parsed)) return raw.slice(0, 10)
  return new Date(parsed).toISOString().slice(0, 10)
}

function buildPlacementIdByRequestMap(decisionRows = []) {
  const rows = Array.isArray(decisionRows) ? decisionRows : []
  const map = new Map()
  for (const row of rows) {
    const requestId = String(row?.requestId || '').trim()
    const placementId = String(row?.placementId || '').trim()
    if (!requestId || !placementId || map.has(requestId)) continue
    map.set(requestId, placementId)
  }
  return map
}

function resolveFactPlacementId(row, placementIdByRequest = new Map()) {
  const placementId = String(row?.placementId || '').trim()
  if (placementId) return placementId
  const requestId = String(row?.requestId || '').trim()
  if (!requestId) return ''
  return String(placementIdByRequest.get(requestId) || '').trim()
}

function buildRevenueByPlacementMap(factRows = [], placementIdByRequest = new Map()) {
  const rows = Array.isArray(factRows) ? factRows : []
  const map = new Map()
  for (const row of rows) {
    const revenueUsd = conversionFactRevenueUsd(row)
    if (revenueUsd <= 0) continue
    const placementId = resolveFactPlacementId(row, placementIdByRequest)
    if (!placementId) continue
    map.set(placementId, round((map.get(placementId) || 0) + revenueUsd, 4))
  }
  return map
}

function computeRevenueFromFacts(factRows = []) {
  const rows = Array.isArray(factRows) ? factRows : []
  let total = 0
  for (const row of rows) {
    total += conversionFactRevenueUsd(row)
  }
  return round(total, 4)
}

function computeMetricsSummary(factRows = []) {
  const impressions = state.globalStats.impressions
  const clicks = state.globalStats.clicks
  const revenueUsd = computeRevenueFromFacts(factRows)
  const revenueBreakdown = computeRevenueBreakdownFromFacts(factRows)
  const requests = state.globalStats.requests
  const served = state.globalStats.served

  const ctr = impressions > 0 ? clicks / impressions : 0
  const ecpm = impressions > 0 ? (revenueUsd / impressions) * 1000 : 0
  const fillRate = requests > 0 ? served / requests : 0

  return {
    revenueUsd: round(revenueUsd, 2),
    impressions,
    clicks,
    ctr: round(ctr, 4),
    ecpm: round(ecpm, 2),
    fillRate: round(fillRate, 4),
    revenueBreakdown: {
      cpaRevenueUsd: round(revenueBreakdown.cpaRevenueUsd, 2),
      cpcRevenueUsd: round(revenueBreakdown.cpcRevenueUsd, 2),
    },
  }
}

function computeMetricsByDay(factRows = []) {
  state.dailyMetrics = ensureDailyMetricsWindow(state.dailyMetrics)
  const rows = createDailyMetricsSeed(7)
  const byDate = new Map(rows.map((row) => [row.date, row]))

  for (const metric of state.dailyMetrics) {
    const target = byDate.get(String(metric?.date || ''))
    if (!target) continue
    target.impressions += toPositiveInteger(metric?.impressions, 0)
    target.clicks += toPositiveInteger(metric?.clicks, 0)
  }

  for (const fact of Array.isArray(factRows) ? factRows : []) {
    const dateKey = conversionFactDateKey(fact)
    const target = byDate.get(dateKey)
    if (!target) continue
    target.revenueUsd = round(target.revenueUsd + conversionFactRevenueUsd(fact), 4)
  }

  return rows.map((row) => ({
    day: new Date(`${row.date}T00:00:00`).toLocaleDateString('en-US', { weekday: 'short' }),
    revenueUsd: round(row.revenueUsd, 2),
    impressions: row.impressions,
    clicks: row.clicks,
  }))
}

function computeMetricsByPlacement(scope = {}, factRows = []) {
  const placementIdByRequest = buildPlacementIdByRequestMap(state.decisionLogs)
  const revenueByPlacement = buildRevenueByPlacementMap(
    Array.isArray(factRows) ? factRows : [],
    placementIdByRequest,
  )
  const placementScope = getPlacementsForScope(scope, { createIfMissing: false, clone: true })
  const basePlacements = Array.isArray(placementScope.placements) ? placementScope.placements : []
  const observedPlacementIds = new Set([
    ...basePlacements.map((item) => String(item?.placementId || '').trim()).filter(Boolean),
    ...Object.keys(state.placementStats || {}),
    ...Array.from(revenueByPlacement.keys()),
  ])
  const placements = mergePlacementRowsWithObserved(
    basePlacements,
    Array.from(observedPlacementIds),
    placementScope.appId,
  )

  return placements.map((placement) => {
    const stats = ensurePlacementStats(placement.placementId)
    const ctr = stats.impressions > 0 ? stats.clicks / stats.impressions : 0
    const fillRate = stats.requests > 0 ? stats.served / stats.requests : 0

    return {
      placementId: placement.placementId,
      layer: layerFromPlacementKey(placement.placementKey),
      revenueUsd: round(revenueByPlacement.get(placement.placementId) || 0, 2),
      ctr: round(ctr, 4),
      fillRate: round(fillRate, 4),
    }
  })
}

function placementMatchesSelector(placement, request) {
  const requestedPlacementId = String(request.placementId || '').trim()
  const requestedPlacementKey = String(request.placementKey || '').trim()
  const event = String(request.event || '').trim().toLowerCase()

  if (requestedPlacementId) return placement.placementId === requestedPlacementId
  if (requestedPlacementKey) return placement.placementKey === requestedPlacementKey

  const surface = EVENT_SURFACE_MAP[event]
  if (!surface) return true
  return placement.surface === surface
}

function pickPlacementForRequest(request) {
  const placements = getPlacementsForApp(
    request?.appId,
    request?.accountId,
    { createIfMissing: true, clone: false },
  )
  return placements
    .filter((placement) => placementMatchesSelector(placement, request))
    .sort((a, b) => a.priority - b.priority)[0] || null
}

function pickPlacementsForV2BidRequest(request) {
  const placements = getPlacementsForApp(
    request?.appId,
    request?.accountId,
    { createIfMissing: true, clone: false },
  )
  const scopedPlacementId = String(request?.placementId || '').trim()
  const candidates = placements
    .filter((placement) => placementMatchesSelector(placement, {
      placementId: scopedPlacementId,
    }))
    .sort((a, b) => a.priority - b.priority)

  if (scopedPlacementId) {
    return candidates
  }
  return candidates.filter((placement) => placement?.enabled !== false)
}

function getSessionPlacementKey(sessionId, placementId) {
  return `${sessionId}::${placementId}`
}

function getUserPlacementDayKey(userId, placementId) {
  return `${userId}::${placementId}::${getTodayKey()}`
}

function recordServeCounters(placement, request) {
  const placementStats = ensurePlacementStats(placement.placementId)

  state.globalStats.requests += 1
  state.globalStats.served += 1
  state.globalStats.impressions += 1

  placementStats.requests += 1
  placementStats.served += 1
  placementStats.impressions += 1

  appendDailyMetric({ impressions: 1 })

  const sessionId = String(request.sessionId || '').trim()
  if (sessionId) {
    const key = getSessionPlacementKey(sessionId, placement.placementId)
    runtimeMemory.perSessionPlacementCount.set(key, (runtimeMemory.perSessionPlacementCount.get(key) || 0) + 1)
    runtimeMemory.cooldownBySessionPlacement.set(key, Date.now())
  }

  const userId = String(request.userId || '').trim()
  if (userId) {
    const dayKey = getUserPlacementDayKey(userId, placement.placementId)
    runtimeMemory.perUserPlacementDayCount.set(dayKey, (runtimeMemory.perUserPlacementDayCount.get(dayKey) || 0) + 1)
  }
}

function recordClickCounters(placementId) {
  const normalizedPlacementId = normalizePlacementIdWithMigration(
    String(placementId || '').trim(),
    PLACEMENT_ID_FROM_ANSWER,
  )
  const placementStats = ensurePlacementStats(normalizedPlacementId)
  state.globalStats.clicks += 1
  placementStats.clicks += 1
  appendDailyMetric({ clicks: 1 })
}

function recordBlockedOrNoFill(placement) {
  const placementStats = ensurePlacementStats(placement.placementId)
  state.globalStats.requests += 1
  placementStats.requests += 1
}

function matchBlockedTopic(context, blockedTopics) {
  if (!blockedTopics.length) return ''
  const corpus = `${String(context?.query || '')} ${String(context?.answerText || '')}`.toLowerCase()
  for (const topic of blockedTopics) {
    if (corpus.includes(topic)) return topic
  }
  return ''
}

function resolveIntentPostRulePolicy(request, placement) {
  const context = request?.context && typeof request.context === 'object' ? request.context : {}
  const placementBlockedTopics = normalizeStringList(placement?.trigger?.blockedTopics)
  const requestBlockedTopics = normalizeStringList(context?.blockedTopics)

  const isNextStepIntentCard = String(placement?.placementKey || '').trim() === NEXT_STEP_INTENT_CARD_PLACEMENT_KEY
  if (!isNextStepIntentCard) {
    return {
      intentThreshold: clampNumber(placement?.trigger?.intentThreshold, 0, 1, 0.6),
      cooldownSeconds: toPositiveInteger(placement?.trigger?.cooldownSeconds, 0),
      maxPerSession: toPositiveInteger(placement?.frequencyCap?.maxPerSession, 0),
      maxPerUserPerDay: toPositiveInteger(placement?.frequencyCap?.maxPerUserPerDay, 0),
      blockedTopics: mergeNormalizedStringLists(placementBlockedTopics, requestBlockedTopics),
    }
  }

  const placementIntentThreshold = clampNumber(placement?.trigger?.intentThreshold, 0, 1, 0)
  const placementCooldownSeconds = toPositiveInteger(placement?.trigger?.cooldownSeconds, 0)
  const placementMaxPerSession = toPositiveInteger(placement?.frequencyCap?.maxPerSession, 0)
  const placementMaxPerUserPerDay = toPositiveInteger(placement?.frequencyCap?.maxPerUserPerDay, 0)

  return {
    intentThreshold: Math.max(placementIntentThreshold, NEXT_STEP_INTENT_POST_RULES.intentThresholdFloor),
    cooldownSeconds: Math.max(placementCooldownSeconds, NEXT_STEP_INTENT_POST_RULES.cooldownSeconds),
    maxPerSession: placementMaxPerSession > 0 ? placementMaxPerSession : NEXT_STEP_INTENT_POST_RULES.maxPerSession,
    maxPerUserPerDay: placementMaxPerUserPerDay > 0
      ? placementMaxPerUserPerDay
      : NEXT_STEP_INTENT_POST_RULES.maxPerUserPerDay,
    blockedTopics: mergeNormalizedStringLists(
      placementBlockedTopics,
      requestBlockedTopics,
      NEXT_STEP_SENSITIVE_TOPICS,
    ),
  }
}

function buildRuntimeAdRequest(request, placement, intentScore, requestId = '') {
  const context = request?.context && typeof request.context === 'object' ? request.context : {}
  return {
    requestId: String(requestId || '').trim(),
    appId: String(request?.appId || '').trim(),
    accountId: normalizeControlPlaneAccountId(request?.accountId || resolveAccountIdForApp(request?.appId)),
    sessionId: String(request?.sessionId || '').trim(),
    userId: String(request?.userId || '').trim(),
    placementId: placement?.placementKey || placement?.placementId || ATTACH_MVP_PLACEMENT_KEY,
    context: {
      query: String(context.query || '').trim(),
      answerText: String(context.answerText || '').trim(),
      locale: String(context.locale || '').trim() || 'en-US',
      intentScore,
      intentClass: String(context.intentClass || '').trim(),
    },
  }
}

function detectLocaleHintFromText(value = '') {
  const text = String(value || '')
  if (/[\u3400-\u9fff]/.test(text)) return 'zh-CN'
  return 'en-US'
}

function normalizeRetrievalQueryMode(value) {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return DEFAULT_RETRIEVAL_QUERY_MODE
  if (normalized === 'latest_user') return DEFAULT_RETRIEVAL_QUERY_MODE
  if (normalized === 'recent_turns_concat') return 'recent_user_turns_concat'
  if (!RETRIEVAL_QUERY_MODES.has(normalized)) return DEFAULT_RETRIEVAL_QUERY_MODE
  return normalized
}

function tokenizeRetrievalText(value = '') {
  return String(value || '')
    .toLowerCase()
    .split(/[^a-z0-9\u4e00-\u9fff]+/g)
    .map((item) => item.trim())
    .filter((item) => item.length >= 2)
}

function isRetrievalEntityToken(token = '') {
  const normalized = String(token || '').trim().toLowerCase()
  if (!normalized) return false
  if (RETRIEVAL_ENTITY_STOPWORDS.has(normalized)) return false
  if (/^\d+$/.test(normalized)) return false
  if (normalized.length < 3 && !/[\u4e00-\u9fff]{2,}/.test(normalized)) return false
  return true
}

function extractRetrievalEntitiesFromTurns(recentTurns = [], options = {}) {
  const maxCount = Math.max(1, toPositiveInteger(options.maxCount, 12))
  const rows = Array.isArray(recentTurns) ? recentTurns : []
  if (rows.length <= 0) return []

  const orderedRows = [...rows].reverse()
  const dedupe = new Set()
  const entities = []

  for (const role of ['user', 'assistant']) {
    for (const row of orderedRows) {
      if (row?.role !== role) continue
      const tokens = tokenizeRetrievalText(row?.content || '')
      for (const token of tokens) {
        if (!isRetrievalEntityToken(token)) continue
        if (dedupe.has(token)) continue
        dedupe.add(token)
        entities.push(token)
        if (entities.length >= maxCount) return entities
      }
    }
  }

  return entities
}

function isAssistantEntityToken(token = '') {
  const normalized = String(token || '').trim().toLowerCase()
  if (!normalized) return false
  if (RETRIEVAL_ASSISTANT_ENTITY_STOPWORDS.has(normalized)) return false
  if (/^\d+$/.test(normalized)) return false
  if (normalized.length < 3 && !/[\u4e00-\u9fff]{2,}/.test(normalized)) return false
  return true
}

function normalizeAssistantEntityPhrase(value = '') {
  return String(value || '')
    .trim()
    .replace(/^[-*•\d).(:\s]+/g, '')
    .replace(/[:;,]+$/g, '')
    .replace(/^["'`]+|["'`]+$/g, '')
    .replace(/\s+/g, ' ')
    .trim()
}

function isAssistantEntityPhrase(phrase = '', options = {}) {
  const normalized = normalizeAssistantEntityPhrase(phrase)
  if (!normalized) return false
  if (normalized.length < 3 || normalized.length > 32) return false
  const lower = normalized.toLowerCase()
  if (RETRIEVAL_ASSISTANT_ENTITY_STOPWORDS.has(lower)) return false
  if (/^\d+$/.test(lower)) return false
  const words = lower.split(/\s+/g).filter(Boolean)
  if (words.length <= 0 || words.length > 4) return false
  if (RETRIEVAL_ASSISTANT_ENTITY_STOPWORDS.has(words[0])) return false
  if (words.every((word) => RETRIEVAL_ASSISTANT_ENTITY_STOPWORDS.has(word))) return false
  const entityLike = Boolean(options.aliasSignal)
    || /[A-Z]/.test(normalized)
    || /[.\-+&]/.test(normalized)
    || words.some((word) => /[0-9]/.test(word))
  if (!entityLike) return false
  return true
}

function collectAssistantEntityCandidates(content = '') {
  const text = String(content || '')
  if (!text) return []
  const candidates = []
  const pushCandidate = (value, aliasSignal = false) => {
    const normalized = normalizeAssistantEntityPhrase(value)
    if (!normalized) return
    candidates.push({
      value: normalized,
      aliasSignal: Boolean(aliasSignal),
    })
  }
  const pushSplitCandidates = (value, aliasSignal = false) => {
    const source = String(value || '').trim()
    if (!source) return
    const parts = source.split(/[\/|,;]/g)
    for (const part of parts) {
      pushCandidate(part, aliasSignal)
    }
  }

  const strongMatches = [...text.matchAll(/\*\*([^*]{2,96})\*\*/g)]
  for (const match of strongMatches) {
    const chunk = String(match?.[1] || '').trim()
    if (!chunk) continue
    pushSplitCandidates(chunk, false)
    const aliasMatches = [...chunk.matchAll(/([A-Za-z][A-Za-z0-9.+\- ]{1,40})\s*\(([^)]{2,40})\)/g)]
    for (const aliasMatch of aliasMatches) {
      pushCandidate(aliasMatch?.[1], true)
      pushCandidate(aliasMatch?.[2], true)
    }
  }

  const slashPairs = [...text.matchAll(/([A-Za-z][A-Za-z0-9.+\- ]{1,40})\s*\/\s*([A-Za-z][A-Za-z0-9.+\- ]{1,40})/g)]
  for (const match of slashPairs) {
    pushCandidate(match?.[1], false)
    pushCandidate(match?.[2], false)
  }

  const aliasPairs = [...text.matchAll(/([A-Za-z][A-Za-z0-9.+\- ]{1,40})\s*\(([^)]{2,40})\)/g)]
  for (const match of aliasPairs) {
    pushCandidate(match?.[1], true)
    pushCandidate(match?.[2], true)
  }

  const domainMatches = text.match(/\b[A-Za-z0-9-]+\.[A-Za-z]{2,}\b/g) || []
  for (const domain of domainMatches) {
    pushCandidate(domain, false)
  }

  const camelCaseMatches = text.match(/\b[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]+)+\b/g) || []
  for (const raw of camelCaseMatches) {
    pushCandidate(raw, false)
  }

  return candidates
}

function extractAssistantEntityTokens(assistantText = '', options = {}) {
  const maxCount = Math.max(1, toPositiveInteger(options.maxCount, 16))
  const candidates = collectAssistantEntityCandidates(assistantText)
  if (candidates.length <= 0) {
    return {
      rawTokens: [],
      filteredTokens: [],
    }
  }

  const rawDedupe = new Set()
  const rawTokens = []
  const filteredDedupe = new Set()
  const filteredTokens = []

  for (const candidate of candidates) {
    const normalized = normalizeAssistantEntityPhrase(candidate?.value)
    if (!normalized) continue
    const normalizedLower = normalized.toLowerCase()
    if (!rawDedupe.has(normalizedLower)) {
      rawDedupe.add(normalizedLower)
      rawTokens.push(normalizedLower)
    }
    if (!isAssistantEntityPhrase(normalized, { aliasSignal: candidate?.aliasSignal })) continue
    if (!isAssistantEntityToken(normalizedLower)) continue
    if (filteredDedupe.has(normalizedLower)) continue
    filteredDedupe.add(normalizedLower)
    filteredTokens.push(normalizedLower)
    if (filteredTokens.length >= maxCount) {
      break
    }
  }

  return {
    rawTokens,
    filteredTokens,
  }
}

function buildRetrievalQuery(primaryQuery = '', entities = [], options = {}) {
  const query = clipText(primaryQuery, 1200)
  if (!query) return ''
  const maxTokens = Math.max(1, toPositiveInteger(options.maxEntityTokens, 24))
  const baseTokens = tokenizeRetrievalText(query)
    .filter((token) => isRetrievalEntityToken(token))
  const dedupe = new Set()
  const mergedTokens = []

  for (const token of baseTokens) {
    if (dedupe.has(token)) continue
    dedupe.add(token)
    mergedTokens.push(token)
    if (mergedTokens.length >= maxTokens) break
  }

  if (mergedTokens.length < maxTokens) {
    const extraTokens = (Array.isArray(entities) ? entities : [])
      .map((item) => String(item || '').trim().toLowerCase())
      .filter((item) => isRetrievalEntityToken(item))
    for (const token of extraTokens) {
      if (dedupe.has(token)) continue
      dedupe.add(token)
      mergedTokens.push(token)
      if (mergedTokens.length >= maxTokens) break
    }
  }

  if (mergedTokens.length <= 0) return query
  return clipText(mergedTokens.join(' '), 1200)
}

function deriveBidMessageContext(messages = [], options = {}) {
  const rows = Array.isArray(messages) ? messages : []
  const normalized = rows
    .map((row) => ({
      role: String(row?.role || '').trim().toLowerCase(),
      content: String(row?.content || '').trim(),
      timestamp: String(row?.timestamp || '').trim(),
    }))
    .filter((row) => row.content && V2_BID_MESSAGE_ROLES.has(row.role))
  let latestUserRow = null
  let latestUserIndex = -1
  for (let index = normalized.length - 1; index >= 0; index -= 1) {
    if (normalized[index]?.role === 'user') {
      latestUserRow = normalized[index]
      latestUserIndex = index
      break
    }
  }

  let latestAssistantRow = null
  if (latestUserIndex >= 0) {
    for (let index = latestUserIndex + 1; index < normalized.length; index += 1) {
      if (normalized[index]?.role === 'assistant') {
        latestAssistantRow = normalized[index]
      }
    }
  }

  const query = clipText(latestUserRow?.content || '', 1200)
  const answerText = clipText(latestAssistantRow?.content || '', 1200)
  const latestUserQuery = query
  const recentTurns = [latestUserRow, latestAssistantRow].filter(Boolean)
  const retrievalEntities = Array.from(new Set(
    tokenizeRetrievalText(latestUserQuery).filter((token) => isRetrievalEntityToken(token)),
  )).slice(0, 12)
  const assistantEntityExtraction = extractAssistantEntityTokens(answerText, {
    maxCount: Math.max(1, toPositiveInteger(options.assistantEntityMaxCount, 16)),
  })
  const assistantEntityTokensRaw = assistantEntityExtraction.rawTokens
  const assistantEntityTokensFiltered = assistantEntityExtraction.filteredTokens
  const latestUserEntityTokenSet = new Set(
    tokenizeRetrievalText(latestUserQuery).filter((token) => isRetrievalEntityToken(token)),
  )
  const brandEntityTokens = assistantEntityTokensFiltered.filter((token) => (
    tokenizeRetrievalText(token)
      .filter((part) => isRetrievalEntityToken(part))
      .some((part) => latestUserEntityTokenSet.has(part))
  ))
  const semanticQuery = clipText(latestUserQuery || query, 1200)
  const sparseQuery = buildRetrievalQuery(
    semanticQuery,
    [...assistantEntityTokensFiltered, ...retrievalEntities],
    {
      maxEntityTokens: Math.max(1, toPositiveInteger(options.sparseQueryMaxTokens, 18)),
    },
  )
  const retrievalQuery = sparseQuery
  const localeHint = detectLocaleHintFromText(`${query} ${answerText}`)
  return {
    contextWindowMode: 'latest_turn_only',
    query: clipText(query, 1200),
    answerText: clipText(answerText, 1200),
    latestUserQuery: clipText(latestUserQuery || query, 1200),
    semanticQuery,
    sparseQuery,
    retrievalEntities,
    assistantEntityTokensRaw,
    assistantEntityTokensFiltered,
    assistantEntityTokens: assistantEntityTokensFiltered,
    brandEntityTokens,
    retrievalQuery,
    recentTurns,
    localeHint,
  }
}

function toDecisionAdFromBid(bid) {
  if (!bid || typeof bid !== 'object') return null
  const adId = String(bid.bidId || '').trim() || String(bid.url || '').trim()
  if (!adId) return null
  const imageUrl = toHttpUrl(
    bid.image_url
    || bid.imageUrl,
  )

  return {
    adId,
    title: String(bid.headline || '').trim(),
    description: String(bid.description || '').trim(),
    targetUrl: String(bid.url || '').trim(),
    disclosure: 'Sponsored',
    sourceNetwork: String(bid.dsp || '').trim(),
    tracking: {
      clickUrl: String(bid.url || '').trim(),
    },
    bidValue: clampNumber(bid.price, 0, Number.MAX_SAFE_INTEGER, 0),
    ...(imageUrl ? { image_url: imageUrl } : {}),
  }
}

function resolveTriggerTypeByPlacementId(placementId = '') {
  const normalizedPlacementId = normalizePlacementIdWithMigration(placementId, PLACEMENT_ID_FROM_ANSWER)
  if (normalizedPlacementId === PLACEMENT_ID_INTENT_RECOMMENDATION) {
    return 'intent_recommendation'
  }
  return 'from_answer'
}

function isTimeoutLikeMessage(value) {
  const text = String(value || '').trim().toLowerCase()
  if (!text) return false
  return text.includes('timeout') || text.includes('timed out') || text.includes('abort')
}

function normalizeInventoryPrecheck(value, errorMessage = '') {
  const summary = value && typeof value === 'object' ? value : {}
  return {
    ready: typeof summary.ready === 'boolean' ? summary.ready : null,
    totalOffers: toPositiveInteger(summary.totalOffers, 0),
    missingNetworks: Array.isArray(summary.missingNetworks) ? summary.missingNetworks : [],
    checkedAt: String(summary.checkedAt || '').trim(),
    error: String(errorMessage || '').trim(),
  }
}

function buildV2BidBudgetSignal(stageDurationsMs = {}, budgetMs = V2_BID_BUDGET_MS) {
  const durations = stageDurationsMs && typeof stageDurationsMs === 'object' ? stageDurationsMs : {}
  const exceeded = {
    intent: toPositiveInteger(durations.intent, 0) > toPositiveInteger(budgetMs.intent, 0),
    retrieval: toPositiveInteger(durations.retrieval, 0) > toPositiveInteger(budgetMs.retrieval, 0),
    ranking: toPositiveInteger(durations.ranking, 0) > toPositiveInteger(budgetMs.ranking, 0),
    delivery: toPositiveInteger(durations.delivery, 0) > toPositiveInteger(budgetMs.delivery, 0),
    total: toPositiveInteger(durations.total, 0) > toPositiveInteger(budgetMs.total, 0),
  }
  const stage = ['intent', 'retrieval', 'ranking', 'delivery', 'total']
    .find((item) => exceeded[item]) || ''
  return {
    budgetMs,
    budgetExceeded: exceeded,
    timeoutSignal: {
      occurred: Boolean(stage),
      stage,
      budgetMs: stage ? toPositiveInteger(budgetMs[stage], 0) : 0,
    },
  }
}

function createV2BidStageStatusMap() {
  return {
    intent: 'pending',
    opportunity: 'pending',
    retrieval: 'pending',
    ranking: 'pending',
    delivery: 'pending',
    attribution: 'pending',
  }
}

function applyRiskMultiplierToBidPrice(bid, multiplier = 1) {
  const safeMultiplier = clampNumber(multiplier, 0.1, 1, 1)
  if (!bid || typeof bid !== 'object') return bid
  const next = {
    ...bid,
    price: round(clampNumber(bid.price, 0, Number.MAX_SAFE_INTEGER, 0) * safeMultiplier, 4),
  }
  if (next.pricing && typeof next.pricing === 'object') {
    const pricing = { ...next.pricing }
    if (Number.isFinite(Number(pricing.cpcUsd))) {
      pricing.cpcUsd = round(clampNumber(pricing.cpcUsd, 0, Number.MAX_SAFE_INTEGER, 0) * safeMultiplier, 4)
    }
    next.pricing = pricing
  }
  return next
}

async function selectWinnerWithBudgetAndRisk({
  rankedCandidates = [],
  request = {},
  requestId = '',
  placementId = '',
  scoreFloor = 0.32,
}) {
  const ranked = Array.isArray(rankedCandidates) ? rankedCandidates : []
  const diagnostics = {
    evaluatedCount: 0,
    eligibleCount: 0,
    budgetRejectedCount: 0,
    budgetUnconfiguredCount: 0,
    riskBlockedCount: 0,
    selectedCampaignId: '',
  }
  for (const candidate of ranked) {
    diagnostics.evaluatedCount += 1
    if (clampNumber(candidate?.rankScore, 0, 1, 0) < clampNumber(scoreFloor, 0, 1, 0.32)) {
      continue
    }
    const candidateBid = mapRankedCandidateToBid(candidate, { placement: 'block' })
    if (!candidateBid) continue
    diagnostics.eligibleCount += 1

    const mappedCampaignId = resolveCampaignIdFromBid(candidateBid) || resolveCampaignIdFromCandidate(candidate)
    const fallbackCampaignId = `cmp_unmapped_${String(
      candidateBid.bidId || candidate.offerId || createId('offer'),
    ).trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').slice(0, 36)}`
    const campaignId = mappedCampaignId || fallbackCampaignId
    if (!mappedCampaignId && isBudgetEnforced()) {
      diagnostics.budgetUnconfiguredCount += 1
      continue
    }
    const normalizedBid = {
      ...candidateBid,
      campaignId,
    }

    const riskDecision = evaluateBidRisk({
      campaignId,
      appId: request.appId,
      accountId: request.accountId,
      userId: request.userId,
      sessionId: request.chatId,
      placementId,
    })
    if (isRiskEnforced() && riskDecision.decision === 'block') {
      diagnostics.riskBlockedCount += 1
      continue
    }
    const riskMultiplier = (
      isRiskEnforced()
      && riskDecision.decision === 'degrade'
      && riskDecision.multiplier > 0
    )
      ? riskDecision.multiplier
      : 1
    const riskAdjustedBid = applyRiskMultiplierToBidPrice(normalizedBid, riskMultiplier)

    const reserveUsd = round(
      clampNumber(
        riskAdjustedBid?.pricing?.cpcUsd ?? riskAdjustedBid?.price,
        0,
        Number.MAX_SAFE_INTEGER,
        0,
      ),
      4,
    )
    let budgetDecision = {
      mode: BUDGET_ENFORCEMENT_MODE,
      decision: 'skipped',
      reasonCode: 'budget_off',
      reservationId: '',
    }
    if (isBudgetEnforced() || isBudgetMonitorOnly()) {
      const reservation = await tryReserveCampaignBudget({
        campaignId,
        requestId,
        adId: String(riskAdjustedBid.bidId || '').trim(),
        reserveUsd,
        accountId: request.accountId,
        appId: request.appId,
        source: 'v2_bid',
      })
      const wouldBlockForMissingCampaign = !mappedCampaignId && !reservation.allowed
      budgetDecision = {
        mode: BUDGET_ENFORCEMENT_MODE,
        decision: reservation.allowed
          ? 'reserved'
          : (isBudgetMonitorOnly() || !isBudgetEnforced() ? 'monitor_would_block' : 'blocked'),
        reasonCode: wouldBlockForMissingCampaign
          ? 'budget_unconfigured'
          : (reservation.reasonCode || (reservation.allowed ? 'budget_reserved' : 'budget_exhausted')),
        reservationId: reservation.reservation?.reservationId || '',
        budgetSnapshot: reservation.budgetSnapshot || null,
      }
      if (!reservation.allowed) {
        diagnostics.budgetRejectedCount += 1
        if (isBudgetEnforced()) continue
      }
    }

    diagnostics.selectedCampaignId = campaignId
    const winnerPricing = riskAdjustedBid.pricing && typeof riskAdjustedBid.pricing === 'object'
      ? {
          ...riskAdjustedBid.pricing,
          pricingSemanticsVersion: CPC_PRICING_SEMANTICS_VERSION,
          billingUnit: 'cpc',
        }
      : riskAdjustedBid.pricing
    const winnerPrice = round(
      clampNumber(
        winnerPricing?.cpcUsd ?? riskAdjustedBid?.price,
        0,
        Number.MAX_SAFE_INTEGER,
        0,
      ),
      4,
    )
    return {
      winnerCandidate: candidate,
      winnerBid: {
        ...riskAdjustedBid,
        campaignId,
        price: winnerPrice,
        pricing: winnerPricing,
      },
      reasonCode: 'served',
      budgetDecision,
      riskDecision: {
        ...riskDecision,
        multiplierApplied: riskMultiplier,
      },
      diagnostics,
    }
  }

  let reasonCode = 'inventory_no_match'
  if (isBudgetEnforced() && diagnostics.budgetUnconfiguredCount > 0 && diagnostics.eligibleCount === diagnostics.budgetUnconfiguredCount) {
    reasonCode = 'budget_unconfigured'
  } else if (isBudgetEnforced() && diagnostics.budgetRejectedCount > 0) {
    reasonCode = 'budget_exhausted'
  } else if (isRiskEnforced() && diagnostics.riskBlockedCount > 0 && diagnostics.eligibleCount === diagnostics.riskBlockedCount) {
    reasonCode = 'risk_blocked'
  }

  return {
    winnerCandidate: null,
    winnerBid: null,
    reasonCode,
    budgetDecision: {
      mode: BUDGET_ENFORCEMENT_MODE,
      decision: 'none',
      reasonCode: reasonCode === 'budget_unconfigured' || reasonCode === 'budget_exhausted'
        ? reasonCode
        : 'budget_not_selected',
      reservationId: '',
    },
    riskDecision: {
      mode: RISK_ENFORCEMENT_MODE,
      decision: reasonCode === 'risk_blocked' ? 'block' : 'allow',
      reasonCode: reasonCode === 'risk_blocked' ? 'risk_blocked' : 'risk_allow',
      multiplierApplied: 1,
    },
    diagnostics,
  }
}

async function evaluateSinglePlacementOpportunity({
  request = {},
  requestId = '',
  placement = null,
  messageContext = {},
  intent = {},
  intentUpstreamFailure = null,
  precheckInventory = {},
  writer,
  runtimeConfig = null,
}) {
  const placementId = normalizePlacementIdWithMigration(
    String(placement?.placementId || '').trim(),
    PLACEMENT_ID_FROM_ANSWER,
  )
  const placementKey = String(placement?.placementKey || PLACEMENT_KEY_BY_ID[placementId] || '').trim()
  const triggerType = resolveTriggerTypeByPlacementId(placementId)
  const blockedTopics = normalizeStringList(placement?.trigger?.blockedTopics)
  const intentThreshold = clampNumber(placement?.trigger?.intentThreshold, 0, 1, 0.6)
  const languageMatchMode = String(runtimeConfig?.languagePolicy?.localeMatchMode || 'locale_or_base').trim() || 'locale_or_base'
  const retrievalPolicy = runtimeConfig?.retrievalPolicy && typeof runtimeConfig.retrievalPolicy === 'object'
    ? runtimeConfig.retrievalPolicy
    : {}
  const retrievalQueryMode = normalizeRetrievalQueryMode(retrievalPolicy.queryMode)
  const semanticRetrievalQuery = retrievalQueryMode === 'recent_user_turns_concat'
    ? String(messageContext.query || '').trim()
    : String(
      messageContext.semanticQuery
      || messageContext.latestUserQuery
      || messageContext.query
      || '',
    ).trim()
  const sparseRetrievalQuery = retrievalQueryMode === 'recent_user_turns_concat'
    ? String(messageContext.query || '').trim()
    : String(
      messageContext.sparseQuery
      || messageContext.retrievalQuery
      || messageContext.latestUserQuery
      || messageContext.query
      || '',
    ).trim()
  const brandEntityTokens = Array.isArray(messageContext.brandEntityTokens)
    ? messageContext.brandEntityTokens
    : []
  const retrievalNetworks = deriveInventoryNetworksFromPlacement(placement, runtimeConfig)
  const retrievalFilters = {
    networks: retrievalNetworks,
    market: 'US',
    language: 'en-US',
  }
  const retrievalLanguageResolved = languageMatchMode === 'exact'
    ? {
        requested: 'en-US',
        normalized: 'en-us',
        base: 'en',
        accepted: ['en-us'],
      }
    : {
        requested: 'en-US',
        normalized: 'en-us',
        base: 'en',
        accepted: ['en-us', 'en'],
      }
  const lexicalTopK = toPositiveInteger(retrievalPolicy.lexicalTopK, 120)
  const vectorTopK = toPositiveInteger(retrievalPolicy.vectorTopK, 120)
  const finalTopK = toPositiveInteger(retrievalPolicy.finalTopK, 40)
  const bm25RefreshIntervalMs = toPositiveInteger(retrievalPolicy.bm25RefreshIntervalMs, 10 * 60 * 1000)
  const brandMissPenalty = clampNumber(retrievalPolicy?.brandIntent?.houseMissPenalty, 0, 1, 0.08)
  const houseShareCap = clampNumber(retrievalPolicy?.brandIntent?.houseShareCap, 0, 1, 0.6)
  const hybridSparseWeight = clampNumber(retrievalPolicy?.hybrid?.sparseWeight, 0, 1, 0.8)
  const hybridDenseWeight = clampNumber(retrievalPolicy?.hybrid?.denseWeight, 0, 1, 0.2)
  const hybridStrategy = String(retrievalPolicy?.hybrid?.strategy || 'rrf_then_linear').trim() || 'rrf_then_linear'
  const configuredMinLexicalScore = clampNumber(runtimeConfig?.relevancePolicy?.minLexicalScore, 0, 1, 0.02)
  const configuredMinVectorScore = clampNumber(runtimeConfig?.relevancePolicy?.minVectorScore, 0, 1, 0.14)
  const configuredTopicCoverageThreshold = clampNumber(
    runtimeConfig?.relevancePolicy?.topicCoverageThreshold,
    0,
    1,
    0.05,
  )
  const compositeGateStrict = clampNumber(runtimeConfig?.relevancePolicy?.compositeGateStrict, 0, 1, 0.44)
  const compositeGateRelaxedRaw = clampNumber(runtimeConfig?.relevancePolicy?.compositeGateRelaxed, 0, 1, 0.36)
  const compositeGateRelaxed = Math.min(compositeGateStrict, compositeGateRelaxedRaw)
  const compositeGateThresholdVersion = String(
    runtimeConfig?.relevancePolicy?.compositeGateThresholdVersion || 'composite_single_gate_v1',
  ).trim() || 'composite_single_gate_v1'
  const minLexicalScore = Math.max(configuredMinLexicalScore, 0.02)
  const minVectorScore = Math.max(configuredMinVectorScore, 0.14)
  const thresholdFloorsApplied = {
    minLexicalScore: {
      configured: configuredMinLexicalScore,
      floor: 0.02,
      effective: minLexicalScore,
    },
    minVectorScore: {
      configured: configuredMinVectorScore,
      floor: 0.14,
      effective: minVectorScore,
    },
  }
  const intentScoreFloor = clampNumber(runtimeConfig?.relevancePolicy?.intentScoreFloor, 0, 1, 0.38)
  const houseLowInfoFilterEnabled = parseFeatureSwitch(
    runtimeConfig?.relevancePolicy?.houseLowInfoFilterEnabled,
    true,
  )
  const scoreFloor = placementId === PLACEMENT_ID_INTENT_RECOMMENDATION
    ? intentScoreFloor
    : 0.32

  const stageStatusMap = createV2BidStageStatusMap()
  if (placement?.enabled === false) {
    stageStatusMap.intent = 'skipped'
  } else if (intentUpstreamFailure) {
    stageStatusMap.intent = 'error'
  } else {
    stageStatusMap.intent = 'ok'
  }

  const stageDurationsMs = {
    retrieval: 0,
    ranking: 0,
    delivery: 0,
  }

  let reasonCode = 'inventory_no_match'
  let winnerBid = null
  let winnerCandidate = null
  let upstreamFailure = intentUpstreamFailure
  let retrievalDebug = {
    lexicalHitCount: 0,
    bm25HitCount: 0,
    vectorHitCount: 0,
    fusedHitCount: 0,
    queryMode: retrievalQueryMode,
    queryUsed: sparseRetrievalQuery,
    semanticQuery: semanticRetrievalQuery,
    sparseQuery: sparseRetrievalQuery,
    contextWindowMode: String(messageContext?.contextWindowMode || 'latest_turn_only').trim() || 'latest_turn_only',
    assistantEntityTokensRaw: Array.isArray(messageContext?.assistantEntityTokensRaw)
      ? messageContext.assistantEntityTokensRaw
      : [],
    assistantEntityTokensFiltered: Array.isArray(messageContext?.assistantEntityTokensFiltered)
      ? messageContext.assistantEntityTokensFiltered
      : [],
    filters: retrievalFilters,
    languageMatchMode,
    languageResolved: retrievalLanguageResolved,
    scoring: {
      strategy: hybridStrategy,
      sparseWeight: hybridSparseWeight,
      denseWeight: hybridDenseWeight,
      sparseNormalization: 'min_max',
      denseNormalization: 'cosine_shift',
      rrfK: 60,
    },
    scoreStats: {
      sparseMin: 0,
      sparseMax: 0,
      denseMin: 0,
      denseMax: 0,
    },
    brandIntentDetected: false,
    brandEntityTokens: [],
    penaltiesApplied: [],
    houseShareBeforeCap: 0,
    houseShareAfterCap: 0,
    brandIntentBlockedNoHit: false,
    options: [],
  }
  let rankingDebug = {
    relevanceGate: {
      applied: placementId === PLACEMENT_ID_INTENT_RECOMMENDATION,
      placementId,
      minLexicalScore,
      minVectorScore,
      baseEligibleCount: 0,
      filteredCount: 0,
      eligibleCount: 0,
      triggered: false,
    },
    relevanceFilteredCount: 0,
    gateStrategy: 'composite_single_gate',
    gateWeights: { fused: 0.55, relevance: 0.35, topic: 0.1 },
    thresholdsApplied: {
      topicCoverage: configuredTopicCoverageThreshold,
      strict: compositeGateStrict,
      relaxed: compositeGateRelaxed,
      thresholdVersion: compositeGateThresholdVersion,
    },
    candidates: [],
    thresholdFloorsApplied,
  }
  let gatePassed = false
  let budgetDecision = {
    mode: BUDGET_ENFORCEMENT_MODE,
    decision: 'skipped',
    reasonCode: 'budget_not_evaluated',
    reservationId: '',
  }
  let riskDecision = {
    mode: RISK_ENFORCEMENT_MODE,
    decision: 'allow',
    reasonCode: 'risk_not_evaluated',
    multiplierApplied: 1,
  }

  const opportunityRecord = await writer.createOpportunityRecord({
    requestId,
    appId: request.appId,
    placementId,
    state: 'received',
    placementConfigVersion: placement?.configVersion || 1,
    payload: {
      messageContext,
      intent,
      intentThreshold,
      blockedTopics,
      placementKey,
      createdBy: 'v2_bid_opportunity_first',
    },
  })
  stageStatusMap.opportunity = 'persisted'

  if (placement?.enabled === false) {
    reasonCode = 'placement_unavailable'
    stageStatusMap.retrieval = 'skipped'
    stageStatusMap.ranking = 'skipped'
  } else if (intentUpstreamFailure) {
    reasonCode = intentUpstreamFailure.timeout ? 'upstream_timeout' : 'upstream_error'
    stageStatusMap.retrieval = 'skipped'
    stageStatusMap.ranking = 'skipped'
    rankingDebug = {
      ...rankingDebug,
      upstreamFailure: intentUpstreamFailure,
    }
  } else {
    const blockedTopic = matchBlockedTopic(
      {
        query: messageContext.query,
        answerText: messageContext.answerText,
      },
      blockedTopics,
    )

    if (blockedTopic) {
      reasonCode = 'policy_blocked'
      stageStatusMap.retrieval = 'blocked'
      stageStatusMap.ranking = 'blocked'
      rankingDebug = {
        ...rankingDebug,
        policyBlockedTopic: blockedTopic,
      }
    } else if (intent.score < intentThreshold) {
      reasonCode = 'policy_blocked'
      stageStatusMap.retrieval = 'blocked'
      stageStatusMap.ranking = 'blocked'
      rankingDebug = {
        ...rankingDebug,
        intentBelowThreshold: true,
        threshold: intentThreshold,
        intentScore: intent.score,
      }
    } else {
      gatePassed = true
      try {
        const retrievalStartedAt = Date.now()
        const retrieval = await retrieveOpportunityCandidates({
          query: sparseRetrievalQuery,
          semanticQuery: semanticRetrievalQuery,
          sparseQuery: sparseRetrievalQuery,
          topicQuery: String(messageContext.latestUserQuery || semanticRetrievalQuery || sparseRetrievalQuery || '').trim(),
          brandEntityTokens,
          contextWindowMode: String(messageContext?.contextWindowMode || 'latest_turn_only').trim() || 'latest_turn_only',
          assistantEntityTokensRaw: Array.isArray(messageContext?.assistantEntityTokensRaw)
            ? messageContext.assistantEntityTokensRaw
            : [],
          assistantEntityTokensFiltered: Array.isArray(messageContext?.assistantEntityTokensFiltered)
            ? messageContext.assistantEntityTokensFiltered
            : [],
          filters: retrievalFilters,
          queryMode: retrievalQueryMode,
          languageMatchMode,
          minLexicalScore,
          houseLowInfoFilterEnabled,
          lexicalTopK,
          vectorTopK,
          finalTopK,
          bm25RefreshIntervalMs,
          bm25ColdStartWaitMs: toPositiveInteger(retrievalPolicy.bm25ColdStartWaitMs, 120),
          brandMissPenalty,
          houseShareCap,
          hybridStrategy,
          hybridSparseWeight,
          hybridDenseWeight,
        }, {
          pool: isPostgresSettlementStore() ? settlementStore.pool : null,
          enableFallbackWhenInventoryUnavailable: isInventoryFallbackEnabled(),
          fallbackProvider: fetchLiveFallbackOpportunityCandidates,
        })
        stageDurationsMs.retrieval = Math.max(0, Date.now() - retrievalStartedAt)

        retrievalDebug = retrieval?.debug && typeof retrieval.debug === 'object'
          ? retrieval.debug
          : retrievalDebug
        retrievalDebug = {
          ...retrievalDebug,
          contextWindowMode: String(messageContext?.contextWindowMode || retrievalDebug?.contextWindowMode || 'latest_turn_only')
            .trim() || 'latest_turn_only',
          assistantEntityTokensRaw: Array.isArray(messageContext?.assistantEntityTokensRaw)
            ? messageContext.assistantEntityTokensRaw
            : (Array.isArray(retrievalDebug?.assistantEntityTokensRaw) ? retrievalDebug.assistantEntityTokensRaw : []),
          assistantEntityTokensFiltered: Array.isArray(messageContext?.assistantEntityTokensFiltered)
            ? messageContext.assistantEntityTokensFiltered
            : (Array.isArray(retrievalDebug?.assistantEntityTokensFiltered) ? retrievalDebug.assistantEntityTokensFiltered : []),
        }
        stageStatusMap.retrieval = toPositiveInteger(retrievalDebug.fusedHitCount, 0) > 0 ? 'hit' : 'miss'

        const rankingStartedAt = Date.now()
        const relevancePolicyV2 = resolvePlacementRelevancePolicyV2({
          requestId,
          placementId,
          placement,
          runtimeConfig,
        })
        const ranking = rankOpportunityCandidates({
          candidates: Array.isArray(retrieval?.candidates) ? retrieval.candidates : [],
          query: messageContext.query,
          answerText: messageContext.answerText,
          intentClass: intent.class,
          blockedTopics,
          intentScore: intent.score,
          scoreFloor,
          minLexicalScore,
          minVectorScore,
          topicCoverageGateEnabled: placementId === PLACEMENT_ID_INTENT_RECOMMENDATION,
          topicCoverageThreshold: configuredTopicCoverageThreshold,
          compositeGateStrict,
          compositeGateRelaxed,
          compositeGateThresholdVersion,
          compositeGateFusedWeight: 0.55,
          compositeGateRelevanceWeight: 0.35,
          compositeGateTopicWeight: 0.1,
          placementId,
          triggerType,
          placement: 'block',
          relevancePolicyV2,
        })
        stageDurationsMs.ranking = Math.max(0, Date.now() - rankingStartedAt)

        rankingDebug = ranking?.debug && typeof ranking.debug === 'object' ? ranking.debug : {}
        rankingDebug = {
          ...rankingDebug,
          thresholdFloorsApplied,
        }
        const budgetRiskSelection = await selectWinnerWithBudgetAndRisk({
          rankedCandidates: ranking?.ranked,
          request,
          requestId,
          placementId,
          scoreFloor: clampNumber(ranking?.debug?.scoreFloor, 0, 1, scoreFloor),
        })
        winnerCandidate = budgetRiskSelection?.winnerCandidate && typeof budgetRiskSelection.winnerCandidate === 'object'
          ? budgetRiskSelection.winnerCandidate
          : null
        winnerBid = budgetRiskSelection?.winnerBid && typeof budgetRiskSelection.winnerBid === 'object'
          ? budgetRiskSelection.winnerBid
          : null
        const rankingReasonCode = String(ranking?.reasonCode || '').trim()
        reasonCode = winnerCandidate
          ? (rankingReasonCode === 'relevance_pass_relaxed_same_vertical' ? rankingReasonCode : 'served')
          : String(budgetRiskSelection?.reasonCode || rankingReasonCode || 'inventory_no_match')
        budgetDecision = budgetRiskSelection?.budgetDecision && typeof budgetRiskSelection.budgetDecision === 'object'
          ? budgetRiskSelection.budgetDecision
          : budgetDecision
        riskDecision = budgetRiskSelection?.riskDecision && typeof budgetRiskSelection.riskDecision === 'object'
          ? budgetRiskSelection.riskDecision
          : riskDecision
        rankingDebug = {
          ...rankingDebug,
          budgetDiagnostics: budgetRiskSelection?.diagnostics || null,
          budgetDecision,
          riskDecision,
        }
        stageStatusMap.ranking = winnerCandidate ? 'selected' : (
          reasonCode === 'budget_unconfigured'
          || reasonCode === 'budget_exhausted'
          || reasonCode === 'risk_blocked'
            ? 'blocked'
            : 'no_fill'
        )
        if (winnerBid) {
          if (winnerBid.pricing && typeof winnerBid.pricing === 'object') {
            winnerBid.pricing = {
              ...winnerBid.pricing,
              pricingSemanticsVersion: CPC_PRICING_SEMANTICS_VERSION,
              billingUnit: 'cpc',
            }
            winnerBid.price = round(
              clampNumber(
                winnerBid.pricing.cpcUsd ?? winnerBid.price,
                0,
                Number.MAX_SAFE_INTEGER,
                0,
              ),
              4,
            )
          }
          const winnerCampaignId = resolveCampaignIdFromBid(winnerBid)
          if (winnerCampaignId) {
            recordCampaignImpressionForRisk(winnerCampaignId)
          }
          winnerBid = injectTrackingScopeIntoBid(winnerBid, {
            accountId: request.accountId,
          })
        }
        if (
          !winnerBid
          && reasonCode === 'inventory_no_match'
          && toPositiveInteger(retrievalDebug?.fusedHitCount, 0) <= 0
          && precheckInventory.ready === false
          && precheckInventory.totalOffers <= 0
        ) {
          reasonCode = 'inventory_empty'
          rankingDebug = {
            ...rankingDebug,
            inventoryReadiness: {
              ready: precheckInventory.ready,
              totalOffers: precheckInventory.totalOffers,
              missingNetworks: precheckInventory.missingNetworks,
              checkedAt: precheckInventory.checkedAt,
            },
          }
        }
      } catch (error) {
        upstreamFailure = {
          stage: 'retrieval_or_ranking',
          error: error instanceof Error ? error.message : 'retrieval_or_ranking_failed',
          timeout: isTimeoutLikeMessage(error instanceof Error ? error.message : ''),
        }
        reasonCode = upstreamFailure.timeout ? 'upstream_timeout' : 'upstream_error'
        stageStatusMap.retrieval = stageStatusMap.retrieval === 'pending' ? 'error' : stageStatusMap.retrieval
        stageStatusMap.ranking = 'error'
        rankingDebug = {
          ...rankingDebug,
          upstreamFailure,
        }
      }
    }
  }

  const deliveryStatus = winnerBid ? 'served' : 'no_fill'
  const pricingSnapshot = winnerBid?.pricing && typeof winnerBid.pricing === 'object'
    ? winnerBid.pricing
    : null
  const pricingVersion = String(pricingSnapshot?.modelVersion || rankingDebug?.pricingModel || 'cpa_mock_v2').trim()
  const pricingSemanticsVersion = CPC_PRICING_SEMANTICS_VERSION
  const deliveryStartedAt = Date.now()

  await writer.writeDeliveryRecord({
    requestId,
    opportunityKey: opportunityRecord.opportunityKey,
    responseReference: `${requestId}:${placementId}`,
    renderAttemptId: 'render_primary',
    appId: request.appId,
    placementId,
    deliveryStatus,
    noFillReasonCode: winnerBid ? '' : reasonCode,
    payload: {
      stageStatusMap,
      reasonCode,
      retrievalDebug,
      rankingDebug,
      budgetDecision,
      riskDecision,
      intent,
      winnerBid: winnerBid || null,
      pricingSnapshot,
      triggerType,
      pricingVersion,
      pricingSemanticsVersion,
    },
  })
  await writer.updateOpportunityState(
    opportunityRecord.opportunityKey,
    winnerBid ? 'served' : 'no_fill',
    {
      reasonCode,
      deliveryStatus,
      updatedAt: nowIso(),
    },
  )
  stageDurationsMs.delivery = Math.max(0, Date.now() - deliveryStartedAt)
  stageStatusMap.delivery = 'persisted'

  return {
    placement,
    placementId,
    placementKey,
    triggerType,
    blockedTopics,
    intentThreshold,
    stageStatusMap,
    stageDurationsMs,
    reasonCode,
    winnerBid,
    winnerCandidate,
    retrievalDebug,
    rankingDebug,
    budgetDecision,
    riskDecision,
    opportunityRecord,
    gatePassed,
    upstreamFailure,
    pricingSnapshot,
    pricingVersion,
    pricingSemanticsVersion,
  }
}

async function evaluateV2BidOpportunityFirst(payload) {
  const request = payload && typeof payload === 'object' ? payload : {}
  request.appId = String(request.appId || DEFAULT_CONTROL_PLANE_APP_ID).trim()
  request.accountId = normalizeControlPlaneAccountId(
    request.accountId || resolveAccountIdForApp(request.appId),
    '',
  )

  const startedAt = Date.now()
  const requestId = createId('adreq')
  const timestamp = nowIso()
  const placements = pickPlacementsForV2BidRequest({
    appId: request.appId,
    accountId: request.accountId,
    placementId: request.placementId,
  })
  const runtimeConfig = loadRuntimeConfig(process.env, { strict: false })
  const retrievalPolicy = runtimeConfig?.retrievalPolicy && typeof runtimeConfig.retrievalPolicy === 'object'
    ? runtimeConfig.retrievalPolicy
    : {}
  const messageContext = deriveBidMessageContext(request.messages, {
    sparseQueryMaxTokens: toPositiveInteger(retrievalPolicy.sparseQueryMaxTokens, 18),
    assistantEntityMaxCount: toPositiveInteger(retrievalPolicy.assistantEntityMaxCount, 16),
  })
  const writer = createOpportunityChainWriter()

  let precheckInventory = normalizeInventoryPrecheck()
  try {
    const inventoryReadiness = await getCachedInventoryReadinessSummary()
    precheckInventory = normalizeInventoryPrecheck(inventoryReadiness)
  } catch (error) {
    precheckInventory = normalizeInventoryPrecheck(
      null,
      error instanceof Error ? error.message : 'inventory_precheck_failed',
    )
  }

  let intent = {
    score: 0,
    class: 'non_commercial',
    source: 'rule',
    latencyMs: 0,
  }
  let intentUpstreamFailure = null
  let intentLatencyMs = 0
  if (placements.length > 0) {
    const intentStartedAt = Date.now()
    try {
      intent = await scoreIntentOpportunityFirst({
        query: messageContext.query,
        answerText: messageContext.answerText,
        locale: messageContext.localeHint || 'en-US',
        recentTurns: messageContext.recentTurns,
      }, {
        runtimeConfig,
        useLlmFallback: isLlmIntentFallbackEnabled(),
        llmTimeoutMs: 300,
      })
    } catch (error) {
      intentUpstreamFailure = {
        stage: 'intent',
        error: error instanceof Error ? error.message : 'intent_failed',
        timeout: isTimeoutLikeMessage(error instanceof Error ? error.message : ''),
      }
    } finally {
      intentLatencyMs = Math.max(0, Date.now() - intentStartedAt)
    }
  }

  const placementResults = placements.length > 0
    ? await Promise.all(
      placements.map((placement) => evaluateSinglePlacementOpportunity({
        request,
        requestId,
        placement,
        messageContext,
        intent,
        intentUpstreamFailure,
        precheckInventory,
        writer,
        runtimeConfig,
      })),
    )
    : []

  const placementOptions = placementResults.map((item) => ({
    placementId: item.placementId,
    gatePassed: item.gatePassed,
    reasonCode: item.reasonCode,
    bid: item.winnerBid,
    budgetDecision: item.budgetDecision || null,
    riskDecision: item.riskDecision || null,
    bidPrice: clampNumber(item.winnerBid?.price, 0, Number.MAX_SAFE_INTEGER, 0),
    ecpmUsd: clampNumber(
      item.winnerBid?.pricing?.ecpmUsd ?? item.winnerCandidate?.pricing?.ecpmUsd,
      0,
      Number.MAX_SAFE_INTEGER,
      0,
    ),
    relevanceScore: clampNumber(
      item.winnerCandidate?.relevanceScore,
      0,
      1,
      clampNumber(item.winnerCandidate?.rankScore, 0, 1, 0),
    ),
    rankScore: clampNumber(item.winnerCandidate?.rankScore, 0, 1, 0),
    auctionScore: clampNumber(item.winnerCandidate?.auctionScore, 0, 1, 0),
    priority: toPositiveInteger(item.placement?.priority, Number.MAX_SAFE_INTEGER),
    stageStatusMap: item.stageStatusMap,
  }))

  const globalAuction = runGlobalPlacementAuction({
    options: placementOptions,
  })
  const winnerPlacementId = String(globalAuction?.winnerPlacementId || '').trim()
  const winnerBid = globalAuction?.winner?.bid && typeof globalAuction.winner.bid === 'object'
    ? globalAuction.winner.bid
    : null
  const scoredOptionsByPlacementId = new Map(
    (Array.isArray(globalAuction?.scoredOptions) ? globalAuction.scoredOptions : [])
      .map((item) => [String(item?.placementId || '').trim(), item]),
  )
  const selectedPlacementId = String(
    winnerPlacementId
    || globalAuction?.selectedOption?.placementId
    || '',
  ).trim()
  const selectedPlacementResult = placementResults.find((item) => item.placementId === selectedPlacementId) || null
  const selectedPlacement = selectedPlacementResult?.placement || placements[0] || null
  const normalizedPlacementId = normalizePlacementIdWithMigration(
    String(selectedPlacement?.placementId || selectedPlacementId || request.placementId || '').trim(),
    PLACEMENT_ID_FROM_ANSWER,
  )
  const placementKey = String(
    selectedPlacement?.placementKey
    || selectedPlacementResult?.placementKey
    || PLACEMENT_KEY_BY_ID[normalizedPlacementId]
    || '',
  ).trim()
  const triggerType = selectedPlacementResult?.triggerType || resolveTriggerTypeByPlacementId(normalizedPlacementId)
  const reasonCode = winnerBid
    ? 'served'
    : String(
      globalAuction?.noBidReasonCode
      || selectedPlacementResult?.reasonCode
      || (placements.length === 0 ? 'placement_unavailable' : 'inventory_no_match'),
    ).trim()

  const stageStatusMap = selectedPlacementResult?.stageStatusMap
    ? { ...selectedPlacementResult.stageStatusMap }
    : {
      intent: placements.length > 0
        ? (intentUpstreamFailure ? 'error' : 'ok')
        : 'skipped',
      opportunity: placements.length > 0 ? 'pending' : 'skipped',
      retrieval: placements.length > 0 ? 'pending' : 'skipped',
      ranking: placements.length > 0 ? 'pending' : 'skipped',
      delivery: placements.length > 0 ? 'pending' : 'skipped',
      attribution: 'pending',
    }

  const stageDurationsMs = {
    intent: intentLatencyMs,
    retrieval: placementResults.length > 0
      ? Math.max(...placementResults.map((item) => toPositiveInteger(item?.stageDurationsMs?.retrieval, 0)))
      : 0,
    ranking: placementResults.length > 0
      ? Math.max(...placementResults.map((item) => toPositiveInteger(item?.stageDurationsMs?.ranking, 0)))
      : 0,
    delivery: placementResults.length > 0
      ? Math.max(...placementResults.map((item) => toPositiveInteger(item?.stageDurationsMs?.delivery, 0)))
      : 0,
    total: 0,
  }

  const retrievalDebug = selectedPlacementResult?.retrievalDebug && typeof selectedPlacementResult.retrievalDebug === 'object'
    ? selectedPlacementResult.retrievalDebug
    : {
      lexicalHitCount: 0,
      bm25HitCount: 0,
      vectorHitCount: 0,
      fusedHitCount: 0,
      queryMode: normalizeRetrievalQueryMode(runtimeConfig?.retrievalPolicy?.queryMode),
      queryUsed: '',
      semanticQuery: '',
      sparseQuery: '',
      contextWindowMode: 'latest_turn_only',
      assistantEntityTokensRaw: [],
      assistantEntityTokensFiltered: [],
      filters: {
        networks: [],
        market: '',
        language: '',
      },
      languageMatchMode: String(runtimeConfig?.languagePolicy?.localeMatchMode || 'locale_or_base').trim() || 'locale_or_base',
      languageResolved: {
        requested: '',
        normalized: '',
        base: '',
        accepted: [],
      },
      scoring: {
        strategy: 'rrf_then_linear',
        sparseWeight: clampNumber(runtimeConfig?.retrievalPolicy?.hybrid?.sparseWeight, 0, 1, 0.8),
        denseWeight: clampNumber(runtimeConfig?.retrievalPolicy?.hybrid?.denseWeight, 0, 1, 0.2),
        sparseNormalization: 'min_max',
        denseNormalization: 'cosine_shift',
        rrfK: 60,
      },
      scoreStats: {
        sparseMin: 0,
        sparseMax: 0,
        denseMin: 0,
        denseMax: 0,
      },
      brandIntentDetected: false,
      brandEntityTokens: [],
      penaltiesApplied: [],
      houseShareBeforeCap: 0,
      houseShareAfterCap: 0,
      brandIntentBlockedNoHit: false,
      options: [],
    }
  const rankingDebug = selectedPlacementResult?.rankingDebug && typeof selectedPlacementResult.rankingDebug === 'object'
    ? selectedPlacementResult.rankingDebug
    : {
      gateStrategy: 'composite_single_gate',
      gateWeights: { fused: 0.55, relevance: 0.35, topic: 0.1 },
      thresholdsApplied: {
        topicCoverage: clampNumber(runtimeConfig?.relevancePolicy?.topicCoverageThreshold, 0, 1, 0.05),
        strict: clampNumber(runtimeConfig?.relevancePolicy?.compositeGateStrict, 0, 1, 0.44),
        relaxed: Math.min(
          clampNumber(runtimeConfig?.relevancePolicy?.compositeGateStrict, 0, 1, 0.44),
          clampNumber(runtimeConfig?.relevancePolicy?.compositeGateRelaxed, 0, 1, 0.36),
        ),
        thresholdVersion: String(runtimeConfig?.relevancePolicy?.compositeGateThresholdVersion || 'composite_single_gate_v1')
          .trim() || 'composite_single_gate_v1',
      },
      candidates: [],
      thresholdFloorsApplied: {
        minLexicalScore: {
          configured: clampNumber(runtimeConfig?.relevancePolicy?.minLexicalScore, 0, 1, 0.02),
          floor: 0.02,
          effective: Math.max(clampNumber(runtimeConfig?.relevancePolicy?.minLexicalScore, 0, 1, 0.02), 0.02),
        },
        minVectorScore: {
          configured: clampNumber(runtimeConfig?.relevancePolicy?.minVectorScore, 0, 1, 0.14),
          floor: 0.14,
          effective: Math.max(clampNumber(runtimeConfig?.relevancePolicy?.minVectorScore, 0, 1, 0.14), 0.14),
        },
      },
    }
  const pricingSnapshot = selectedPlacementResult?.pricingSnapshot
    || (winnerBid?.pricing && typeof winnerBid.pricing === 'object' ? winnerBid.pricing : null)
  const pricingVersion = String(
    selectedPlacementResult?.pricingVersion
    || pricingSnapshot?.modelVersion
    || rankingDebug?.pricingModel
    || 'cpa_mock_v2',
  ).trim()
  const pricingSemanticsVersion = CPC_PRICING_SEMANTICS_VERSION
  const intentThreshold = clampNumber(selectedPlacementResult?.intentThreshold, 0, 1, 0.6)
  const budgetDecision = selectedPlacementResult?.budgetDecision && typeof selectedPlacementResult.budgetDecision === 'object'
    ? selectedPlacementResult.budgetDecision
    : {
        mode: BUDGET_ENFORCEMENT_MODE,
        decision: 'skipped',
        reasonCode: 'budget_not_evaluated',
      }
  const riskDecision = selectedPlacementResult?.riskDecision && typeof selectedPlacementResult.riskDecision === 'object'
    ? selectedPlacementResult.riskDecision
    : {
        mode: RISK_ENFORCEMENT_MODE,
        decision: 'allow',
        reasonCode: 'risk_not_evaluated',
      }

  for (const item of placementResults) {
    const counterPlacement = item.placement || { placementId: item.placementId }
    const isWinner = Boolean(winnerBid && item.placementId === winnerPlacementId)
    if (isWinner) {
      recordServeCounters(counterPlacement, {
        sessionId: request.chatId,
        userId: request.userId,
      })
    } else {
      recordBlockedOrNoFill(counterPlacement)
    }
  }
  if (placementResults.length === 0) {
    recordBlockedOrNoFill({ placementId: normalizedPlacementId })
  }

  stageDurationsMs.total = Math.max(0, Date.now() - startedAt)
  const budgetSignal = buildV2BidBudgetSignal(stageDurationsMs, V2_BID_BUDGET_MS)

  const decisionResult = mapOpportunityReasonToDecision(reasonCode, Boolean(winnerBid))
  const decision = createDecision(
    decisionResult,
    winnerBid ? 'runtime_eligible' : reasonCode,
    clampNumber(intent.score, 0, 1, 0),
  )

  const multiPlacementDiagnostics = {
    evaluatedCount: placementOptions.length,
    winnerPlacementId,
    selectionReason: String(globalAuction?.selectionReason || '').trim(),
    scoring: globalAuction?.scoring && typeof globalAuction.scoring === 'object'
      ? globalAuction.scoring
      : {
          relevanceWeight: 0.95,
          bidWeight: 0.05,
          bidNormalization: 'log1p_max',
          maxBidPrice: 0,
        },
    options: placementOptions.map((item) => {
      const scoredOption = scoredOptionsByPlacementId.get(item.placementId) || {}
      return {
        placementId: item.placementId,
        gatePassed: item.gatePassed,
        reasonCode: item.reasonCode,
        bidPrice: item.bidPrice,
        ecpmUsd: item.ecpmUsd,
        relevanceScore: clampNumber(
          scoredOption.relevanceScore,
          0,
          1,
          clampNumber(item.relevanceScore, 0, 1, 0),
        ),
        bidNormalizedScore: clampNumber(scoredOption.bidNormalizedScore, 0, 1, 0),
        compositeScore: clampNumber(scoredOption.compositeScore, 0, 1, 0),
        rankScore: item.rankScore,
        auctionScore: item.auctionScore,
        budgetDecision: item?.budgetDecision || null,
        riskDecision: item?.riskDecision || null,
        stageStatusMap: item.stageStatusMap,
      }
    }),
    loserSummary: globalAuction?.loserSummary && typeof globalAuction.loserSummary === 'object'
      ? globalAuction.loserSummary
      : { totalOptions: placementOptions.length, reasonCount: {} },
  }

  const runtimeDebug = {
    bidV2: true,
    reasonCode,
    stageStatusMap,
    stageDurationsMs,
    budgetMs: budgetSignal.budgetMs,
    budgetExceeded: budgetSignal.budgetExceeded,
    timeoutSignal: budgetSignal.timeoutSignal,
    precheck: {
      placement: {
        exists: Boolean(selectedPlacement),
        enabled: selectedPlacement ? selectedPlacement.enabled !== false : false,
        placementId: normalizedPlacementId,
      },
      inventory: precheckInventory,
    },
    retrievalDebug,
    rankingDebug,
    relevance: rankingDebug?.relevanceDebug && typeof rankingDebug.relevanceDebug === 'object'
      ? rankingDebug.relevanceDebug
      : null,
    intent,
    triggerType,
    pricingVersion,
    pricingSemanticsVersion,
    pricingSnapshot,
    budgetDecision,
    riskDecision,
    multiPlacement: multiPlacementDiagnostics,
    networkErrors: [],
    snapshotUsage: {},
    networkHealth: getAllNetworkHealth(),
  }

  recordRuntimeNetworkStats(decision.result, runtimeDebug, {
    requestId,
    appId: request.appId,
    accountId: request.accountId,
    placementId: normalizedPlacementId,
  })

  const decisionRequest = {
    appId: request.appId,
    accountId: request.accountId,
    sessionId: request.chatId,
    userId: request.userId,
    turnId: '',
    event: V2_BID_EVENT,
    placementId: normalizedPlacementId,
    placementKey,
    context: {
      query: messageContext.query,
      answerText: messageContext.answerText,
      locale: messageContext.localeHint || 'en-US',
      intentScore: intent.score,
      intentClass: intent.class,
      intentInferenceMeta: {
        inferenceModel: intent?.llm?.model || '',
        inferenceFallbackReason: intent?.llm?.fallbackReason || '',
        inferenceLatencyMs: toPositiveInteger(intent.llmLatencyMs, 0),
      },
    },
  }

  const decisionAd = winnerBid ? toDecisionAdFromBid(winnerBid) : null
  await recordDecisionForRequest({
    request: decisionRequest,
    placement: selectedPlacement || {
      placementId: normalizedPlacementId,
      placementKey,
    },
    requestId,
    decision,
    runtime: {
      ...runtimeDebug,
      metrics: {
        bid_latency_ms: stageDurationsMs.total,
        stage_intent_ms: stageDurationsMs.intent,
        stage_retrieval_ms: stageDurationsMs.retrieval,
        stage_ranking_ms: stageDurationsMs.ranking,
        stage_delivery_ms: stageDurationsMs.delivery,
        retrieval_hit_count: toPositiveInteger(retrievalDebug?.fusedHitCount, 0),
        lexical_hit_count: toPositiveInteger(retrievalDebug?.lexicalHitCount, 0),
        bm25_hit_count: toPositiveInteger(retrievalDebug?.bm25HitCount, 0),
        vector_hit_count: toPositiveInteger(retrievalDebug?.vectorHitCount, 0),
      },
    },
    ads: decisionAd ? [decisionAd] : [],
  })
  persistState(state)

  return {
    requestId,
    timestamp,
    status: 'success',
    message: winnerBid ? 'Bid successful' : 'No bid',
    opportunityId: selectedPlacementResult?.opportunityRecord?.opportunityKey || '',
    intent: {
      score: clampNumber(intent.score, 0, 1, 0),
      class: String(intent.class || 'non_commercial'),
      source: String(intent.source || 'rule'),
    },
    decisionTrace: {
      stageStatus: stageStatusMap,
      reasonCode,
    },
    data: {
      bid: winnerBid,
    },
    diagnostics: {
      reasonCode,
      triggerType,
      pricingVersion,
      pricingSemanticsVersion,
      budgetDecision,
      riskDecision,
      intentThreshold,
      retrievalMode: String(retrievalDebug?.mode || '').trim(),
      retrievalHitCount: toPositiveInteger(retrievalDebug?.fusedHitCount, 0),
      ...(reasonCode === 'policy_blocked'
        ? {
            policyBlockedReason: String(
              rankingDebug?.policyBlockedTopic
              || (rankingDebug?.intentBelowThreshold ? 'intent_below_threshold' : 'policy_blocked'),
            ).trim(),
          }
        : {}),
      stageStatusMap,
      timingsMs: stageDurationsMs,
      budgetMs: budgetSignal.budgetMs,
      budgetExceeded: budgetSignal.budgetExceeded,
      timeoutSignal: budgetSignal.timeoutSignal,
      precheck: {
        placement: {
          exists: Boolean(selectedPlacement),
          enabled: selectedPlacement ? selectedPlacement.enabled !== false : false,
          placementId: normalizedPlacementId,
        },
        inventory: precheckInventory,
      },
      retrievalDebug,
      rankingDebug,
      relevanceDebug: rankingDebug?.relevanceDebug && typeof rankingDebug.relevanceDebug === 'object'
        ? rankingDebug.relevanceDebug
        : null,
      multiPlacement: multiPlacementDiagnostics,
      bidLatencyMs: stageDurationsMs.total,
    },
  }
}

async function evaluateV2BidRequest(payload) {
  return await evaluateV2BidOpportunityFirst(payload)
}

function summarizeRuntimeDebug(debug) {
  if (!debug || typeof debug !== 'object') return {}
  const entityItems = Array.isArray(debug.entities)
    ? debug.entities
      .map((item) => {
        const entityText = String(item?.entityText || '').trim()
        const normalizedText = String(item?.normalizedText || '').trim()
        const entityType = String(item?.entityType || '').trim()
        const confidence = Number(item?.confidence)
        if (!entityText && !normalizedText) return null
        return {
          entityText,
          normalizedText,
          entityType,
          confidence: Number.isFinite(confidence) ? confidence : 0,
        }
      })
      .filter(Boolean)
    : []
  const networkErrors = Array.isArray(debug.networkErrors)
    ? debug.networkErrors.map((item) => ({
        network: item?.network || '',
        errorCode: item?.errorCode || '',
        message: item?.message || '',
      }))
    : []

  return {
    entities: entityItems.length,
    entityItems,
    totalOffers: Number.isFinite(debug.totalOffers) ? debug.totalOffers : 0,
    selectedOffers: Number.isFinite(debug.selectedOffers) ? debug.selectedOffers : 0,
    matchedCandidates: Number.isFinite(debug.matchedCandidates) ? debug.matchedCandidates : 0,
    unmatchedOffers: Number.isFinite(debug.unmatchedOffers) ? debug.unmatchedOffers : 0,
    intentCardVectorFallbackUsed: Boolean(debug.intentCardVectorFallbackUsed),
    intentCardVectorFallbackSelected: Number.isFinite(debug.intentCardVectorFallbackSelected)
      ? debug.intentCardVectorFallbackSelected
      : 0,
    intentCardVectorFallbackMeta: debug.intentCardVectorFallbackMeta &&
      typeof debug.intentCardVectorFallbackMeta === 'object'
      ? {
          itemCount: toPositiveInteger(debug.intentCardVectorFallbackMeta.itemCount, 0),
          vocabularySize: toPositiveInteger(debug.intentCardVectorFallbackMeta.vocabularySize, 0),
          candidateCount: toPositiveInteger(debug.intentCardVectorFallbackMeta.candidateCount, 0),
          topK: toPositiveInteger(debug.intentCardVectorFallbackMeta.topK, 0),
          minScore: clampNumber(debug.intentCardVectorFallbackMeta.minScore, 0, 1, 0),
        }
      : null,
    noFillReason: String(debug.noFillReason || '').trim(),
    keywords: String(debug.keywords || '').trim(),
    ner: debug.ner && typeof debug.ner === 'object'
      ? {
          status: String(debug.ner.status || '').trim(),
          message: String(debug.ner.message || '').trim(),
          model: String(debug.ner.model || '').trim(),
        }
      : {},
    networkHits: debug.networkHits && typeof debug.networkHits === 'object' ? debug.networkHits : {},
    networkErrors,
    snapshotUsage: debug.snapshotUsage && typeof debug.snapshotUsage === 'object' ? debug.snapshotUsage : {},
    networkHealth: debug.networkHealth && typeof debug.networkHealth === 'object' ? debug.networkHealth : {},
    relevance: debug.relevance && typeof debug.relevance === 'object' ? debug.relevance : null,
  }
}

function clipText(value, maxLength = 800) {
  const text = String(value || '').trim()
  if (!text) return ''
  if (text.length <= maxLength) return text
  return `${text.slice(0, maxLength)}...`
}

function normalizeIntentInferenceMeta(value, options = {}) {
  const input = value && typeof value === 'object' ? value : {}
  const force = options?.force === true
  const inferenceFallbackReason = String(
    input.inferenceFallbackReason || input.fallbackReason || '',
  ).trim()
  const inferenceModel = String(input.inferenceModel || input.model || '').trim()
  const inferenceLatencyMs = toPositiveInteger(input.inferenceLatencyMs, 0)

  if (!force && !inferenceFallbackReason && !inferenceModel && inferenceLatencyMs === 0) {
    return null
  }

  return {
    inferenceFallbackReason,
    inferenceModel,
    inferenceLatencyMs,
  }
}

function buildDecisionInputSnapshot(request, placement, intentScore) {
  const context = request?.context && typeof request.context === 'object' ? request.context : {}
  const placementKey = String(placement?.placementKey || request?.placementKey || '').trim()
  const isNextStepIntentCard = placementKey === NEXT_STEP_INTENT_CARD_PLACEMENT_KEY
  const postRulePolicy = context?.postRulePolicy && typeof context.postRulePolicy === 'object'
    ? context.postRulePolicy
    : null
  const intentInference = normalizeIntentInferenceMeta(context.intentInferenceMeta, {
    force: isNextStepIntentCard,
  })

  return {
    appId: String(request?.appId || '').trim(),
    accountId: normalizeControlPlaneAccountId(request?.accountId || resolveAccountIdForApp(request?.appId), ''),
    sessionId: String(request?.sessionId || '').trim(),
    turnId: String(request?.turnId || '').trim(),
    event: String(request?.event || '').trim(),
    placementId: String(placement?.placementId || '').trim(),
    placementKey,
    query: clipText(context.query, 280),
    answerText: clipText(context.answerText, 800),
    locale: String(context.locale || '').trim(),
    intentClass: String(context.intentClass || '').trim(),
    intentScore: Number.isFinite(intentScore) ? intentScore : 0,
    ...(intentInference ? { intentInference } : {}),
    ...(postRulePolicy
      ? {
          postRules: {
            intentThreshold: Number(postRulePolicy.intentThreshold) || 0,
            cooldownSeconds: toPositiveInteger(postRulePolicy.cooldownSeconds, 0),
            maxPerSession: toPositiveInteger(postRulePolicy.maxPerSession, 0),
            maxPerUserPerDay: toPositiveInteger(postRulePolicy.maxPerUserPerDay, 0),
            blockedTopicCount: Array.isArray(postRulePolicy.blockedTopics) ? postRulePolicy.blockedTopics.length : 0,
          },
        }
      : {}),
  }
}

function summarizeAdsForDecisionLog(ads) {
  if (!Array.isArray(ads)) return []
  return ads
    .map((item) => {
      const ad = item && typeof item === 'object' ? item : null
      if (!ad) return null
      const adId = String(ad.adId || '').trim()
      const title = String(ad.title || '').trim()
      const targetUrl = String(ad.targetUrl || '').trim()
      if (!adId && !title && !targetUrl) return null
      return {
        adId,
        title,
        entityText: String(ad.entityText || '').trim(),
        sourceNetwork: String(ad.sourceNetwork || '').trim(),
        reason: String(ad.reason || '').trim(),
        targetUrl: clipText(targetUrl, 240),
      }
    })
    .filter(Boolean)
}

async function recordDecisionForRequest({ request, placement, requestId, decision, runtime, ads }) {
  const result = DECISION_REASON_ENUM.has(decision?.result) ? decision.result : 'error'
  const reason = DECISION_REASON_ENUM.has(decision?.reason) ? decision.reason : 'error'
  const context = request?.context && typeof request.context === 'object' ? request.context : {}
  const placementKey = String(placement?.placementKey || request?.placementKey || '').trim()
  const isNextStepIntentCard = placementKey === NEXT_STEP_INTENT_CARD_PLACEMENT_KEY
  const intentInference = normalizeIntentInferenceMeta(context.intentInferenceMeta, {
    force: isNextStepIntentCard,
  })

  const payload = {
    requestId,
    appId: request?.appId || '',
    accountId: normalizeControlPlaneAccountId(request?.accountId || resolveAccountIdForApp(request?.appId), ''),
    sessionId: request?.sessionId || '',
    turnId: request?.turnId || '',
    event: request?.event || '',
    placementId: placement?.placementId || '',
    placementKey,
    result,
    reason,
    reasonDetail: decision?.reasonDetail || '',
    intentScore: Number.isFinite(decision?.intentScore) ? decision.intentScore : 0,
    input: buildDecisionInputSnapshot(request, placement, decision?.intentScore),
    ads: summarizeAdsForDecisionLog(ads),
    ...(intentInference ? { intentInference } : {}),
  }

  if (runtime && typeof runtime === 'object') {
    payload.runtime = runtime
    if (runtime.stageStatusMap && typeof runtime.stageStatusMap === 'object') {
      payload.stage_status_map = runtime.stageStatusMap
    }
    if (typeof runtime.reasonCode === 'string' && runtime.reasonCode.trim()) {
      payload.reason_code = runtime.reasonCode.trim()
    }
    if (runtime.retrievalDebug && typeof runtime.retrievalDebug === 'object') {
      payload.retrieval_debug = runtime.retrievalDebug
    }
  }

  await recordDecision(payload)
  await recordEvent({
    eventType: 'decision',
    requestId: payload.requestId || '',
    appId: payload.appId || '',
    accountId: payload.accountId || '',
    sessionId: payload.sessionId || '',
    turnId: payload.turnId || '',
    placementId: payload.placementId || '',
    placementKey: payload.placementKey || '',
    event: payload.event || '',
    result,
    reason,
    reasonDetail: payload.reasonDetail || '',
  })
}

async function evaluateRequest(payload) {
  const request = payload && typeof payload === 'object' ? payload : {}
  request.appId = String(request.appId || '').trim()
  request.accountId = normalizeControlPlaneAccountId(request.accountId || resolveAccountIdForApp(request.appId))
  const context = request.context && typeof request.context === 'object' ? request.context : {}
  const intentScore = clampNumber(context.intentScore, 0, 1, 0)
  const intentClass = String(context.intentClass || '').trim().toLowerCase()

  const placement = pickPlacementForRequest(request)
  const requestId = createId('adreq')

  if (!placement) {
    const decision = createDecision('blocked', 'placement_not_configured', intentScore)
    await recordDecisionForRequest({
      request,
      placement: null,
      requestId,
      decision,
      ads: [],
    })
    persistState(state)
    return {
      requestId,
      placementId: '',
      decision,
      ads: [],
    }
  }

  if (!placement.enabled) {
    const decision = createDecision('blocked', 'placement_disabled', intentScore)
    recordBlockedOrNoFill(placement)
    await recordDecisionForRequest({
      request,
      placement,
      requestId,
      decision,
      ads: [],
    })
    persistState(state)
    return {
      requestId,
      placementId: placement.placementId,
      decision,
      ads: [],
    }
  }

  const postRulePolicy = resolveIntentPostRulePolicy(request, placement)
  context.postRulePolicy = postRulePolicy
  const blockedTopic = matchBlockedTopic(context, postRulePolicy.blockedTopics)
  if (blockedTopic) {
    const decision = createDecision('blocked', `blocked_topic:${blockedTopic}`, intentScore)
    recordBlockedOrNoFill(placement)
    await recordDecisionForRequest({
      request,
      placement,
      requestId,
      decision,
      ads: [],
    })
    persistState(state)
    return {
      requestId,
      placementId: placement.placementId,
      decision,
      ads: [],
    }
  }

  if (placement.placementKey === NEXT_STEP_INTENT_CARD_PLACEMENT_KEY && intentClass === 'non_commercial') {
    const decision = createDecision('blocked', 'intent_non_commercial', intentScore)
    recordBlockedOrNoFill(placement)
    await recordDecisionForRequest({
      request,
      placement,
      requestId,
      decision,
      ads: [],
    })
    persistState(state)
    return {
      requestId,
      placementId: placement.placementId,
      decision,
      ads: [],
    }
  }

  if (intentScore < postRulePolicy.intentThreshold) {
    const decision = createDecision('blocked', 'intent_below_threshold', intentScore)
    recordBlockedOrNoFill(placement)
    await recordDecisionForRequest({
      request,
      placement,
      requestId,
      decision,
      ads: [],
    })
    persistState(state)
    return {
      requestId,
      placementId: placement.placementId,
      decision,
      ads: [],
    }
  }

  const sessionId = String(request.sessionId || '').trim()
  const userId = String(request.userId || '').trim()

  if (postRulePolicy.cooldownSeconds > 0 && sessionId) {
    const cooldownKey = getSessionPlacementKey(sessionId, placement.placementId)
    const lastTs = runtimeMemory.cooldownBySessionPlacement.get(cooldownKey) || 0
    const withinCooldown = Date.now() - lastTs < postRulePolicy.cooldownSeconds * 1000
    if (withinCooldown) {
      const decision = createDecision('blocked', 'cooldown', intentScore)
      recordBlockedOrNoFill(placement)
      await recordDecisionForRequest({
        request,
        placement,
        requestId,
        decision,
        ads: [],
      })
      persistState(state)
      return {
        requestId,
        placementId: placement.placementId,
        decision,
        ads: [],
      }
    }
  }

  if (postRulePolicy.maxPerSession > 0 && sessionId) {
    const sessionCapKey = getSessionPlacementKey(sessionId, placement.placementId)
    const count = runtimeMemory.perSessionPlacementCount.get(sessionCapKey) || 0
    if (count >= postRulePolicy.maxPerSession) {
      const decision = createDecision('blocked', 'frequency_cap_session', intentScore)
      recordBlockedOrNoFill(placement)
      await recordDecisionForRequest({
        request,
        placement,
        requestId,
        decision,
        ads: [],
      })
      persistState(state)
      return {
        requestId,
        placementId: placement.placementId,
        decision,
        ads: [],
      }
    }
  }

  if (postRulePolicy.maxPerUserPerDay > 0 && userId) {
    const userCapKey = getUserPlacementDayKey(userId, placement.placementId)
    const count = runtimeMemory.perUserPlacementDayCount.get(userCapKey) || 0
    if (count >= postRulePolicy.maxPerUserPerDay) {
      const decision = createDecision('blocked', 'frequency_cap_user_day', intentScore)
      recordBlockedOrNoFill(placement)
      await recordDecisionForRequest({
        request,
        placement,
        requestId,
        decision,
        ads: [],
      })
      persistState(state)
      return {
        requestId,
        placementId: placement.placementId,
        decision,
        ads: [],
      }
    }
  }

  const expectedRevenue = clampNumber(
    context.expectedRevenue,
    0,
    Number.MAX_SAFE_INTEGER,
    round(0.08 + intentScore * 0.25, 4),
  )

  if (expectedRevenue < placement.trigger.minExpectedRevenue) {
    const decision = createDecision('no_fill', 'revenue_below_min', intentScore)
    recordBlockedOrNoFill(placement)
    await recordDecisionForRequest({
      request,
      placement,
      requestId,
      decision,
      ads: [],
    })
    persistState(state)
    return {
      requestId,
      placementId: placement.placementId,
      decision,
      ads: [],
    }
  }

  const runtimeAdRequest = buildRuntimeAdRequest(request, placement, intentScore, requestId)
  let runtimeResult

  try {
    runtimeResult = await runAdsRetrievalPipeline(runtimeAdRequest)
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Runtime pipeline failed'
    const decision = createDecision('no_fill', 'runtime_pipeline_fail_open', intentScore)
    recordRuntimeNetworkStats(decision.result, null, {
      requestId,
      appId: request.appId,
      accountId: request.accountId,
      placementId: placement.placementId,
      runtimeError: true,
      failOpenApplied: true,
    })
    recordBlockedOrNoFill(placement)
    await recordDecisionForRequest({
      request,
      placement,
      requestId,
      decision,
      runtime: {
        failOpenApplied: true,
        failureMode: 'runtime_pipeline_exception',
        message: errorMessage,
      },
      ads: [],
    })
    persistState(state)
    return {
      requestId,
      placementId: placement.placementId,
      decision,
      ads: [],
    }
  }

  const runtimeAds = Array.isArray(runtimeResult?.adResponse?.ads) ? runtimeResult.adResponse.ads : []
  const runtimeRequestId = String(runtimeResult?.adResponse?.requestId || requestId)
  const runtimeDebug = summarizeRuntimeDebug(runtimeResult?.debug)

  if (runtimeAds.length === 0) {
    const decision = createDecision('no_fill', 'runtime_no_offer', intentScore)
    recordRuntimeNetworkStats(decision.result, runtimeDebug, {
      requestId: runtimeRequestId,
      appId: request.appId,
      accountId: request.accountId,
      placementId: placement.placementId,
    })
    recordBlockedOrNoFill(placement)
    await recordDecisionForRequest({
      request,
      placement,
      requestId: runtimeRequestId,
      decision,
      runtime: runtimeDebug,
      ads: [],
    })
    persistState(state)
    return {
      requestId: runtimeRequestId,
      placementId: placement.placementId,
      decision,
      ads: [],
    }
  }

  recordServeCounters(placement, request)

  const scopedRuntimeAds = injectTrackingScopeIntoAds(runtimeAds, {
    accountId: request.accountId,
  })
  const ads = scopedRuntimeAds.map((ad) => ({
    ...ad,
    disclosure: placement.disclosure || ad.disclosure || 'Sponsored',
  }))

  const decision = createDecision('served', 'runtime_eligible', intentScore)
  recordRuntimeNetworkStats(decision.result, runtimeDebug, {
    requestId: runtimeRequestId,
    appId: request.appId,
    accountId: request.accountId,
    placementId: placement.placementId,
  })
  await recordDecisionForRequest({
    request,
    placement,
    requestId: runtimeRequestId,
    decision,
    runtime: runtimeDebug,
    ads,
  })

  persistState(state)

  return {
    requestId: runtimeRequestId,
    placementId: placement.placementId,
    decision,
    ads,
  }
}

function buildPlacementFromPatch(placement, patch, configVersion) {
  return normalizePlacement({
    ...placement,
    ...patch,
    configVersion,
    trigger: {
      ...placement.trigger,
      ...(patch?.trigger && typeof patch.trigger === 'object' ? patch.trigger : {}),
    },
    frequencyCap: {
      ...placement.frequencyCap,
      ...(patch?.frequencyCap && typeof patch.frequencyCap === 'object' ? patch.frequencyCap : {}),
    },
  })
}

function applyPlacementPatch(placement, patch, configVersion) {
  const next = buildPlacementFromPatch(placement, patch, configVersion)

  placement.configVersion = next.configVersion
  placement.enabled = next.enabled
  placement.disclosure = next.disclosure
  placement.priority = next.priority
  placement.surface = next.surface
  placement.format = next.format
  placement.placementKey = next.placementKey
  placement.trigger = next.trigger
  placement.frequencyCap = next.frequencyCap
  placement.bidders = next.bidders
  placement.fallback = next.fallback
  placement.relevancePolicyV2 = next.relevancePolicyV2
  placement.maxFanout = next.maxFanout
  placement.globalTimeoutMs = next.globalTimeoutMs

  return placement
}

function filterRowsByScope(rows, scope = {}) {
  const list = Array.isArray(rows) ? rows : []
  if (!scopeHasFilters(scope)) return list
  return list.filter((row) => recordMatchesScope(row, scope))
}

function computeScopedMetricsSummary(decisionRows, eventRows, factRows, controlPlaneAuditRows = []) {
  const requests = decisionRows.length
  const servedRows = decisionRows.filter((row) => String(row?.result || '') === 'served')
  const served = servedRows.length
  const bidKnownCount = decisionRows.filter((row) => String(row?.result || '') !== 'error').length
  const bidUnknownCount = Math.max(0, requests - bidKnownCount)
  const impressions = served
  const clicks = eventRows.filter((row) => isClickEventLogRow(row)).length
  const dismisses = eventRows.filter((row) => isDismissEventLogRow(row)).length
  const revenueUsd = computeRevenueFromFacts(factRows)
  const revenueBreakdown = computeRevenueBreakdownFromFacts(factRows)
  const ctr = impressions > 0 ? clicks / impressions : 0
  const ecpm = impressions > 0 ? (revenueUsd / impressions) * 1000 : 0
  const fillRate = requests > 0 ? served / requests : 0
  const offTopicProxyRate = impressions > 0 ? dismisses / impressions : 0
  const bidFillRateKnown = bidKnownCount > 0 ? served / bidKnownCount : 0
  const unknownRate = requests > 0 ? bidUnknownCount / requests : 0
  const resultBreakdown = {
    served: decisionRows.filter((row) => String(row?.result || '') === 'served').length,
    noFill: decisionRows.filter((row) => String(row?.result || '') === 'no_fill').length,
    blocked: decisionRows.filter((row) => String(row?.result || '') === 'blocked').length,
    error: decisionRows.filter((row) => String(row?.result || '') === 'error').length,
  }
  const placementUnavailableCount = decisionRows.filter((row) => (
    String(row?.reasonDetail || row?.reason || '').trim() === 'placement_unavailable'
  )).length
  const inventoryEmptyCount = decisionRows.filter((row) => (
    String(row?.reasonDetail || row?.reason || '').trim() === 'inventory_empty'
  )).length
  const scopeViolationCount = (Array.isArray(controlPlaneAuditRows) ? controlPlaneAuditRows : []).filter((row) => (
    String(row?.action || '').trim() === 'agent_access_deny'
    && String(row?.metadata?.code || '').toUpperCase().includes('SCOPE_VIOLATION')
  )).length
  const timeoutRelatedCount = decisionRows.filter((row) => {
    const runtime = row?.runtime && typeof row.runtime === 'object' ? row.runtime : {}
    if (runtime?.timeoutSignal?.occurred === true) return true
    const reasonCode = String(runtime.reasonCode || row?.reasonDetail || row?.reason || '').toLowerCase()
    if (reasonCode.includes('timeout')) return true
    const upstreamMessage = String(runtime?.rankingDebug?.upstreamFailure?.error || '').toLowerCase()
    return upstreamMessage.includes('timeout')
  }).length
  const precheckInventoryNotReadyCount = decisionRows.filter((row) => {
    const runtime = row?.runtime && typeof row.runtime === 'object' ? row.runtime : {}
    return runtime?.precheck?.inventory?.ready === false
  }).length
  const budgetExceededCount = decisionRows.filter((row) => {
    const runtime = row?.runtime && typeof row.runtime === 'object' ? row.runtime : {}
    const exceeded = runtime?.budgetExceeded && typeof runtime.budgetExceeded === 'object'
      ? runtime.budgetExceeded
      : {}
    return Object.values(exceeded).some(Boolean)
  }).length
  const budgetBlockedCount = decisionRows.filter((row) => {
    const runtime = row?.runtime && typeof row.runtime === 'object' ? row.runtime : {}
    const reasonCode = String(runtime.reasonCode || row?.reasonDetail || row?.reason || '').trim().toLowerCase()
    if (reasonCode === 'budget_exhausted' || reasonCode === 'budget_unconfigured') return true
    const budgetDecision = runtime?.budgetDecision && typeof runtime.budgetDecision === 'object'
      ? runtime.budgetDecision
      : {}
    return String(budgetDecision.decision || '').trim().toLowerCase().includes('block')
  }).length
  const riskBlockedCount = decisionRows.filter((row) => {
    const runtime = row?.runtime && typeof row.runtime === 'object' ? row.runtime : {}
    const reasonCode = String(runtime.reasonCode || row?.reasonDetail || row?.reason || '').trim().toLowerCase()
    if (reasonCode === 'risk_blocked') return true
    const riskDecision = runtime?.riskDecision && typeof runtime.riskDecision === 'object'
      ? runtime.riskDecision
      : {}
    return String(riskDecision.decision || '').trim().toLowerCase() === 'block'
  }).length
  const relevanceStrictBlockedCount = decisionRows.filter((row) => {
    const runtime = row?.runtime && typeof row.runtime === 'object' ? row.runtime : {}
    const relevance = runtime?.relevance && typeof runtime.relevance === 'object'
      ? runtime.relevance
      : (runtime?.rankingDebug?.relevanceDebug && typeof runtime.rankingDebug.relevanceDebug === 'object'
        ? runtime.rankingDebug.relevanceDebug
        : {})
    const stage = String(relevance.gateStage || '').trim().toLowerCase()
    const blockedReason = String(relevance.blockedReason || runtime.reasonCode || '').trim().toLowerCase()
    return stage === 'blocked' && blockedReason.includes('relevance_blocked_strict')
  }).length
  const relevanceRelaxedRecoveryCount = decisionRows.filter((row) => {
    const runtime = row?.runtime && typeof row.runtime === 'object' ? row.runtime : {}
    const relevance = runtime?.relevance && typeof runtime.relevance === 'object'
      ? runtime.relevance
      : (runtime?.rankingDebug?.relevanceDebug && typeof runtime.rankingDebug.relevanceDebug === 'object'
        ? runtime.rankingDebug.relevanceDebug
        : {})
    const stage = String(relevance.gateStage || '').trim().toLowerCase()
    return stage === 'relaxed' && String(row?.result || '').trim().toLowerCase() === 'served'
  }).length
  const fillRateByPlacement = {}
  for (const row of decisionRows) {
    const placementId = String(row?.placementId || '').trim()
    if (!placementId) continue
    if (!Object.prototype.hasOwnProperty.call(fillRateByPlacement, placementId)) {
      fillRateByPlacement[placementId] = {
        requests: 0,
        served: 0,
      }
    }
    fillRateByPlacement[placementId].requests += 1
    if (String(row?.result || '').trim().toLowerCase() === 'served') {
      fillRateByPlacement[placementId].served += 1
    }
  }
  const normalizedFillRateByPlacement = Object.fromEntries(
    Object.entries(fillRateByPlacement).map(([placementId, bucket]) => [
      placementId,
      bucket.requests > 0 ? round(bucket.served / bucket.requests, 4) : 0,
    ]),
  )

  return {
    revenueUsd: round(revenueUsd, 2),
    impressions,
    clicks,
    dismisses,
    ctr: round(ctr, 4),
    ecpm: round(ecpm, 2),
    fillRate: round(fillRate, 4),
    offTopicProxyRate: round(offTopicProxyRate, 4),
    relevanceStrictBlockRate: requests > 0 ? round(relevanceStrictBlockedCount / requests, 4) : 0,
    relevanceRelaxedRecoveryRate: requests > 0 ? round(relevanceRelaxedRecoveryCount / requests, 4) : 0,
    fillRateByPlacement: normalizedFillRateByPlacement,
    bidKnownCount,
    bidUnknownCount,
    bidFillRateKnown: round(bidFillRateKnown, 4),
    unknownRate: round(unknownRate, 4),
    resultBreakdown,
    timeoutRelatedCount,
    precheckInventoryNotReadyCount,
    budgetExceededCount,
    budgetBlockedCount,
    riskBlockedCount,
    relevanceStrictBlockedCount,
    relevanceRelaxedRecoveryCount,
    reasonCounts: {
      placementUnavailable: placementUnavailableCount,
      inventoryEmpty: inventoryEmptyCount,
      scopeViolation: scopeViolationCount,
    },
    reasonRatios: {
      placementUnavailable: requests > 0 ? round(placementUnavailableCount / requests, 4) : 0,
      inventoryEmpty: requests > 0 ? round(inventoryEmptyCount / requests, 4) : 0,
      scopeViolation: requests > 0 ? round(scopeViolationCount / requests, 4) : 0,
    },
    revenueBreakdown: {
      cpaRevenueUsd: round(revenueBreakdown.cpaRevenueUsd, 2),
      cpcRevenueUsd: round(revenueBreakdown.cpcRevenueUsd, 2),
    },
  }
}

function computeScopedMetricsByDay(decisionRows, eventRows, factRows) {
  const rows = createDailyMetricsSeed(7)
  const byDate = new Map(rows.map((row) => [row.date, row]))

  for (const row of decisionRows) {
    const dateKey = String(row?.createdAt || '').slice(0, 10)
    const target = byDate.get(dateKey)
    if (!target) continue
    if (String(row?.result || '') === 'served') {
      target.impressions += 1
    }
  }

  for (const row of eventRows) {
    if (!isClickEventLogRow(row)) continue
    const dateKey = String(row?.createdAt || '').slice(0, 10)
    const target = byDate.get(dateKey)
    if (!target) continue
    target.clicks += 1
  }

  for (const row of factRows) {
    const dateKey = conversionFactDateKey(row)
    const target = byDate.get(dateKey)
    if (!target) continue
    target.revenueUsd = round(target.revenueUsd + conversionFactRevenueUsd(row), 4)
  }

  return rows.map((row) => ({
    day: new Date(`${row.date}T00:00:00`).toLocaleDateString('en-US', { weekday: 'short' }),
    revenueUsd: round(row.revenueUsd, 2),
    impressions: row.impressions,
    clicks: row.clicks,
  }))
}

function computeScopedMetricsByPlacement(decisionRows, eventRows, factRows, scope = {}) {
  const decisionStatsByPlacement = new Map()
  for (const row of decisionRows) {
    const placementId = String(row?.placementId || '').trim()
    if (!placementId) continue
    if (!decisionStatsByPlacement.has(placementId)) {
      decisionStatsByPlacement.set(placementId, {
        requests: 0,
        served: 0,
        impressions: 0,
        relevanceStrictBlocked: 0,
        relevanceRelaxedRecovered: 0,
      })
    }
    const stats = decisionStatsByPlacement.get(placementId)
    stats.requests += 1
    const runtime = row?.runtime && typeof row.runtime === 'object' ? row.runtime : {}
    const relevance = runtime?.relevance && typeof runtime.relevance === 'object'
      ? runtime.relevance
      : (runtime?.rankingDebug?.relevanceDebug && typeof runtime.rankingDebug.relevanceDebug === 'object'
        ? runtime.rankingDebug.relevanceDebug
        : {})
    const gateStage = String(relevance.gateStage || '').trim().toLowerCase()
    const blockedReason = String(relevance.blockedReason || runtime.reasonCode || '').trim().toLowerCase()
    if (gateStage === 'blocked' && blockedReason.includes('relevance_blocked_strict')) {
      stats.relevanceStrictBlocked += 1
    }
    if (String(row?.result || '') === 'served') {
      stats.served += 1
      stats.impressions += 1
      if (gateStage === 'relaxed') {
        stats.relevanceRelaxedRecovered += 1
      }
    }
  }

  const placementIdByRequest = buildPlacementIdByRequestMap(decisionRows)
  const revenueByPlacement = buildRevenueByPlacementMap(factRows, placementIdByRequest)

  const clicksByPlacement = new Map()
  const dismissByPlacement = new Map()
  for (const row of eventRows) {
    const placementId = String(row?.placementId || '').trim()
    if (!placementId) continue
    if (isClickEventLogRow(row)) {
      clicksByPlacement.set(placementId, (clicksByPlacement.get(placementId) || 0) + 1)
      continue
    }
    if (isDismissEventLogRow(row)) {
      dismissByPlacement.set(placementId, (dismissByPlacement.get(placementId) || 0) + 1)
    }
  }
  const placementScope = getPlacementsForScope(scope, { createIfMissing: false, clone: true })
  const observedPlacementIds = new Set([
    ...Array.from(decisionStatsByPlacement.keys()),
    ...Array.from(clicksByPlacement.keys()),
    ...Array.from(revenueByPlacement.keys()),
  ])
  const placements = mergePlacementRowsWithObserved(
    placementScope.placements,
    Array.from(observedPlacementIds),
    placementScope.appId,
  )

  return placements.map((placement) => {
    const stats = decisionStatsByPlacement.get(placement.placementId) || {
      requests: 0,
      served: 0,
      impressions: 0,
    }
    const clicks = clicksByPlacement.get(placement.placementId) || 0
    const dismisses = dismissByPlacement.get(placement.placementId) || 0
    const ctr = stats.impressions > 0 ? clicks / stats.impressions : 0
    const fillRate = stats.requests > 0 ? stats.served / stats.requests : 0

    return {
      placementId: placement.placementId,
      layer: layerFromPlacementKey(placement.placementKey),
      revenueUsd: round(revenueByPlacement.get(placement.placementId) || 0, 2),
      ctr: round(ctr, 4),
      fillRate: round(fillRate, 4),
      offTopicProxyRate: stats.impressions > 0 ? round(dismisses / stats.impressions, 4) : 0,
      relevanceStrictBlockRate: stats.requests > 0 ? round(stats.relevanceStrictBlocked / stats.requests, 4) : 0,
      relevanceRelaxedRecoveryRate: stats.requests > 0 ? round(stats.relevanceRelaxedRecovered / stats.requests, 4) : 0,
    }
  })
}

function computeScopedNetworkFlowStats(rows) {
  const stats = createInitialNetworkFlowStats()
  for (const row of rows) {
    const networkErrors = Array.isArray(row?.networkErrors) ? row.networkErrors : []
    const snapshotUsage = row?.snapshotUsage && typeof row.snapshotUsage === 'object' ? row.snapshotUsage : {}
    const healthSummary = row?.networkHealthSummary && typeof row.networkHealthSummary === 'object'
      ? row.networkHealthSummary
      : { degraded: 0, open: 0 }
    const hasNetworkError = networkErrors.length > 0
    const hasSnapshotFallback = Object.values(snapshotUsage).some(Boolean)
    const runtimeError = row?.runtimeError === true
    const isDegraded = runtimeError || hasNetworkError || hasSnapshotFallback || healthSummary.degraded > 0
      || healthSummary.open > 0
    const decisionResult = String(row?.decisionResult || '')

    stats.totalRuntimeEvaluations += 1
    if (isDegraded) stats.degradedRuntimeEvaluations += 1
    if (decisionResult === 'served' && isDegraded) stats.resilientServes += 1
    if (decisionResult === 'served' && hasNetworkError) stats.servedWithNetworkErrors += 1
    if (decisionResult === 'no_fill' && hasNetworkError) stats.noFillWithNetworkErrors += 1
    if (decisionResult === 'error' || runtimeError) stats.runtimeErrors += 1
    if (healthSummary.open > 0) stats.circuitOpenEvaluations += 1
  }
  return stats
}

function createSettlementAggregateRow(seed = {}) {
  return {
    accountId: String(seed.accountId || '').trim(),
    appId: String(seed.appId || '').trim(),
    placementId: String(seed.placementId || '').trim(),
    layer: String(seed.layer || '').trim(),
    requests: 0,
    served: 0,
    impressions: 0,
    clicks: 0,
    settledConversions: 0,
    settledRevenueUsd: 0,
    ctr: 0,
    fillRate: 0,
    ecpm: 0,
    cpa: 0,
  }
}

function finalizeSettlementAggregateRow(row) {
  const requests = toPositiveInteger(row?.requests, 0)
  const served = toPositiveInteger(row?.served, 0)
  const impressions = toPositiveInteger(row?.impressions, 0)
  const clicks = toPositiveInteger(row?.clicks, 0)
  const settledConversions = toPositiveInteger(row?.settledConversions, 0)
  const settledRevenueUsd = round(clampNumber(row?.settledRevenueUsd, 0, Number.MAX_SAFE_INTEGER, 0), 4)
  const ctr = impressions > 0 ? clicks / impressions : 0
  const fillRate = requests > 0 ? served / requests : 0
  const ecpm = impressions > 0 ? (settledRevenueUsd / impressions) * 1000 : 0
  const cpa = settledConversions > 0 ? settledRevenueUsd / settledConversions : 0

  return {
    accountId: String(row?.accountId || '').trim(),
    appId: String(row?.appId || '').trim(),
    placementId: String(row?.placementId || '').trim(),
    layer: String(row?.layer || '').trim(),
    requests,
    served,
    impressions,
    clicks,
    settledConversions,
    settledRevenueUsd: round(settledRevenueUsd, 2),
    ctr: round(ctr, 4),
    fillRate: round(fillRate, 4),
    ecpm: round(ecpm, 2),
    cpa: round(cpa, 2),
  }
}

function buildDecisionDimensionMap(decisionRows = []) {
  const map = new Map()
  for (const row of decisionRows) {
    const requestId = String(row?.requestId || '').trim()
    if (!requestId || map.has(requestId)) continue
    const appId = String(row?.appId || '').trim()
    map.set(requestId, {
      accountId: normalizeControlPlaneAccountId(row?.accountId || resolveAccountIdForApp(appId), ''),
      appId,
      placementId: String(row?.placementId || '').trim(),
    })
  }
  return map
}

function resolveFactDimensions(row, decisionDimensionMap = new Map()) {
  const requestId = String(row?.requestId || '').trim()
  const dimension = decisionDimensionMap.get(requestId)
  const appId = String(row?.appId || dimension?.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(
    row?.accountId || dimension?.accountId || resolveAccountIdForApp(appId),
    '',
  )
  const placementId = String(row?.placementId || dimension?.placementId || '').trim()
  return {
    accountId,
    appId,
    placementId,
  }
}

function ensureSettlementMapRow(map, key, seed) {
  if (!map.has(key)) {
    map.set(key, createSettlementAggregateRow(seed))
  }
  return map.get(key)
}

function upsertSettlementForDecision(row, maps) {
  const appId = String(row?.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(row?.accountId || resolveAccountIdForApp(appId), '')
  const placementId = String(row?.placementId || '').trim()
  const layer = layerFromPlacementKey(String(row?.placementKey || resolvePlacementKeyById(placementId, appId)))

  if (!accountId || !appId) return

  const totals = maps.totals
  const account = ensureSettlementMapRow(maps.byAccount, accountId, { accountId })
  const app = ensureSettlementMapRow(maps.byApp, `${accountId}::${appId}`, { accountId, appId })
  const placement = placementId
    ? ensureSettlementMapRow(
      maps.byPlacement,
      `${accountId}::${appId}::${placementId}`,
      { accountId, appId, placementId, layer },
    )
    : null

  const targets = placement ? [totals, account, app, placement] : [totals, account, app]
  for (const target of targets) {
    target.requests += 1
    if (String(row?.result || '') === 'served') {
      target.served += 1
      target.impressions += 1
    }
  }
}

function upsertSettlementForClick(row, maps) {
  if (!isClickEventLogRow(row)) return

  const appId = String(row?.appId || '').trim()
  const accountId = normalizeControlPlaneAccountId(row?.accountId || resolveAccountIdForApp(appId), '')
  const placementId = String(row?.placementId || '').trim()
  const layer = layerFromPlacementKey(String(row?.placementKey || resolvePlacementKeyById(placementId, appId)))

  if (!accountId || !appId) return

  const totals = maps.totals
  const account = ensureSettlementMapRow(maps.byAccount, accountId, { accountId })
  const app = ensureSettlementMapRow(maps.byApp, `${accountId}::${appId}`, { accountId, appId })
  const placement = placementId
    ? ensureSettlementMapRow(
      maps.byPlacement,
      `${accountId}::${appId}::${placementId}`,
      { accountId, appId, placementId, layer },
    )
    : null

  const targets = placement ? [totals, account, app, placement] : [totals, account, app]
  for (const target of targets) {
    target.clicks += 1
  }
}

function upsertSettlementForFact(row, maps, decisionDimensionMap) {
  const revenueUsd = conversionFactRevenueUsd(row)
  if (revenueUsd <= 0) return

  const dimension = resolveFactDimensions(row, decisionDimensionMap)
  const accountId = dimension.accountId
  const appId = dimension.appId
  const placementId = dimension.placementId
  if (!accountId || !appId) return
  const layer = layerFromPlacementKey(String(row?.placementKey || resolvePlacementKeyById(placementId, appId)))

  const totals = maps.totals
  const account = ensureSettlementMapRow(maps.byAccount, accountId, { accountId })
  const app = ensureSettlementMapRow(maps.byApp, `${accountId}::${appId}`, { accountId, appId })
  const placement = placementId
    ? ensureSettlementMapRow(
      maps.byPlacement,
      `${accountId}::${appId}::${placementId}`,
      { accountId, appId, placementId, layer },
    )
    : null

  const targets = placement ? [totals, account, app, placement] : [totals, account, app]
  for (const target of targets) {
    target.settledConversions += 1
    target.settledRevenueUsd = round(target.settledRevenueUsd + revenueUsd, 4)
  }
}

function rankSettlementRows(rows = []) {
  return rows.sort((a, b) => {
    if (b.settledRevenueUsd !== a.settledRevenueUsd) return b.settledRevenueUsd - a.settledRevenueUsd
    if (b.settledConversions !== a.settledConversions) return b.settledConversions - a.settledConversions
    if (b.impressions !== a.impressions) return b.impressions - a.impressions
    return String(a.appId || a.accountId || a.placementId || '').localeCompare(
      String(b.appId || b.accountId || b.placementId || ''),
    )
  })
}

function computeSettlementAggregates(scope = {}, factRowsInput = null, options = {}) {
  const decisionRows = Array.isArray(options?.decisionRows)
    ? options.decisionRows
    : filterRowsByScope(state.decisionLogs, scope)
  const eventRows = Array.isArray(options?.eventRows)
    ? options.eventRows
    : filterRowsByScope(state.eventLogs, scope)
  const factRows = Array.isArray(factRowsInput)
    ? factRowsInput
    : filterRowsByScope(state.conversionFacts, scope)

  const maps = {
    totals: createSettlementAggregateRow({}),
    byAccount: new Map(),
    byApp: new Map(),
    byPlacement: new Map(),
  }
  const decisionDimensionMap = buildDecisionDimensionMap(decisionRows)

  for (const row of decisionRows) {
    upsertSettlementForDecision(row, maps)
  }
  for (const row of eventRows) {
    upsertSettlementForClick(row, maps)
  }
  for (const row of factRows) {
    upsertSettlementForFact(row, maps, decisionDimensionMap)
  }

  const byAccount = rankSettlementRows(
    Array.from(maps.byAccount.values()).map((item) => finalizeSettlementAggregateRow(item)),
  )
  const byApp = rankSettlementRows(
    Array.from(maps.byApp.values()).map((item) => finalizeSettlementAggregateRow(item)),
  )
  const byPlacement = rankSettlementRows(
    Array.from(maps.byPlacement.values()).map((item) => finalizeSettlementAggregateRow(item)),
  )

  return {
    settlementModel: 'MIXED_CPA_CPC',
    currency: 'USD',
    totals: finalizeSettlementAggregateRow(maps.totals),
    byAccount,
    byApp,
    byPlacement,
  }
}

async function getDashboardStatePayload(scopeInput = {}) {
  await ensureSettlementStoreReady()
  const scope = normalizeScopeFilters(scopeInput)
  const hasScope = scopeHasFilters(scope)
  const networkHealth = getAllNetworkHealth()
  const scopedApps = getScopedApps(scope)
  const hasScopedApps = scopedApps.length > 0
  const shouldApplyScope = hasScope && hasScopedApps
  const emptyScoped = hasScope && !hasScopedApps
  const dataScope = shouldApplyScope ? scope : {}

  const decisionLogs = emptyScoped
    ? []
    : await listDecisionLogs(dataScope)
  const eventLogs = emptyScoped
    ? []
    : await listEventLogs(dataScope)
  const conversionFacts = emptyScoped
    ? []
    : await listConversionFacts(dataScope)
  const controlPlaneAuditLogs = emptyScoped
    ? []
    : (shouldApplyScope ? filterRowsByScope(state.controlPlaneAuditLogs, scope) : state.controlPlaneAuditLogs)
  const networkFlowLogs = emptyScoped
    ? []
    : (shouldApplyScope ? filterRowsByScope(state.networkFlowLogs, scope) : state.networkFlowLogs)

  const metricsSummary = computeScopedMetricsSummary(
    decisionLogs,
    eventLogs,
    conversionFacts,
    controlPlaneAuditLogs,
  )
  const metricsByDay = computeScopedMetricsByDay(decisionLogs, eventLogs, conversionFacts)
  const metricsByPlacement = emptyScoped
    ? []
    : computeScopedMetricsByPlacement(decisionLogs, eventLogs, conversionFacts, scope)
  const networkFlowStats = emptyScoped
    ? createInitialNetworkFlowStats()
    : shouldApplyScope
    ? computeScopedNetworkFlowStats(networkFlowLogs)
    : state.networkFlowStats
  const settlementAggregates = emptyScoped
    ? computeSettlementAggregates({ appId: '__none__', accountId: '__none__' }, [])
    : computeSettlementAggregates(shouldApplyScope ? scope : {}, conversionFacts, {
      decisionRows: decisionLogs,
      eventRows: eventLogs,
    })
  const placementScope = emptyScoped
    ? { appId: '', placements: [] }
    : getPlacementsForScope(scope, { createIfMissing: false, clone: true })
  const placementConfigVersion = emptyScoped
    ? 1
    : resolvePlacementConfigVersionForScope(scope, placementScope.appId)
  const placementAuditLogs = emptyScoped
    ? []
    : (shouldApplyScope ? filterRowsByScope(state.placementAuditLogs, scope) : [...state.placementAuditLogs])
  const filteredPlacementAudits = placementAuditLogs.filter((row) => {
    const rowAppId = String(row?.appId || '').trim()
    if (!placementScope.appId) return true
    if (!rowAppId) return placementScope.appId === DEFAULT_CONTROL_PLANE_APP_ID
    return rowAppId === placementScope.appId
  })

  return {
    scope,
    placementConfigVersion,
    metricsSummary,
    metricsByDay,
    metricsByPlacement,
    settlementAggregates,
    placements: placementScope.placements,
    placementAuditLogs: filteredPlacementAudits,
    controlPlaneAuditLogs,
    controlPlaneApps: shouldApplyScope ? scopedApps : state.controlPlane.apps,
    networkHealth,
    networkHealthSummary: summarizeNetworkHealthMap(networkHealth),
    networkFlowStats,
    networkFlowLogs,
    decisionLogs,
    eventLogs,
  }
}

function resolveMediationConfigSnapshot(query = {}) {
  const appId = requiredNonEmptyString(query.appId, 'appId')
  const placementId = assertPlacementIdNotRenamed(
    requiredNonEmptyString(query.placementId, 'placementId'),
    'placementId',
  )
  const environment = normalizeControlPlaneEnvironment(query.environment || 'prod', 'prod')
  const schemaVersion = requiredNonEmptyString(query.schemaVersion, 'schemaVersion')
  const sdkVersion = requiredNonEmptyString(query.sdkVersion, 'sdkVersion')
  const requestAt = requiredNonEmptyString(query.requestAt, 'requestAt')
  const ifNoneMatch = String(query.ifNoneMatch || query.if_none_match || '').trim()

  const placements = getPlacementsForApp(appId, resolveAccountIdForApp(appId), {
    createIfMissing: false,
    clone: false,
  })
  const placement = placements.find((item) => item.placementId === placementId)
  if (!placement) {
    const error = new Error(`placementId not found: ${placementId}`)
    error.code = 'PLACEMENT_NOT_FOUND'
    error.statusCode = 404
    throw error
  }

  const etag = `W/"placement:${appId}:${placement.placementId}:v${placement.configVersion}"`
  if (ifNoneMatch && ifNoneMatch === etag) {
    return {
      statusCode: 304,
      payload: null,
      etag,
    }
  }

  return {
    statusCode: 200,
    etag,
    payload: {
      appId,
      accountId: resolveAccountIdForApp(appId),
      environment,
      placementId: placement.placementId,
      placementKey: placement.placementKey,
      schemaVersion,
      sdkVersion,
      requestAt,
      configVersion: placement.configVersion,
      ttlSec: 300,
      placement,
    },
  }
}

function buildQuickStartVerifyRequest(input = {}) {
  const appId = requiredNonEmptyString(input.appId, 'appId')
  const accountId = normalizeControlPlaneAccountId(
    requiredNonEmptyString(input.accountId || input.account_id, 'accountId'),
    '',
  )
  const environment = normalizeControlPlaneEnvironment(input.environment || 'prod')
  if (String(input.placementId || '').trim()) {
    const error = new Error('placementId is no longer accepted in quick-start verify. Configure default placement in Dashboard.')
    error.code = 'QUICKSTART_PLACEMENT_ID_NOT_ALLOWED'
    error.statusCode = 400
    throw error
  }
  const placement = pickPlacementForRequest({
    appId,
    accountId,
    event: 'answer_completed',
  })
  const placementId = String(placement?.placementId || '').trim() || PLACEMENT_ID_FROM_ANSWER
  return {
    appId,
    accountId,
    environment,
    placementId,
    sessionId: String(input.sessionId || '').trim() || `quickstart_session_${randomToken(8)}`,
    turnId: String(input.turnId || '').trim() || `quickstart_turn_${randomToken(8)}`,
    query: String(input.query || '').trim() || 'Recommend waterproof running shoes',
    answerText: String(input.answerText || '').trim() || 'Prioritize grip and breathable waterproof upper.',
    intentScore: clampNumber(input.intentScore, 0, 1, 0.91),
    locale: String(input.locale || '').trim() || 'en-US',
  }
}

function findActiveApiKeyBySecretFromState(secret) {
  const value = String(secret || '').trim()
  if (!value) return null
  const digest = hashToken(value)
  const rows = Array.isArray(state?.controlPlane?.apiKeys) ? state.controlPlane.apiKeys : []
  const matched = rows.filter((item) => (
    String(item?.status || '').toLowerCase() === 'active'
    && String(item?.secretHash || '') === digest
  ))
  matched.sort((a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')))
  return matched[0] || null
}

async function findActiveApiKeyBySecret(secret) {
  const current = findActiveApiKeyBySecretFromState(secret)
  if (current || !isSupabaseSettlementStore()) return current

  await refreshControlPlaneStateFromStore({ force: true }).catch(() => {})
  return findActiveApiKeyBySecretFromState(secret)
}

function findActiveApiKeyFromState({ appId, accountId = '', environment, keyId = '' }) {
  const normalizedAppId = String(appId || '').trim()
  const normalizedAccountId = normalizeControlPlaneAccountId(accountId, '')
  const normalizedEnvironment = normalizeControlPlaneEnvironment(environment)
  const normalizedKeyId = String(keyId || '').trim()

  let rows = state.controlPlane.apiKeys.filter((item) => (
    item.appId === normalizedAppId
    && item.environment === normalizedEnvironment
    && item.status === 'active'
  ))

  if (normalizedKeyId) {
    rows = rows.filter((item) => item.keyId === normalizedKeyId)
  }
  if (normalizedAccountId) {
    rows = rows.filter((item) => normalizeControlPlaneAccountId(item.accountId || resolveAccountIdForApp(item.appId), '') === normalizedAccountId)
  }

  rows.sort((a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')))
  return rows[0] || null
}

async function findActiveApiKey(input = {}) {
  const current = findActiveApiKeyFromState(input)
  if (current || !isSupabaseSettlementStore()) return current

  await refreshControlPlaneStateFromStore({ force: true }).catch(() => {})
  return findActiveApiKeyFromState(input)
}

async function ensureBootstrapApiKeyForScope({
  appId,
  accountId = '',
  environment = 'prod',
  actor = 'bootstrap',
} = {}) {
  if (STRICT_MANUAL_INTEGRATION) return null

  const normalizedAppId = String(appId || '').trim()
  const normalizedAccountId = normalizeControlPlaneAccountId(accountId || resolveAccountIdForApp(normalizedAppId), '')
  const normalizedEnvironment = normalizeControlPlaneEnvironment(environment)
  if (!normalizedAppId || !normalizedAccountId) return null

  const existing = await findActiveApiKey({
    appId: normalizedAppId,
    accountId: normalizedAccountId,
    environment: normalizedEnvironment,
  })
  if (existing) return existing

  const { keyRecord } = createControlPlaneKeyRecord({
    appId: normalizedAppId,
    accountId: normalizedAccountId,
    environment: normalizedEnvironment,
    keyName: `bootstrap-${normalizedEnvironment}`,
  })

  if (isSupabaseSettlementStore()) {
    await upsertControlPlaneKeyToSupabase(keyRecord)
  }
  upsertControlPlaneStateRecord('apiKeys', 'keyId', keyRecord)
  recordControlPlaneAudit({
    action: 'key_create',
    actor,
    accountId: keyRecord.accountId,
    appId: keyRecord.appId,
    environment: keyRecord.environment,
    resourceType: 'api_key',
    resourceId: keyRecord.keyId,
    metadata: {
      keyName: keyRecord.keyName,
      status: keyRecord.status,
      bootstrap: true,
    },
  })
  return keyRecord
}

function hasRequiredAgentScope(scope, requiredScope) {
  if (!requiredScope) return true
  if (!scope || typeof scope !== 'object') return false
  return scope[requiredScope] === true
}

function getExchangeForbiddenFields(payload) {
  if (!payload || typeof payload !== 'object') return []
  return [...TOKEN_EXCHANGE_FORBIDDEN_FIELDS].filter((key) => Object.prototype.hasOwnProperty.call(payload, key))
}

async function issueDashboardSession(user, options = {}) {
  const inputUser = normalizeDashboardUserRecord(user)
  if (!inputUser) {
    throw new Error('dashboard user is invalid.')
  }
  cleanupExpiredDashboardSessions()
  const { sessionRecord, accessToken } = createDashboardSessionRecord({
    userId: inputUser.userId,
    email: inputUser.email,
    accountId: inputUser.accountId,
    appId: inputUser.appId,
    ttlSeconds: options.ttlSeconds,
    metadata: options.metadata,
  })
  if (isSupabaseSettlementStore()) {
    await upsertDashboardSessionToSupabase(sessionRecord)
  }
  upsertControlPlaneStateRecord('dashboardSessions', 'sessionId', sessionRecord, MAX_DASHBOARD_SESSIONS)
  return {
    sessionRecord,
    accessToken,
  }
}

async function revokeDashboardSessionByToken(accessToken) {
  const session = await findDashboardSessionByPlaintext(accessToken)
  if (!session) return null
  if (String(session.status || '').toLowerCase() !== 'revoked') {
    const revokedAt = nowIso()
    const nextSession = {
      ...session,
      status: 'revoked',
      revokedAt,
      updatedAt: revokedAt,
    }
    if (isSupabaseSettlementStore()) {
      await upsertDashboardSessionToSupabase(nextSession)
    }
    Object.assign(session, nextSession)
  }
  upsertControlPlaneStateRecord('dashboardSessions', 'sessionId', session, MAX_DASHBOARD_SESSIONS)
  return session
}

function recordSecurityDenyAudit({
  req,
  action,
  reason,
  code,
  httpStatus,
  accountId = '',
  appId = '',
  environment = '',
  resourceType = '',
  resourceId = '',
  metadata = {},
}) {
  recordControlPlaneAudit({
    action,
    actor: resolveAuditActor(req, 'security'),
    accountId: normalizeControlPlaneAccountId(accountId || resolveAccountIdForApp(appId), ''),
    appId: String(appId || '').trim(),
    environment: String(environment || '').trim(),
    resourceType: String(resourceType || '').trim(),
    resourceId: String(resourceId || '').trim(),
    metadata: {
      reason: String(reason || '').trim(),
      code: String(code || '').trim(),
      httpStatus: Number(httpStatus || 0),
      ...metadata,
    },
  })
}

async function resolveRuntimeCredential(req) {
  const token = parseBearerToken(req)
  if (!token) {
    return { kind: 'none' }
  }

  if (token.startsWith('sk_')) {
    const key = await findActiveApiKeyBySecret(token)
    if (!key) {
      return {
        kind: 'invalid',
        status: 401,
        code: 'INVALID_API_KEY',
        message: 'API key is invalid or revoked.',
      }
    }
    return {
      kind: 'api_key',
      key,
    }
  }

  if (token.startsWith('atk_')) {
    cleanupExpiredAgentAccessTokens()
    const access = await findAgentAccessTokenByPlaintext(token)
    if (!access) {
      return {
        kind: 'invalid',
        status: 401,
        code: 'INVALID_ACCESS_TOKEN',
        message: 'Agent access token is invalid.',
      }
    }

    const status = String(access.status || '').trim().toLowerCase()
    if (status !== 'active') {
      return {
        kind: 'invalid',
        status: 401,
        code: status === 'expired' ? 'ACCESS_TOKEN_EXPIRED' : 'ACCESS_TOKEN_INACTIVE',
        message: status === 'expired'
          ? 'Agent access token has expired.'
          : `Agent access token is not active (${status || 'unknown'}).`,
        access,
      }
    }

    const expiresAtMs = Date.parse(String(access.expiresAt || ''))
    if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
      const expiredAccess = {
        ...access,
        status: 'expired',
        updatedAt: nowIso(),
      }
      if (isSupabaseSettlementStore()) {
        await upsertAgentAccessTokenToSupabase(expiredAccess)
      }
      Object.assign(access, expiredAccess)
      upsertControlPlaneStateRecord('agentAccessTokens', 'tokenId', access, MAX_AGENT_ACCESS_TOKENS)
      persistState(state)
      return {
        kind: 'invalid',
        status: 401,
        code: 'ACCESS_TOKEN_EXPIRED',
        message: 'Agent access token has expired.',
        access,
      }
    }

    return {
      kind: 'agent_access_token',
      access,
    }
  }

  if (token.startsWith('itk_')) {
    return {
      kind: 'invalid',
      status: 401,
      code: 'UNSUPPORTED_TOKEN_TYPE',
      message: 'integration token cannot be used for runtime API calls.',
    }
  }

  return {
    kind: 'invalid',
    status: 401,
    code: 'UNSUPPORTED_BEARER_TOKEN',
    message: 'Unsupported bearer token.',
  }
}

async function authorizeRuntimeCredential(req, options = {}) {
  const requirement = options && typeof options === 'object' ? options : {}
  const allowAnonymous = requirement.allowAnonymous === true
  const requiredScope = String(requirement.requiredScope || '').trim()
  const requiredAppId = String(requirement.appId || '').trim()
  const requiredEnvironment = String(requirement.environment || '').trim()
  const requiredPlacementId = String(requirement.placementId || '').trim()
  const operation = String(requirement.operation || '').trim() || 'runtime_call'

  const resolved = await resolveRuntimeCredential(req)
  if (resolved.kind === 'none') {
    if (allowAnonymous || !RUNTIME_AUTH_REQUIRED) {
      return { ok: true, mode: 'anonymous' }
    }
    return {
      ok: false,
      status: 401,
      error: {
        code: 'RUNTIME_AUTH_REQUIRED',
        message: 'Runtime authentication is required.',
      },
    }
  }

  if (resolved.kind === 'invalid') {
    if (resolved.access) {
      recordSecurityDenyAudit({
        req,
        action: 'agent_access_deny',
        reason: 'invalid_or_expired_token',
        code: resolved.code,
        httpStatus: resolved.status,
        appId: resolved.access.appId,
        environment: resolved.access.environment,
        resourceType: 'agent_access_token',
        resourceId: resolved.access.tokenId,
        metadata: {
          operation,
        },
      })
      persistState(state)
    }
    return {
      ok: false,
      status: resolved.status,
      error: {
        code: resolved.code,
        message: resolved.message,
      },
    }
  }

  if (resolved.kind === 'api_key') {
    const key = resolved.key
    if (requiredAppId && key.appId && key.appId !== requiredAppId) {
      return {
        ok: false,
        status: 403,
        error: {
          code: 'API_KEY_SCOPE_VIOLATION',
          message: 'API key does not match requested appId.',
        },
      }
    }
    if (requiredEnvironment && key.environment && key.environment !== normalizeControlPlaneEnvironment(requiredEnvironment, '')) {
      return {
        ok: false,
        status: 403,
        error: {
          code: 'API_KEY_SCOPE_VIOLATION',
          message: 'API key does not match requested environment.',
        },
      }
    }
    const touchedKey = {
      ...key,
      lastUsedAt: nowIso(),
    }
    touchedKey.updatedAt = touchedKey.lastUsedAt
    if (isSupabaseSettlementStore()) {
      await upsertControlPlaneKeyToSupabase(touchedKey)
    }
    Object.assign(key, touchedKey)
    upsertControlPlaneStateRecord('apiKeys', 'keyId', key)
    persistState(state)
    return { ok: true, mode: 'api_key', credential: key }
  }

  const access = resolved.access
  if (!hasRequiredAgentScope(access.scope, requiredScope)) {
    recordSecurityDenyAudit({
      req,
      action: 'agent_access_deny',
      reason: 'scope_missing',
      code: 'ACCESS_TOKEN_SCOPE_VIOLATION',
      httpStatus: 403,
      appId: access.appId,
      environment: access.environment,
      resourceType: 'agent_access_token',
      resourceId: access.tokenId,
      metadata: {
        operation,
        requiredScope,
      },
    })
    persistState(state)
    return {
      ok: false,
      status: 403,
      error: {
        code: 'ACCESS_TOKEN_SCOPE_VIOLATION',
        message: `Missing required scope: ${requiredScope}`,
      },
    }
  }

  if (requiredAppId && access.appId && access.appId !== requiredAppId) {
    recordSecurityDenyAudit({
      req,
      action: 'agent_access_deny',
      reason: 'app_mismatch',
      code: 'ACCESS_TOKEN_SCOPE_VIOLATION',
      httpStatus: 403,
      appId: access.appId,
      environment: access.environment,
      resourceType: 'agent_access_token',
      resourceId: access.tokenId,
      metadata: {
        operation,
        requiredAppId,
      },
    })
    persistState(state)
    return {
      ok: false,
      status: 403,
      error: {
        code: 'ACCESS_TOKEN_SCOPE_VIOLATION',
        message: 'Agent access token does not match requested appId.',
      },
    }
  }

  if (
    requiredEnvironment
    && access.environment
    && access.environment !== normalizeControlPlaneEnvironment(requiredEnvironment, '')
  ) {
    recordSecurityDenyAudit({
      req,
      action: 'agent_access_deny',
      reason: 'environment_mismatch',
      code: 'ACCESS_TOKEN_SCOPE_VIOLATION',
      httpStatus: 403,
      appId: access.appId,
      environment: access.environment,
      resourceType: 'agent_access_token',
      resourceId: access.tokenId,
      metadata: {
        operation,
        requiredEnvironment,
      },
    })
    persistState(state)
    return {
      ok: false,
      status: 403,
      error: {
        code: 'ACCESS_TOKEN_SCOPE_VIOLATION',
        message: 'Agent access token does not match requested environment.',
      },
    }
  }

  if (requiredPlacementId && access.placementId && access.placementId !== requiredPlacementId) {
    recordSecurityDenyAudit({
      req,
      action: 'agent_access_deny',
      reason: 'placement_mismatch',
      code: 'ACCESS_TOKEN_SCOPE_VIOLATION',
      httpStatus: 403,
      appId: access.appId,
      environment: access.environment,
      resourceType: 'agent_access_token',
      resourceId: access.tokenId,
      metadata: {
        operation,
        requiredPlacementId,
      },
    })
    persistState(state)
    return {
      ok: false,
      status: 403,
      error: {
        code: 'ACCESS_TOKEN_SCOPE_VIOLATION',
        message: 'Agent access token does not match requested placementId.',
      },
    }
  }

  const touchedAccess = {
    ...access,
    updatedAt: nowIso(),
    metadata: access.metadata && typeof access.metadata === 'object' ? { ...access.metadata } : {},
  }
  touchedAccess.metadata.lastUsedAt = touchedAccess.updatedAt
  if (isSupabaseSettlementStore()) {
    await upsertAgentAccessTokenToSupabase(touchedAccess)
  }
  Object.assign(access, touchedAccess)
  upsertControlPlaneStateRecord('agentAccessTokens', 'tokenId', access, MAX_AGENT_ACCESS_TOKENS)
  persistState(state)
  return { ok: true, mode: 'agent_access_token', credential: access }
}

function applyRuntimeCredentialScope(request, auth, options = {}) {
  const target = request && typeof request === 'object' ? request : {}
  const credential = auth?.credential && typeof auth.credential === 'object' ? auth.credential : {}
  const scopedAppId = String(credential.appId || '').trim()
  const scopedAccountId = normalizeControlPlaneAccountId(
    credential.accountId || credential.organizationId || (scopedAppId ? resolveAccountIdForApp(scopedAppId) : ''),
    '',
  )
  const scopedEnvironment = normalizeControlPlaneEnvironment(credential.environment, '')
  const scopedPlacementId = String(credential.placementId || '').trim()
  const applyEnvironment = options && options.applyEnvironment === true

  if (scopedAppId) {
    target.appId = scopedAppId
  }
  if (!String(target.appId || '').trim()) {
    throw new Error('runtime credential missing app scope.')
  }

  if (scopedAccountId) {
    target.accountId = scopedAccountId
  }
  if (!String(target.accountId || '').trim()) {
    target.accountId = normalizeControlPlaneAccountId(resolveAccountIdForApp(target.appId), '')
  }

  if (applyEnvironment && scopedEnvironment) {
    target.environment = scopedEnvironment
  }

  if (scopedPlacementId) {
    const requestedPlacementId = String(target.placementId || '').trim()
    if (requestedPlacementId && requestedPlacementId !== scopedPlacementId) {
      throw new Error('placementId is outside runtime credential scope.')
    }
    target.placementId = scopedPlacementId
  }

  return target
}

async function recordAttachSdkEvent(request) {
  await recordEvent({
    eventType: 'sdk_event',
    requestId: request.requestId || '',
    appId: request.appId,
    accountId: normalizeControlPlaneAccountId(request.accountId || resolveAccountIdForApp(request.appId), ''),
    sessionId: request.sessionId,
    turnId: request.turnId,
    query: request.query,
    answerText: request.answerText,
    intentScore: request.intentScore,
    locale: request.locale,
    event: ATTACH_MVP_EVENT,
    placementKey: ATTACH_MVP_PLACEMENT_KEY,
  })
}

function createRuntimeRouteDeps() {
  return {
    state,
    sendJson,
    withCors,
    assertPlacementIdNotRenamed,
    authorizeRuntimeCredential,
    applyRuntimeCredentialScope,
    resolveMediationConfigSnapshot,
    requiredNonEmptyString,
    getPlacementsForApp,
    resolveAccountIdForApp,
    readJsonBody,
    normalizeIntentCardRetrievePayload,
    createIntentCardVectorIndex,
    retrieveIntentCardTopK,
    createId,
    normalizeV2BidPayload,
    DEFAULT_CONTROL_PLANE_APP_ID,
    normalizeControlPlaneAccountId,
    evaluateV2BidRequest,
    nowIso,
    createOpportunityChainWriter,
    isPostbackConversionPayload,
    normalizePostbackConversionPayload,
    normalizePlacementIdWithMigration,
    findPlacementIdByRequestId,
    PLACEMENT_ID_FROM_ANSWER,
    recordConversionFact,
    recordClickRevenueFactFromBid,
    findPricingSnapshotByRequestId,
    recordEvent,
    isNextStepIntentCardPayload,
    normalizeNextStepIntentCardPayload,
    clampNumber,
    normalizeNextStepPreferenceFacets,
    PLACEMENT_ID_INTENT_RECOMMENDATION,
    recordClickCounters,
    normalizeAttachMvpPayload,
    ATTACH_MVP_EVENT,
    ATTACH_MVP_PLACEMENT_KEY,
    pickPlacementForRequest,
    persistState,
    round,
  }
}

function createControlPlaneRouteDeps() {
  return {
    state,
    settlementStore,
    STRICT_MANUAL_INTEGRATION,
    MAX_DASHBOARD_USERS,
    MAX_INTEGRATION_TOKENS,
    MAX_AGENT_ACCESS_TOKENS,
    MIN_AGENT_ACCESS_TTL_SECONDS,
    MAX_AGENT_ACCESS_TTL_SECONDS,
    CONTROL_PLANE_ENVIRONMENTS,
    CONTROL_PLANE_KEY_STATUS,
    DEFAULT_CONTROL_PLANE_APP_ID,
    PLACEMENT_ID_FROM_ANSWER,
    sendJson,
    readJsonBody,
    isSupabaseSettlementStore,
    loadControlPlaneStateFromSupabase,
    nowIso,
    buildQuickStartVerifyRequest,
    findActiveApiKey,
    resolveMediationConfigSnapshot,
    evaluateV2BidRequest,
    recordAttachSdkEvent,
    persistState,
    clampNumber,
    queryControlPlaneAudits,
    normalizeDashboardRegisterPayload,
    findDashboardUserByEmail,
    validateDashboardRegisterOwnership,
    findLatestAppForAccount,
    ensureControlPlaneAppAndEnvironment,
    appBelongsToAccountReadThrough,
    ensureBootstrapApiKeyForScope,
    resolveAuditActor,
    createDashboardUserRecord,
    upsertDashboardUserToSupabase,
    upsertControlPlaneStateRecord,
    issueDashboardSession,
    toPublicDashboardUserRecord,
    toPublicDashboardSessionRecord,
    normalizeDashboardLoginPayload,
    verifyPasswordRecord,
    authorizeDashboardScope,
    resolveDashboardSession,
    revokeDashboardSessionByToken,
    resolveAuthorizedDashboardAccount,
    validateDashboardAccountOwnership,
    validateDashboardAppOwnership,
    toPositiveInteger,
    normalizePlacementIdWithMigration,
    cleanupExpiredIntegrationTokens,
    createIntegrationTokenRecord,
    upsertIntegrationTokenToSupabase,
    recordControlPlaneAudit,
    toPublicIntegrationTokenRecord,
    cleanupExpiredAgentAccessTokens,
    getExchangeForbiddenFields,
    findIntegrationTokenByPlaintext,
    recordSecurityDenyAudit,
    tokenFingerprint,
    requiredNonEmptyString,
    hasRequiredAgentScope,
    createMinimalAgentScope,
    createAgentAccessTokenRecord,
    resolveAccountIdForApp,
    upsertAgentAccessTokenToSupabase,
    toPublicAgentAccessTokenRecord,
    normalizeControlPlaneAccountId,
    toPublicApiKeyRecord,
    createControlPlaneKeyRecord,
    upsertControlPlaneKeyToSupabase,
    getDashboardStatePayload,
    scopeHasFilters,
    getScopedApps,
    resolvePlacementScopeAppId,
    getPlacementConfigForApp,
    normalizePlacement,
    assertPlacementIdNotRenamed,
    resolvePlacementKeyById,
    buildPlacementFromPatch,
    syncLegacyPlacementSnapshot,
    recordPlacementAudit,
    applyPlacementPatch,
    listDecisionLogs,
    listEventLogs,
    recordMatchesScope,
    filterRowsByScope,
    getAllNetworkHealth,
    summarizeNetworkHealthMap,
    computeScopedNetworkFlowStats,
    isPostgresSettlementStore,
    getInventoryStatus,
    summarizeInventoryReadiness,
    INVENTORY_SYNC_COMMAND,
    syncInventoryNetworks,
    buildInventoryEmbeddings,
    materializeServingSnapshot,
    listAllowedCorsOriginsFromSupabase,
    replaceAllowedCorsOriginsInSupabase,
    refreshAllowedCorsOriginsFromSupabase,
    normalizeAllowedCorsOriginsPayload,
    getAllowedCorsOrigins,
    upsertCampaignBudgetConfig,
    listCampaignBudgetStatuses,
    getRiskConfigSnapshot,
    updateRiskConfig,
    cleanupExpiredBudgetReservations,
    BUDGET_ENFORCEMENT_MODE,
    RISK_ENFORCEMENT_MODE,
  }
}

export async function requestHandler(req, res, options = {}) {
  const requestUrl = new URL(req.url || '/', REQUEST_BASE_ORIGIN)
  const pathname = requestUrl.pathname
  const apiServiceRole = normalizeApiServiceRole(options?.apiServiceRole || API_SERVICE_ROLE)

  if (req.method === 'OPTIONS') {
    withCors(res)
    res.statusCode = 204
    res.end()
    return
  }

  if (!isRouteAllowedForServiceRole(pathname, apiServiceRole)) {
    sendNotFound(res)
    return
  }

  if ((pathname === '/' || pathname === '/health' || pathname === '/api/health') && req.method === 'GET') {
    sendJson(res, 200, {
      ok: true,
      service: 'mediation-api',
      apiServiceRole,
      updatedAt: state.updatedAt,
      now: nowIso(),
    })
    return
  }

  await refreshControlPlaneStateFromStore().catch((error) => {
    console.error(
      '[mediation-gateway] control plane state refresh warning:',
      error instanceof Error ? error.message : String(error),
    )
  })

  const routeContext = {
    req,
    res,
    pathname,
    requestUrl,
  }

  if (await handleRuntimeRoutes(routeContext, createRuntimeRouteDeps())) {
    return
  }

  if (await handleControlPlaneRoutes(routeContext, createControlPlaneRouteDeps())) {
    return
  }

  sendNotFound(res)
}

function sendInternalError(res, error) {
  console.error('[mediation-gateway] unhandled error:', error)
  sendJson(res, 500, {
    error: {
      code: 'INTERNAL_ERROR',
      message: 'Internal server error',
    },
  })
}

function verifyGatewayReadiness() {
  if (REQUIRE_DURABLE_SETTLEMENT && !isPostgresSettlementStore()) {
    throw new Error('durable settlement is required but supabase store is unavailable.')
  }
  if (REQUIRE_RUNTIME_LOG_DB_PERSISTENCE && !isPostgresSettlementStore()) {
    throw new Error('runtime log DB persistence is required but supabase store is unavailable.')
  }
}

let readyPromise = null

export async function ensureReady() {
  if (!readyPromise) {
    readyPromise = (async () => {
      assertProductionGatewayConfig()
      await ensureSettlementStoreReady()
      verifyGatewayReadiness()
      return true
    })().catch((error) => {
      readyPromise = null
      throw error
    })
  }
  await readyPromise
}

export async function handleGatewayRequest(req, res, options = {}) {
  try {
    await ensureReady()
    const hasOriginHeader = Boolean(normalizeCorsOrigin(req?.headers?.origin || ''))
    if (hasOriginHeader) {
      await refreshControlPlaneStateFromStore().catch((error) => {
        console.error(
          '[mediation-gateway] runtime refresh warning:',
          error instanceof Error ? error.message : String(error),
        )
      })
    }
    const corsCheck = applyCorsOrigin(req, res)
    if (!corsCheck.ok) {
      sendCorsForbidden(res, corsCheck.requestOrigin)
      return
    }
    await requestHandler(req, res, options)
  } catch (error) {
    sendInternalError(res, error)
  }
}

export {
  computeScopedMetricsSummary,
  migrateLegacyPlacementIdsInSupabase,
  normalizeAgentAccessTokenRecord,
  normalizeIntegrationTokenRecord,
  normalizePlacementIdWithMigration,
  summarizeInventoryReadiness,
}

function isDirectExecution() {
  const entry = String(process.argv?.[1] || '').trim()
  if (!entry) return false
  return entry === fileURLToPath(import.meta.url)
}

function resolveLocalListenHost() {
  return String(process.env.MEDIATION_GATEWAY_HOST || '').trim() || '127.0.0.1'
}

function resolveLocalListenPort() {
  const raw = Number(process.env.MEDIATION_GATEWAY_PORT)
  if (!Number.isFinite(raw) || raw <= 0) return 3100
  return Math.floor(raw)
}

async function startLocalServer() {
  await ensureReady()
  const host = resolveLocalListenHost()
  const port = resolveLocalListenPort()
  const server = http.createServer((req, res) => {
    void handleGatewayRequest(req, res, {
      apiServiceRole: API_SERVICE_ROLE,
    })
  })

  server.listen(port, host, () => {
    console.log(`[mediation-gateway] local server listening on http://${host}:${port}`)
  })
}

if (isDirectExecution() && String(process.env.MEDIATION_ENABLE_LOCAL_SERVER || '').trim().toLowerCase() === 'true') {
  startLocalServer().catch((error) => {
    console.error('[mediation-gateway] startup failure:', error)
    process.exit(1)
  })
}
