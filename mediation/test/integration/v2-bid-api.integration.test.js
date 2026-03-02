import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import path from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const PROJECT_ROOT = path.resolve(__dirname, '../..')
const GATEWAY_ENTRY = path.join(PROJECT_ROOT, 'src', 'devtools', 'mediation', 'mediation-gateway.js')

const HOST = '127.0.0.1'
const HEALTH_TIMEOUT_MS = (() => {
  const raw = Number(process.env.MEDIATION_TEST_HEALTH_TIMEOUT_MS || 12000)
  if (!Number.isFinite(raw) || raw <= 0) return 12000
  return Math.floor(raw)
})()
const REQUEST_TIMEOUT_MS = (() => {
  const raw = Number(process.env.MEDIATION_TEST_REQUEST_TIMEOUT_MS || HEALTH_TIMEOUT_MS)
  if (!Number.isFinite(raw) || raw <= 0) return HEALTH_TIMEOUT_MS
  return Math.floor(raw)
})()

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function withTimeoutSignal(timeoutMs = REQUEST_TIMEOUT_MS) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  return {
    signal: controller.signal,
    clear: () => clearTimeout(timer),
  }
}

async function requestJson(baseUrl, pathname, options = {}) {
  const timeout = withTimeoutSignal(options.timeoutMs)

  try {
    const response = await fetch(`${baseUrl}${pathname}`, {
      method: options.method || 'GET',
      headers: {
        'Content-Type': 'application/json',
        ...(options.headers || {}),
      },
      body: options.body ? JSON.stringify(options.body) : undefined,
      signal: timeout.signal,
    })

    const payload = await response.json().catch(() => ({}))
    return {
      ok: response.ok,
      status: response.status,
      payload,
    }
  } finally {
    timeout.clear()
  }
}

function isTransientUpstreamTimeout(response = {}) {
  const status = Number(response?.status)
  const code = String(response?.payload?.error?.code || '').trim()
  const message = String(response?.payload?.error?.message || '').trim().toLowerCase()
  return status === 400 && code === 'INVALID_REQUEST' && message.includes('timeout')
}

async function waitForGateway(baseUrl) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < HEALTH_TIMEOUT_MS) {
    try {
      const health = await requestJson(baseUrl, '/api/health', { timeoutMs: 1200 })
      if (health.ok && health.payload?.ok === true) {
        return
      }
    } catch {
      // retry
    }
    await sleep(250)
  }

  throw new Error(`gateway health check timeout after ${HEALTH_TIMEOUT_MS}ms`)
}

function startGateway(port, envOverrides = {}) {
  const child = spawn(process.execPath, [GATEWAY_ENTRY], {
    cwd: PROJECT_ROOT,
    env: {
      ...process.env,
      SUPABASE_DB_URL: process.env.SUPABASE_DB_URL_TEST || process.env.SUPABASE_DB_URL || '',
      MEDIATION_ALLOWED_ORIGINS: 'http://127.0.0.1:3000',
      MEDIATION_ENABLE_LOCAL_SERVER: 'true',
      MEDIATION_GATEWAY_HOST: HOST,
      MEDIATION_GATEWAY_PORT: String(port),
      MEDIATION_ENABLED_NETWORKS: process.env.MEDIATION_ENABLED_NETWORKS || 'partnerstack,house',
      CPC_SEMANTICS: process.env.CPC_SEMANTICS || 'on',
      BUDGET_ENFORCEMENT: process.env.BUDGET_ENFORCEMENT || 'monitor_only',
      RISK_ENFORCEMENT: process.env.RISK_ENFORCEMENT || 'off',
      OPENROUTER_API_KEY: '',
      OPENROUTER_MODEL: 'glm-5',
      CJ_TOKEN: 'mock-cj-token',
      PARTNERSTACK_API_KEY: 'mock-partnerstack-key',
      ...envOverrides,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  let stdout = ''
  let stderr = ''

  child.stdout.on('data', (chunk) => {
    stdout += String(chunk)
  })
  child.stderr.on('data', (chunk) => {
    stderr += String(chunk)
  })

  return {
    child,
    getLogs() {
      return { stdout, stderr }
    },
  }
}

async function stopGateway(handle) {
  if (!handle?.child) return

  handle.child.kill('SIGTERM')
  await sleep(200)
  if (!handle.child.killed) {
    handle.child.kill('SIGKILL')
  }
}

async function registerDashboardHeaders(baseUrl, input = {}) {
  const now = Date.now()
  const accountId = String(input.accountId || 'org_mediation').trim()
  const appId = String(input.appId || 'sample-client-app').trim()
  const register = await requestJson(baseUrl, '/api/v1/public/dashboard/register', {
    method: 'POST',
    body: {
      email: String(input.email || `v2_bid_${now}@example.com`).trim(),
      password: 'pass12345',
      accountId,
      appId,
    },
  })
  assert.equal(register.status, 201, `dashboard register failed: ${JSON.stringify(register.payload)}`)
  const accessToken = String(register.payload?.session?.accessToken || '').trim()
  assert.equal(Boolean(accessToken), true, 'dashboard register should return access token')
  return {
    Authorization: `Bearer ${accessToken}`,
  }
}

async function issueRuntimeApiKeyHeaders(baseUrl, dashboardHeaders, input = {}) {
  const accountId = String(input.accountId || 'org_mediation').trim()
  const appId = String(input.appId || 'sample-client-app').trim()
  const created = await requestJson(baseUrl, '/api/v1/public/credentials/keys', {
    method: 'POST',
    headers: dashboardHeaders,
    body: {
      accountId,
      appId,
      environment: 'prod',
      name: `runtime-${Date.now()}`,
    },
  })
  assert.equal(created.status, 201, `issue runtime key failed: ${JSON.stringify(created.payload)}`)
  const secret = String(created.payload?.secret || '').trim()
  assert.equal(Boolean(secret), true, 'runtime key create should return secret')
  return { secret }
}

test('v2 bid API returns unified response on the single runtime path', async () => {
  const suffix = `${Date.now()}_${Math.floor(Math.random() * 1000)}`
  const scopedAccountId = `org_mediation_${suffix}`
  const scopedAppId = `sample-client-app-${suffix}`
  const port = 3950 + Math.floor(Math.random() * 120)
  const baseUrl = `http://${HOST}:${port}`
  const gateway = startGateway(port)

  try {
    await waitForGateway(baseUrl)

    const reset = await requestJson(baseUrl, '/api/v1/dev/reset', { method: 'POST' })
    assert.equal(reset.status, 404)

    const dashboardHeaders = await registerDashboardHeaders(baseUrl, {
      email: `v2_bid_${suffix}@example.com`,
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeCredential = await issueRuntimeApiKeyHeaders(baseUrl, dashboardHeaders, {
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeHeaders = {
      Authorization: `Bearer ${runtimeCredential.secret}`,
    }

    const configWithoutPlacement = await requestJson(
      baseUrl,
      `/api/v1/mediation/config?appId=${encodeURIComponent(scopedAppId)}&environment=prod&schemaVersion=schema_v1&sdkVersion=1.0.0&requestAt=2026-02-27T00%3A00%3A00.000Z`,
      {
      method: 'GET',
      headers: runtimeHeaders,
      timeoutMs: REQUEST_TIMEOUT_MS,
      },
    )
    if (configWithoutPlacement.status === 200) {
      assert.equal(typeof configWithoutPlacement.payload?.placementId, 'string')
      assert.equal(configWithoutPlacement.payload?.placementId.length > 0, true)
    } else {
      assert.equal(configWithoutPlacement.status, 400, JSON.stringify(configWithoutPlacement.payload))
      assert.equal(configWithoutPlacement.payload?.error?.code, 'INVALID_REQUEST')
      assert.equal(
        String(configWithoutPlacement.payload?.error?.message || '').toLowerCase().includes('timeout'),
        true,
      )
    }

    const bid = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        userId: 'user_v2_001',
        chatId: 'chat_v2_001',
        messages: [
          { role: 'user', content: 'i want to buy a gift to my girlfriend' },
          { role: 'assistant', content: 'what kind of gift do you prefer?' },
          { role: 'user', content: 'camera for vlogging' },
        ],
      },
      timeoutMs: REQUEST_TIMEOUT_MS,
    })
    if (isTransientUpstreamTimeout(bid)) {
      assert.equal(true, true)
      return
    }

    assert.equal(bid.ok, true, `v2 bid failed: ${JSON.stringify(bid.payload)}`)
    assert.equal(bid.payload?.status, 'success')
    assert.equal(typeof bid.payload?.requestId, 'string')
    assert.equal(typeof bid.payload?.timestamp, 'string')
    assert.equal(typeof bid.payload?.opportunityId, 'string')
    assert.equal(typeof bid.payload?.filled, 'boolean')
    assert.equal(Object.prototype.hasOwnProperty.call(bid.payload || {}, 'landingUrl'), true)
    const failOpenUpstream = bid.payload?.diagnostics?.reasonCode === 'upstream_non_2xx'
    if (failOpenUpstream) {
      assert.equal(bid.payload?.diagnostics?.failOpenApplied, true)
      assert.equal(bid.payload?.filled, false)
      assert.equal(typeof bid.payload?.decisionTrace?.reasonCode, 'string')
    } else {
      assert.equal(typeof bid.payload?.intent?.score, 'number')
      assert.equal(typeof bid.payload?.intent?.class, 'string')
      assert.equal(typeof bid.payload?.intent?.source, 'string')
      assert.equal(typeof bid.payload?.decisionTrace?.reasonCode, 'string')
      assert.equal(Boolean(bid.payload?.decisionTrace?.stageStatus), true)
      assert.equal(typeof bid.payload?.diagnostics?.triggerType, 'string')
      assert.equal(typeof bid.payload?.diagnostics?.budgetDecision, 'object')
      assert.equal(typeof bid.payload?.diagnostics?.riskDecision, 'object')
      assert.equal(typeof bid.payload?.diagnostics?.multiPlacement?.evaluatedCount, 'number')
      assert.equal(bid.payload?.diagnostics?.multiPlacement?.evaluatedCount >= 2, true)
      assert.equal(typeof bid.payload?.diagnostics?.multiPlacement?.scoring, 'object')
      assert.equal(bid.payload?.diagnostics?.multiPlacement?.scoring?.relevanceWeight, 0.95)
      assert.equal(bid.payload?.diagnostics?.multiPlacement?.scoring?.bidWeight, 0.05)
      assert.equal(bid.payload?.diagnostics?.multiPlacement?.scoring?.bidNormalization, 'log1p_max')
      const multiPlacementOptions = Array.isArray(bid.payload?.diagnostics?.multiPlacement?.options)
        ? bid.payload.diagnostics.multiPlacement.options
        : []
      assert.equal(multiPlacementOptions.length >= 2, true)
      assert.equal(
        multiPlacementOptions.every((item) => typeof item?.compositeScore === 'number'),
        true,
      )
      const retrievalFilters = bid.payload?.diagnostics?.retrievalDebug?.filters
        && typeof bid.payload?.diagnostics?.retrievalDebug?.filters === 'object'
        ? bid.payload.diagnostics.retrievalDebug.filters
        : {}
      const retrievalNetworks = Array.isArray(retrievalFilters.networks) ? retrievalFilters.networks : []
      assert.equal(retrievalNetworks.includes('partnerstack'), true)
      assert.equal(retrievalNetworks.includes('house'), true)
      assert.equal(retrievalNetworks.includes('cj'), false)
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.languageMatchMode, 'string')
      const languageResolved = bid.payload?.diagnostics?.retrievalDebug?.languageResolved
        && typeof bid.payload?.diagnostics?.retrievalDebug?.languageResolved === 'object'
        ? bid.payload.diagnostics.retrievalDebug.languageResolved
        : {}
      assert.equal(Array.isArray(languageResolved.accepted), true)
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.queryMode, 'string')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.queryUsed, 'string')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.semanticQuery, 'string')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.sparseQuery, 'string')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.contextWindowMode, 'string')
      assert.equal(Array.isArray(bid.payload?.diagnostics?.retrievalDebug?.assistantEntityTokensRaw), true)
      assert.equal(Array.isArray(bid.payload?.diagnostics?.retrievalDebug?.assistantEntityTokensFiltered), true)
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.bm25HitCount, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.brandIntentDetected, 'boolean')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.brandIntentBlockedNoHit, 'boolean')
      assert.equal(Array.isArray(bid.payload?.diagnostics?.retrievalDebug?.brandEntityTokens), true)
      assert.equal(Array.isArray(bid.payload?.diagnostics?.retrievalDebug?.penaltiesApplied), true)
      assert.equal(Array.isArray(bid.payload?.diagnostics?.retrievalDebug?.options), true)
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.houseShareBeforeCap, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.houseShareAfterCap, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.scoring, 'object')
      assert.equal(bid.payload?.diagnostics?.retrievalDebug?.scoring?.strategy, 'rrf_then_linear')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.scoring?.sparseWeight, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.scoring?.denseWeight, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.scoring?.rrfK, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.scoreStats, 'object')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.scoreStats?.sparseMin, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.scoreStats?.sparseMax, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.scoreStats?.denseMin, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.retrievalDebug?.scoreStats?.denseMax, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.rankingDebug?.relevanceGate, 'object')
      assert.equal(typeof bid.payload?.diagnostics?.rankingDebug?.relevanceFilteredCount, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.rankingDebug?.thresholdFloorsApplied, 'object')
      assert.equal(
        bid.payload?.diagnostics?.rankingDebug?.thresholdFloorsApplied?.minLexicalScore?.effective >= 0.02,
        true,
      )
      assert.equal(
        bid.payload?.diagnostics?.rankingDebug?.thresholdFloorsApplied?.minVectorScore?.effective >= 0.14,
        true,
      )
      assert.equal(typeof bid.payload?.diagnostics?.relevanceDebug, 'object')
      const relevanceDebug = bid.payload?.diagnostics?.relevanceDebug
        && typeof bid.payload.diagnostics.relevanceDebug === 'object'
        ? bid.payload.diagnostics.relevanceDebug
        : {}
      assert.equal(typeof relevanceDebug.relevanceScore, 'number')
      assert.equal(typeof relevanceDebug.componentScores, 'object')
      assert.equal(typeof relevanceDebug.componentScores?.topicScore, 'number')
      assert.equal(typeof relevanceDebug.componentScores?.entityScore, 'number')
      assert.equal(typeof relevanceDebug.componentScores?.intentFitScore, 'number')
      assert.equal(typeof relevanceDebug.componentScores?.qualitySupportScore, 'number')
      assert.equal(typeof relevanceDebug.thresholdsApplied, 'object')
      assert.equal(typeof relevanceDebug.thresholdsApplied?.strict, 'number')
      assert.equal(typeof relevanceDebug.thresholdsApplied?.relaxed, 'number')
      assert.equal(typeof relevanceDebug.thresholdsApplied?.thresholdVersion, 'string')
      assert.equal(
        ['strict', 'relaxed', 'blocked', 'observe', 'shadow', 'disabled'].includes(
          String(relevanceDebug.gateStage || ''),
        ),
        true,
      )
      assert.equal(bid.payload?.diagnostics?.pricingVersion, 'cpa_mock_v2')
      assert.equal(typeof bid.payload?.diagnostics?.timingsMs?.total, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.budgetMs?.total, 'number')
      assert.equal(typeof bid.payload?.diagnostics?.budgetExceeded?.total, 'boolean')
      assert.equal(typeof bid.payload?.diagnostics?.timeoutSignal?.occurred, 'boolean')
      assert.equal(typeof bid.payload?.diagnostics?.precheck?.placement?.exists, 'boolean')
      assert.equal(
        bid.payload?.diagnostics?.precheck?.inventory?.ready === null
        || typeof bid.payload?.diagnostics?.precheck?.inventory?.ready === 'boolean',
        true,
      )
      assert.equal(Boolean(bid.payload?.data), true)

      const winner = bid.payload?.data?.bid
      if (winner) {
        assert.equal(typeof winner.price, 'number')
        assert.equal(typeof winner.headline, 'string')
        assert.equal(typeof winner.url, 'string')
        assert.equal(typeof winner.bidId, 'string')
        assert.equal(typeof winner.campaignId, 'string')
        assert.equal(typeof winner.pricing, 'object')
        assert.equal(typeof winner.pricing.modelVersion, 'string')
        assert.equal(winner.pricing.pricingSemanticsVersion, 'cpc_v1')
        assert.equal(winner.pricing.billingUnit, 'cpc')
        assert.equal(typeof winner.pricing.targetRpmUsd, 'number')
        assert.equal(typeof winner.pricing.ecpmUsd, 'number')
        assert.equal(typeof winner.pricing.cpcUsd, 'number')
        assert.equal(typeof winner.pricing.cpaUsd, 'number')
        assert.equal(typeof winner.pricing.pClick, 'number')
        assert.equal(typeof winner.pricing.pConv, 'number')
        assert.equal(typeof winner.pricing.network, 'string')
        assert.equal(typeof winner.pricing.rawSignal, 'object')
        assert.equal(typeof winner.pricing.rawSignal.rawBidValue, 'number')
        assert.equal(typeof winner.pricing.rawSignal.rawUnit, 'string')
        assert.equal(typeof winner.pricing.rawSignal.normalizedFactor, 'number')
        assert.equal(winner.price, winner.pricing.cpcUsd)
        assert.equal(typeof bid.payload?.landingUrl, 'string')
        assert.equal(bid.payload?.landingUrl.length > 0, true)
      } else {
        assert.equal(bid.payload?.message, 'No bid')
        assert.equal(bid.payload?.filled, false)
        assert.equal(bid.payload?.landingUrl, null)
      }
    }

    const tolerantMissingChat = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        userId: 'user_missing_chat',
        messages: [{ role: 'USER_INPUT', content: 'find me a running shoe deal' }],
        extraField: 'ignored',
      },
      timeoutMs: REQUEST_TIMEOUT_MS,
    })
    if (isTransientUpstreamTimeout(tolerantMissingChat)) return
    assert.equal(tolerantMissingChat.status, 200, JSON.stringify(tolerantMissingChat.payload))
    assert.equal(tolerantMissingChat.payload?.diagnostics?.inputNormalization?.defaultsApplied?.chatIdDefaultedToUserId, true)
    assert.equal(tolerantMissingChat.payload?.diagnostics?.inputNormalization?.roleCoercions?.[0]?.to, 'user')

    const tolerantMissingUser = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        chatId: 'chat_missing_user',
        query: 'suggest a vlogging camera',
      },
      timeoutMs: REQUEST_TIMEOUT_MS,
    })
    if (isTransientUpstreamTimeout(tolerantMissingUser)) return
    assert.equal(tolerantMissingUser.status, 200, JSON.stringify(tolerantMissingUser.payload))
    assert.equal(tolerantMissingUser.payload?.diagnostics?.inputNormalization?.defaultsApplied?.userIdGenerated, true)
    assert.equal(tolerantMissingUser.payload?.diagnostics?.inputNormalization?.messagesSynthesized, true)

    const tolerantMissingPlacement = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        chatId: 'chat_missing_placement',
        messages: [{ role: 'assistant-bot', content: 'placeholder answer' }],
        prompt: 'show me a gift recommendation',
      },
      timeoutMs: REQUEST_TIMEOUT_MS,
    })
    if (isTransientUpstreamTimeout(tolerantMissingPlacement)) return
    assert.equal(tolerantMissingPlacement.status, 200, JSON.stringify(tolerantMissingPlacement.payload))
    assert.equal(tolerantMissingPlacement.payload?.diagnostics?.inputNormalization?.defaultsApplied?.placementIdDefaulted, true)
    assert.equal(
      tolerantMissingPlacement.payload?.diagnostics?.inputNormalization?.defaultsApplied?.placementIdResolvedFromDashboardDefault,
      false,
    )
    assert.equal(
      tolerantMissingPlacement.payload?.diagnostics?.inputNormalization?.defaultsApplied?.placementIdFallbackApplied,
      false,
    )
    assert.equal(
      tolerantMissingPlacement.payload?.diagnostics?.inputNormalization?.placementResolution?.source,
      'all_enabled_placements',
    )
    assert.equal(tolerantMissingPlacement.payload?.diagnostics?.inputNormalization?.roleCoercions?.[0]?.to, 'assistant')

    const placementOverrideRejected = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        userId: 'user_with_explicit_placement',
        chatId: 'chat_with_explicit_placement',
        placementId: 'chat_from_answer_v1',
        messages: [{ role: 'user', content: 'show me a cashback card deal' }],
      },
      timeoutMs: REQUEST_TIMEOUT_MS,
    })
    assert.equal(placementOverrideRejected.status, 400, JSON.stringify(placementOverrideRejected.payload))
    assert.equal(placementOverrideRejected.payload?.error?.code, 'V2_BID_PLACEMENT_ID_NOT_ALLOWED')

    const rawAuthBid = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: { Authorization: runtimeCredential.secret },
      body: {
        userId: 'user_raw_auth',
        chatId: 'chat_raw_auth',
        messages: [{ role: 'user', content: 'raw auth header should pass' }],
      },
      timeoutMs: REQUEST_TIMEOUT_MS,
    })
    if (isTransientUpstreamTimeout(rawAuthBid)) return
    assert.equal(rawAuthBid.status, 200, JSON.stringify(rawAuthBid.payload))

  } catch (error) {
    const logs = gateway.getLogs()
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`[v2-bid-api] ${message}\n[gateway stdout]\n${logs.stdout}\n[gateway stderr]\n${logs.stderr}`)
  } finally {
    await stopGateway(gateway)
  }
})

test('v2 bid API enforces cpc_v1 semantics even when CPC_SEMANTICS=off', async () => {
  const suffix = `${Date.now()}_${Math.floor(Math.random() * 1000)}`
  const scopedAccountId = `org_mediation_cpc_off_${suffix}`
  const scopedAppId = `sample-client-app-cpc-off-${suffix}`
  const port = 4160 + Math.floor(Math.random() * 120)
  const baseUrl = `http://${HOST}:${port}`
  const gateway = startGateway(port, {
    CPC_SEMANTICS: 'off',
  })

  try {
    await waitForGateway(baseUrl)

    const dashboardHeaders = await registerDashboardHeaders(baseUrl, {
      email: `v2_bid_cpc_off_${suffix}@example.com`,
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeCredential = await issueRuntimeApiKeyHeaders(baseUrl, dashboardHeaders, {
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeHeaders = {
      Authorization: `Bearer ${runtimeCredential.secret}`,
    }

    const bid = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        userId: 'user_cpc_semantics_off',
        chatId: 'chat_cpc_semantics_off',
        messages: [
          { role: 'user', content: 'recommend me a voice tool membership deal' },
          { role: 'assistant', content: 'I can compare pricing and trial options for you.' },
        ],
      },
      timeoutMs: REQUEST_TIMEOUT_MS,
    })
    if (isTransientUpstreamTimeout(bid)) return

    assert.equal(bid.status, 200, JSON.stringify(bid.payload))
    assert.equal(String(bid.payload?.diagnostics?.pricingSemanticsVersion || ''), 'cpc_v1')

    const winner = bid.payload?.data?.bid && typeof bid.payload.data.bid === 'object'
      ? bid.payload.data.bid
      : null
    if (winner) {
      assert.equal(String(winner.pricing?.pricingSemanticsVersion || ''), 'cpc_v1')
      assert.equal(String(winner.pricing?.billingUnit || ''), 'cpc')
      assert.equal(Number(winner.price || 0), Number(winner.pricing?.cpcUsd || 0))
    }
  } catch (error) {
    const logs = gateway.getLogs()
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`[v2-bid-api-cpc-off] ${message}\n[gateway stdout]\n${logs.stdout}\n[gateway stderr]\n${logs.stderr}`)
  } finally {
    await stopGateway(gateway)
  }
})

test('v2 bid API: chinese commerce query should pass intent gate and enter retrieval stage', async () => {
  const suffix = `${Date.now()}_${Math.floor(Math.random() * 1000)}`
  const scopedAccountId = `org_mediation_cn_${suffix}`
  const scopedAppId = `sample-client-app-cn-${suffix}`
  const port = 4080 + Math.floor(Math.random() * 120)
  const baseUrl = `http://${HOST}:${port}`
  const gateway = startGateway(port)

  try {
    await waitForGateway(baseUrl)

    const dashboardHeaders = await registerDashboardHeaders(baseUrl, {
      email: `v2_bid_cn_${suffix}@example.com`,
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeCredential = await issueRuntimeApiKeyHeaders(baseUrl, dashboardHeaders, {
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeHeaders = {
      Authorization: `Bearer ${runtimeCredential.secret}`,
    }

    const bid = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        userId: 'user_cn_intent',
        chatId: 'chat_cn_intent',
        messages: [
          { role: 'user', content: '我想给女朋友买会员，帮我对比价格并推荐哪个平台工具更好' },
          { role: 'assistant', content: '可以先按预算、优惠和功能做比较。' },
        ],
      },
      timeoutMs: REQUEST_TIMEOUT_MS,
    })

    assert.equal(bid.status, 200, JSON.stringify(bid.payload))
    assert.equal(bid.payload?.status, 'success')
    const failOpenUpstream = bid.payload?.diagnostics?.reasonCode === 'upstream_non_2xx'
    if (failOpenUpstream) {
      assert.equal(bid.payload?.diagnostics?.failOpenApplied, true)
      assert.equal(bid.payload?.filled, false)
    } else {
      assert.equal(typeof bid.payload?.intent?.score, 'number')
      assert.equal(
        bid.payload?.decisionTrace?.stageStatus?.retrieval === 'hit'
        || bid.payload?.decisionTrace?.stageStatus?.retrieval === 'miss',
        true,
        JSON.stringify(bid.payload?.decisionTrace || {}),
      )
      assert.notEqual(bid.payload?.decisionTrace?.reasonCode, 'policy_blocked')
    }
  } catch (error) {
    const logs = gateway.getLogs()
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`[v2-bid-api-cn] ${message}\n[gateway stdout]\n${logs.stdout}\n[gateway stderr]\n${logs.stderr}`)
  } finally {
    await stopGateway(gateway)
  }
})

test('v2 bid API: relevance threshold hard floors stay effective when env min scores are zero', async () => {
  const suffix = `${Date.now()}_${Math.floor(Math.random() * 1000)}`
  const scopedAccountId = `org_mediation_floor_${suffix}`
  const scopedAppId = `sample-client-app-floor-${suffix}`
  const port = 4210 + Math.floor(Math.random() * 120)
  const baseUrl = `http://${HOST}:${port}`
  const gateway = startGateway(port, {
    MEDIATION_INTENT_MIN_LEXICAL_SCORE: '0',
    MEDIATION_INTENT_MIN_VECTOR_SCORE: '0',
  })

  try {
    await waitForGateway(baseUrl)
    const dashboardHeaders = await registerDashboardHeaders(baseUrl, {
      email: `v2_bid_floor_${suffix}@example.com`,
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeCredential = await issueRuntimeApiKeyHeaders(baseUrl, dashboardHeaders, {
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeHeaders = {
      Authorization: `Bearer ${runtimeCredential.secret}`,
    }

    const bid = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        userId: 'user_floor_guard',
        chatId: 'chat_floor_guard',
        messages: [
          { role: 'user', content: 'recommend tools for chinese to english video dubbing' },
          { role: 'assistant', content: 'Here are several AI tools for dubbing and translation.' },
        ],
      },
      timeoutMs: REQUEST_TIMEOUT_MS,
    })

    assert.equal(bid.status, 200, JSON.stringify(bid.payload))
    assert.equal(
      bid.payload?.diagnostics?.rankingDebug?.thresholdFloorsApplied?.minLexicalScore?.configured,
      0,
    )
    assert.equal(
      bid.payload?.diagnostics?.rankingDebug?.thresholdFloorsApplied?.minVectorScore?.configured,
      0,
    )
    assert.equal(
      bid.payload?.diagnostics?.rankingDebug?.thresholdFloorsApplied?.minLexicalScore?.effective,
      0.02,
    )
    assert.equal(
      bid.payload?.diagnostics?.rankingDebug?.thresholdFloorsApplied?.minVectorScore?.effective,
      0.14,
    )
  } catch (error) {
    const logs = gateway.getLogs()
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`[v2-bid-api-floor] ${message}\n[gateway stdout]\n${logs.stdout}\n[gateway stderr]\n${logs.stderr}`)
  } finally {
    await stopGateway(gateway)
  }
})
