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

function startGateway(port) {
  const child = spawn(process.execPath, [GATEWAY_ENTRY], {
    cwd: PROJECT_ROOT,
    env: {
      ...process.env,
      SUPABASE_DB_URL: process.env.SUPABASE_DB_URL_TEST || process.env.SUPABASE_DB_URL || '',
      MEDIATION_ALLOWED_ORIGINS: 'http://127.0.0.1:3000',
      MEDIATION_ENABLE_LOCAL_SERVER: 'true',
      MEDIATION_GATEWAY_HOST: HOST,
      MEDIATION_GATEWAY_PORT: String(port),
      OPENROUTER_API_KEY: '',
      OPENROUTER_MODEL: 'glm-5',
      CJ_TOKEN: 'mock-cj-token',
      PARTNERSTACK_API_KEY: 'mock-partnerstack-key',
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
  const email = String(input.email || `owner_${now}@example.com`)
  const password = String(input.password || 'pass12345')
  const accountId = String(input.accountId || 'org_mediation')
  const appId = String(input.appId || 'sample-client-app')
  const register = await requestJson(baseUrl, '/api/v1/public/dashboard/register', {
    method: 'POST',
    body: {
      email,
      password,
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

async function issueRuntimeApiKeyHeaders(baseUrl, input = {}, headers = {}) {
  const accountId = String(input.accountId || 'org_mediation')
  const appId = String(input.appId || 'sample-client-app')
  const environment = String(input.environment || 'prod')
  const created = await requestJson(baseUrl, '/api/v1/public/credentials/keys', {
    method: 'POST',
    headers,
    body: {
      accountId,
      appId,
      environment,
      name: `runtime-${environment}`,
    },
  })
  assert.equal(created.status, 201, `issue runtime key failed: ${JSON.stringify(created.payload)}`)
  const secret = String(created.payload?.secret || '').trim()
  assert.equal(Boolean(secret), true, 'runtime key create should return secret')
  return {
    Authorization: `Bearer ${secret}`,
  }
}

test('legacy evaluate endpoint is removed', async () => {
  const suffix = `${Date.now()}_${Math.floor(Math.random() * 1000)}`
  const scopedAccountId = `org_mediation_${suffix}`
  const scopedAppId = `sample-client-app-${suffix}`
  const port = 3450 + Math.floor(Math.random() * 200)
  const baseUrl = `http://${HOST}:${port}`
  const gateway = startGateway(port)

  try {
    await waitForGateway(baseUrl)
    const reset = await requestJson(baseUrl, '/api/v1/dev/reset', { method: 'POST' })
    assert.equal(reset.status, 404)
    const dashboardHeaders = await registerDashboardHeaders(baseUrl, {
      email: `next-step-reason-priority-${suffix}@example.com`,
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeHeaders = await issueRuntimeApiKeyHeaders(baseUrl, {
      accountId: scopedAccountId,
      appId: scopedAppId,
    }, dashboardHeaders)

    const evaluate = await requestJson(baseUrl, '/api/v1/sdk/evaluate', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        sessionId: `reason_priority_session_${Date.now()}`,
        turnId: `reason_priority_turn_${Date.now()}`,
        userId: 'reason_priority_user',
        placementId: 'chat_intent_recommendation_v1',
        query: 'Explain why the sky is blue in simple physics terms.',
        answerText: 'Rayleigh scattering causes shorter wavelengths to scatter more.',
        intentScore: 0.1,
        locale: 'en-US',
      },
    })

    assert.equal(evaluate.status, 404)
    assert.equal(evaluate.payload?.error?.code, 'NOT_FOUND')
  } catch (error) {
    const logs = gateway.getLogs()
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(
      `[next-step-reason-priority] ${message}\n[gateway stdout]\n${logs.stdout}\n[gateway stderr]\n${logs.stderr}`,
    )
  } finally {
    await stopGateway(gateway)
  }
})

test('next-step decision logs are recorded through v2 bid flow', async () => {
  const suffix = `${Date.now()}_${Math.floor(Math.random() * 1000)}`
  const scopedAccountId = `org_mediation_${suffix}`
  const scopedAppId = `sample-client-app-${suffix}`
  const port = 3550 + Math.floor(Math.random() * 200)
  const baseUrl = `http://${HOST}:${port}`
  const gateway = startGateway(port)

  try {
    await waitForGateway(baseUrl)
    const reset = await requestJson(baseUrl, '/api/v1/dev/reset', { method: 'POST' })
    assert.equal(reset.status, 404)
    const dashboardHeaders = await registerDashboardHeaders(baseUrl, {
      email: `next-step-observability-${suffix}@example.com`,
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeHeaders = await issueRuntimeApiKeyHeaders(baseUrl, {
      accountId: scopedAccountId,
      appId: scopedAppId,
    }, dashboardHeaders)

    const enablePlacement = await requestJson(baseUrl, '/api/v1/dashboard/placements/chat_intent_recommendation_v1', {
      method: 'PUT',
      headers: dashboardHeaders,
      body: {
        enabled: true,
      },
    })
    assert.equal(enablePlacement.ok, true, 'chat_intent_recommendation_v1 should be enabled for next-step checks')
    const disableFromAnswerPlacement = await requestJson(baseUrl, '/api/v1/dashboard/placements/chat_from_answer_v1', {
      method: 'PUT',
      headers: dashboardHeaders,
      body: {
        enabled: false,
      },
    })
    assert.equal(disableFromAnswerPlacement.ok, true, 'chat_from_answer_v1 should be disabled for next-step checks')

    const bid = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        userId: `inference_observe_user_${Date.now()}`,
        chatId: `inference_observe_chat_${Date.now()}`,
        messages: [
          { role: 'user', content: 'I want to buy a running shoe for daily gym training' },
          { role: 'assistant', content: 'You can compare running shoes by cushioning and durability.' },
        ],
      },
    })

    assert.equal(bid.ok, true, `v2 bid failed: ${JSON.stringify(bid.payload)}`)
    const requestId = String(bid.payload?.requestId || '').trim()
    assert.equal(requestId.length > 0, true, 'v2 bid must return requestId')

    const decisions = await requestJson(baseUrl, `/api/v1/dashboard/decisions?requestId=${encodeURIComponent(requestId)}`, {
      headers: dashboardHeaders,
    })
    assert.equal(decisions.ok, true, `decision query failed: ${JSON.stringify(decisions.payload)}`)

    const items = Array.isArray(decisions.payload?.items) ? decisions.payload.items : []
    const row = items.find((item) => String(item?.requestId || '').trim() === requestId)
    assert.equal(Boolean(row), true, 'decision row should be present')

    assert.equal(['served', 'blocked', 'no_fill', 'error'].includes(String(row?.result || '')), true)
    assert.equal(String(row?.requestId || '').trim(), requestId)
    assert.equal(String(row?.placementId || '').trim(), 'chat_intent_recommendation_v1')
    assert.equal(Boolean(row?.runtime && typeof row.runtime === 'object'), true)
    assert.equal(Boolean(row?.runtime?.bidV2), true)
    assert.equal(Boolean(row?.runtime?.relevance && typeof row.runtime.relevance === 'object'), true)
    assert.equal(
      ['strict', 'relaxed', 'blocked', 'observe', 'shadow', 'disabled'].includes(
        String(row?.runtime?.relevance?.gateStage || ''),
      ),
      true,
    )
    assert.equal(typeof row?.runtime?.relevance?.thresholdsApplied?.thresholdVersion, 'string')

    const events = await requestJson(
      baseUrl,
      `/api/v1/dashboard/events?eventType=decision&requestId=${encodeURIComponent(requestId)}`,
      {
        headers: dashboardHeaders,
      },
    )
    assert.equal(events.ok, true, `event query failed: ${JSON.stringify(events.payload)}`)
    const eventItems = Array.isArray(events.payload?.items) ? events.payload.items : []
    const eventRow = eventItems.find((item) => String(item?.requestId || '').trim() === requestId)
    assert.equal(Boolean(eventRow), true, 'decision event row should be present')
    assert.equal(eventRow?.eventType, 'decision')
    assert.equal(eventRow?.developerOnly, true)
    assert.equal(eventRow?.observabilityTier, 'developer')
    assert.equal(typeof eventRow?.developerTrace?.schemaVersion, 'string')
    assert.equal(eventRow?.developerTrace?.scope, 'developer_only')
    assert.equal(typeof eventRow?.developerTrace?.io?.input?.query, 'string')
    assert.equal(typeof eventRow?.developerTrace?.io?.output?.result, 'string')
    assert.equal(typeof eventRow?.developerTrace?.steps?.search?.keywords?.queryUsed, 'string')
    assert.equal(Array.isArray(eventRow?.developerTrace?.steps?.search?.results), true)
    assert.equal(Array.isArray(eventRow?.developerTrace?.steps?.matching?.scoredOffers), true)
    assert.equal(Array.isArray(eventRow?.developerTrace?.steps?.pricing?.offerQuotes), true)
    assert.equal(typeof eventRow?.developerTrace?.steps?.pipeline?.timingsMs, 'object')
    const quoteItems = Array.isArray(eventRow?.developerTrace?.steps?.pricing?.offerQuotes)
      ? eventRow.developerTrace.steps.pricing.offerQuotes
      : []
    if (quoteItems.length > 0) {
      assert.equal(typeof quoteItems[0]?.quote?.cpcUsd, 'number')
      assert.equal(typeof quoteItems[0]?.quote?.price, 'number')
    }
  } catch (error) {
    const logs = gateway.getLogs()
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(
      `[next-step-inference-observability] ${message}\n[gateway stdout]\n${logs.stdout}\n[gateway stderr]\n${logs.stderr}`,
    )
  } finally {
    await stopGateway(gateway)
  }
})
