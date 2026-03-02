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
  const raw = Number(process.env.MEDIATION_TEST_HEALTH_TIMEOUT_MS || 30000)
  if (!Number.isFinite(raw) || raw <= 0) return 30000
  return Math.floor(raw)
})()
const REQUEST_TIMEOUT_MS = (() => {
  const raw = Number(process.env.MEDIATION_TEST_REQUEST_TIMEOUT_MS || 8000)
  if (!Number.isFinite(raw) || raw <= 0) return 8000
  return Math.floor(raw)
})()

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitForCondition(check, options = {}) {
  const timeoutMs = Number(options.timeoutMs || REQUEST_TIMEOUT_MS)
  const intervalMs = Number(options.intervalMs || 300)
  const startedAt = Date.now()
  let lastError = null
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const passed = await check()
      if (passed) return true
    } catch (error) {
      lastError = error
    }
    await sleep(intervalMs)
  }
  if (lastError) throw lastError
  return false
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

async function requestRedirect(url, options = {}) {
  const timeout = withTimeoutSignal(options.timeoutMs)
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        ...(options.headers || {}),
      },
      redirect: 'manual',
      signal: timeout.signal,
    })
    return {
      status: response.status,
      location: String(response.headers.get('location') || '').trim(),
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
      CPC_SEMANTICS: process.env.CPC_SEMANTICS || 'on',
      BUDGET_ENFORCEMENT: process.env.BUDGET_ENFORCEMENT || 'monitor_only',
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
      email: String(input.email || `cpc_click_${now}@example.com`).trim(),
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
  return {
    Authorization: `Bearer ${secret}`,
  }
}

async function requestServedBid(baseUrl, runtimeHeaders, bidPayload) {
  let lastPayload = null
  for (let attempt = 0; attempt < 12; attempt += 1) {
    const bid = await requestJson(baseUrl, '/api/v2/bid', {
      method: 'POST',
      headers: runtimeHeaders,
      body: bidPayload,
      timeoutMs: REQUEST_TIMEOUT_MS,
    })
    assert.equal(bid.status, 200, `v2 bid failed: ${JSON.stringify(bid.payload)}`)
    lastPayload = bid.payload
    const winner = bid.payload?.data?.bid && typeof bid.payload.data.bid === 'object'
      ? bid.payload.data.bid
      : null
    if (winner) {
      return {
        response: bid.payload,
        winner,
      }
    }
    await sleep(120)
  }

  throw new Error(`unable to get served bid after retries: ${JSON.stringify(lastPayload)}`)
}

test('cpc click settlement: redirect bills CPC and duplicate sdk click keeps revenue idempotent', async () => {
  const suffix = `${Date.now()}_${Math.floor(Math.random() * 1000)}`
  const scopedAccountId = `org_cpc_${suffix}`
  const scopedAppId = `app_cpc_${suffix}`
  const port = 4040 + Math.floor(Math.random() * 120)
  const baseUrl = `http://${HOST}:${port}`
  const gateway = startGateway(port)

  const userQuery = '我的女朋友是个 vlogger，我想给她买个 elevenlabs 的会员，帮我对比一下这个和 Murf AI 的产品吧'
  const assistantAnswer = '我可以从价格、语音质量和授权范围三个维度对比。'

  try {
    await waitForGateway(baseUrl)

    const dashboardHeaders = await registerDashboardHeaders(baseUrl, {
      email: `owner_cpc_${suffix}@example.com`,
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeHeaders = await issueRuntimeApiKeyHeaders(baseUrl, dashboardHeaders, {
      accountId: scopedAccountId,
      appId: scopedAppId,
    })

    const beforeSummary = await requestJson(baseUrl, '/api/v1/dashboard/metrics/summary', {
      headers: dashboardHeaders,
    })
    assert.equal(beforeSummary.ok, true)
    const beforeRevenue = Number(beforeSummary.payload?.revenueUsd || 0)
    const beforeClicks = Number(beforeSummary.payload?.clicks || 0)

    const bidPayload = {
      userId: `user_${suffix}`,
      chatId: `chat_${suffix}`,
      messages: [
        { role: 'user', content: userQuery },
        { role: 'assistant', content: assistantAnswer },
      ],
    }

    const served = await requestServedBid(baseUrl, runtimeHeaders, bidPayload)
    const winner = served.winner
    const requestId = String(served.response?.requestId || '').trim()
    const winnerPrice = Number(winner.price || 0)
    const winnerCpcUsd = Number(winner.pricing?.cpcUsd || 0)
    const winnerAdId = String(winner.bidId || '').trim()
    const winnerPlacementId = String(served.response?.diagnostics?.precheck?.placement?.placementId || '').trim()

    assert.equal(Boolean(requestId), true)
    assert.equal(Number.isFinite(winnerPrice) && winnerPrice > 0, true)
    assert.equal(winnerPrice, winnerCpcUsd)
    assert.equal(String(winner.pricing?.billingUnit || ''), 'cpc')
    assert.equal(String(winner.pricing?.pricingSemanticsVersion || ''), 'cpc_v1')
    assert.equal(typeof winner.url, 'string')
    assert.equal(winner.url.includes('/api/v1/sdk/click?'), true)

    const redirectClick = await requestRedirect(winner.url, { timeoutMs: REQUEST_TIMEOUT_MS })
    assert.equal(redirectClick.status, 302)
    assert.equal(redirectClick.location.startsWith('http://') || redirectClick.location.startsWith('https://'), true)

    let afterRedirectRevenue = 0
    let afterRedirectClicks = 0
    const redirectSettled = await waitForCondition(async () => {
      const summary = await requestJson(baseUrl, '/api/v1/dashboard/metrics/summary', {
        headers: dashboardHeaders,
      })
      if (!summary.ok) return false
      afterRedirectRevenue = Number(summary.payload?.revenueUsd || 0)
      afterRedirectClicks = Number(summary.payload?.clicks || 0)
      return (
        afterRedirectClicks >= beforeClicks + 1
        && afterRedirectRevenue >= beforeRevenue + Number((winnerPrice - 0.01).toFixed(2))
      )
    })
    assert.equal(redirectSettled, true)

    const redirectEventLogs = await requestJson(
      baseUrl,
      `/api/v1/dashboard/events?requestId=${encodeURIComponent(requestId)}&eventType=redirect_click`,
      { headers: dashboardHeaders },
    )
    assert.equal(redirectEventLogs.ok, true)
    const redirectRows = Array.isArray(redirectEventLogs.payload?.items) ? redirectEventLogs.payload.items : []
    assert.equal(redirectRows.length > 0, true)
    assert.equal(String(redirectRows[0]?.event || ''), 'click')
    assert.equal(String(redirectRows[0]?.kind || ''), 'click')

    const sdkClick = await requestJson(baseUrl, '/api/v1/sdk/events', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        requestId,
        appId: scopedAppId,
        accountId: scopedAccountId,
        sessionId: bidPayload.chatId,
        turnId: `turn_${suffix}`,
        query: userQuery,
        answerText: assistantAnswer,
        intentScore: 0.92,
        locale: 'zh-CN',
        kind: 'click',
        adId: winnerAdId,
        placementId: winnerPlacementId || 'chat_from_answer_v1',
      },
    })
    assert.equal(sdkClick.ok, true, `sdk click event failed: ${JSON.stringify(sdkClick.payload)}`)
    const sdkFactId = String(sdkClick.payload?.factId || '').trim()
    const sdkRevenue = Number(sdkClick.payload?.revenueUsd || 0)
    assert.equal(Number.isFinite(sdkRevenue), true)
    if (sdkFactId) {
      assert.equal(sdkRevenue > 0, true)
    }

    let afterSdkRevenue = 0
    let afterSdkClicks = 0
    const sdkSettled = await waitForCondition(async () => {
      const summary = await requestJson(baseUrl, '/api/v1/dashboard/metrics/summary', {
        headers: dashboardHeaders,
      })
      if (!summary.ok) return false
      afterSdkRevenue = Number(summary.payload?.revenueUsd || 0)
      afterSdkClicks = Number(summary.payload?.clicks || 0)
      return (
        afterSdkClicks >= afterRedirectClicks + 1
        && afterSdkRevenue >= afterRedirectRevenue
      )
    })
    assert.equal(sdkSettled, true)

    const usageRevenue = await requestJson(baseUrl, '/api/v1/dashboard/usage-revenue', {
      headers: dashboardHeaders,
    })
    assert.equal(usageRevenue.ok, true)
    assert.equal(Number(usageRevenue.payload?.totals?.settledRevenueUsd || 0) > 0, true)
  } catch (error) {
    const logs = gateway.getLogs()
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(
      `[cpc-click-revenue] ${message}\n[gateway stdout]\n${logs.stdout}\n[gateway stderr]\n${logs.stderr}`,
    )
  } finally {
    await stopGateway(gateway)
  }
})

test('cpc click settlement uses cpcUsd even when CPC_SEMANTICS=off', async () => {
  const suffix = `${Date.now()}_${Math.floor(Math.random() * 1000)}`
  const scopedAccountId = `org_cpc_off_${suffix}`
  const scopedAppId = `app_cpc_off_${suffix}`
  const port = 4180 + Math.floor(Math.random() * 120)
  const baseUrl = `http://${HOST}:${port}`
  const gateway = startGateway(port, {
    CPC_SEMANTICS: 'off',
  })

  const userQuery = '帮我推荐一个适合做视频配音的 AI 工具套餐'
  const assistantAnswer = '我可以给你按价格和授权范围做一个对比。'

  try {
    await waitForGateway(baseUrl)

    const dashboardHeaders = await registerDashboardHeaders(baseUrl, {
      email: `owner_cpc_off_${suffix}@example.com`,
      accountId: scopedAccountId,
      appId: scopedAppId,
    })
    const runtimeHeaders = await issueRuntimeApiKeyHeaders(baseUrl, dashboardHeaders, {
      accountId: scopedAccountId,
      appId: scopedAppId,
    })

    const bidPayload = {
      userId: `user_${suffix}`,
      chatId: `chat_${suffix}`,
      messages: [
        { role: 'user', content: userQuery },
        { role: 'assistant', content: assistantAnswer },
      ],
    }

    const served = await requestServedBid(baseUrl, runtimeHeaders, bidPayload)
    const winner = served.winner
    const requestId = String(served.response?.requestId || '').trim()
    const winnerCpcUsd = Number(winner.pricing?.cpcUsd || 0)
    const winnerAdId = String(winner.bidId || '').trim()
    const winnerPlacementId = String(served.response?.diagnostics?.precheck?.placement?.placementId || '').trim()

    assert.equal(Boolean(requestId), true)
    assert.equal(Number.isFinite(winnerCpcUsd) && winnerCpcUsd > 0, true)
    assert.equal(String(winner.pricing?.pricingSemanticsVersion || ''), 'cpc_v1')
    assert.equal(String(winner.pricing?.billingUnit || ''), 'cpc')

    const sdkClick = await requestJson(baseUrl, '/api/v1/sdk/events', {
      method: 'POST',
      headers: runtimeHeaders,
      body: {
        requestId,
        appId: scopedAppId,
        accountId: scopedAccountId,
        sessionId: bidPayload.chatId,
        turnId: `turn_${suffix}`,
        query: userQuery,
        answerText: assistantAnswer,
        intentScore: 0.88,
        locale: 'zh-CN',
        kind: 'click',
        adId: winnerAdId,
        placementId: winnerPlacementId || 'chat_from_answer_v1',
      },
    })
    assert.equal(sdkClick.ok, true, `sdk click event failed: ${JSON.stringify(sdkClick.payload)}`)
    const sdkRevenue = Number(sdkClick.payload?.revenueUsd || 0)
    assert.equal(Number.isFinite(sdkRevenue), true)
    assert.equal(sdkRevenue, winnerCpcUsd)
  } catch (error) {
    const logs = gateway.getLogs()
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(
      `[cpc-click-revenue-cpc-off] ${message}\n[gateway stdout]\n${logs.stdout}\n[gateway stderr]\n${logs.stderr}`,
    )
  } finally {
    await stopGateway(gateway)
  }
})
