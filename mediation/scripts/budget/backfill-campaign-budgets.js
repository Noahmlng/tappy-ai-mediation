#!/usr/bin/env node
import { createHash } from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { derivePartnerStackBrandId, derivePartnerStackCampaignId } from '../../src/offers/network-mappers.js'
import { parseArgs, printJson, withDbPool } from '../inventory/common.js'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const PROJECT_ROOT = path.resolve(__dirname, '..', '..')
const OUTPUT_ROOT = path.join(PROJECT_ROOT, 'output', 'budget-backfill')

const DEFAULT_SCOPE = Object.freeze({
  accountId: 'org_prod_test_7gwxu5',
  appId: 'org_prod_test_7gwxu5_app',
})

const SUPPORTED_MODES = new Set(['dry-run', 'apply', 'report'])

function cleanText(value) {
  return String(value || '').trim()
}

function parseJsonObject(value) {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value
  if (!value || typeof value !== 'string') return {}
  try {
    const parsed = JSON.parse(value)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {}
  } catch {
    return {}
  }
}

function nowIso() {
  return new Date().toISOString()
}

function timestampTag(inputDate = new Date()) {
  const yyyy = inputDate.getFullYear()
  const mm = String(inputDate.getMonth() + 1).padStart(2, '0')
  const dd = String(inputDate.getDate()).padStart(2, '0')
  const hh = String(inputDate.getHours()).padStart(2, '0')
  const mi = String(inputDate.getMinutes()).padStart(2, '0')
  const ss = String(inputDate.getSeconds()).padStart(2, '0')
  return `${yyyy}${mm}${dd}-${hh}${mi}${ss}`
}

function toSafeInteger(value, fallback = 0) {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  return Math.max(0, Math.floor(n))
}

function toSafeBoolean(value) {
  if (typeof value === 'boolean') return value
  const normalized = cleanText(value).toLowerCase()
  if (!normalized) return false
  return ['1', 'true', 'yes', 'on'].includes(normalized)
}

function makeStableHash(seed = '') {
  return createHash('sha256').update(String(seed)).digest('hex').slice(0, 12)
}

function normalizeCampaignId(value = '') {
  return cleanText(value)
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

function resolveNetwork(value = '') {
  const normalized = cleanText(value).toLowerCase()
  if (!normalized) return 'unknown'
  return normalized
}

function deriveFallbackCampaignId(row = {}) {
  const network = resolveNetwork(row.network)
  const seed = [
    network,
    cleanText(row.offer_id),
    cleanText(row.upstream_offer_id),
    cleanText(row.target_url),
  ].join('|')
  return `campaign_${network}_${makeStableHash(seed)}`
}

function buildPartnerstackDeriveRecord(row = {}, metadata = {}) {
  const destination = cleanText(
    metadata.destinationUrl
    || metadata.destination_url
    || row.target_url,
  )
  const stackKey = cleanText(metadata.stackKey || metadata.stack_key)
  const partnershipKey = cleanText(
    metadata.partnershipKey
    || metadata.partnership_key
    || row.upstream_offer_id,
  )

  return {
    key: partnershipKey,
    id: cleanText(row.upstream_offer_id || row.offer_id),
    name: cleanText(row.title),
    destinationUrl: destination,
    destination_url: destination,
    stackKey,
    link: {
      destination,
      stack_key: stackKey,
    },
  }
}

export function deriveCampaignIdFromInventoryRow(row = {}) {
  const metadata = parseJsonObject(row.metadata)
  const fromMetadata = resolveCampaignIdFromMetadata(metadata)
  if (fromMetadata) return fromMetadata

  if (resolveNetwork(row.network) === 'partnerstack') {
    const campaignId = derivePartnerStackCampaignId(buildPartnerstackDeriveRecord(row, metadata))
    if (campaignId) return campaignId
  }

  return deriveFallbackCampaignId(row)
}

function deriveBrandIdFromInventoryRow(row = {}) {
  const metadata = parseJsonObject(row.metadata)
  const existing = cleanText(metadata.brandId || metadata.brand_id)
  if (existing) return existing
  if (resolveNetwork(row.network) !== 'partnerstack') return ''
  return derivePartnerStackBrandId(buildPartnerstackDeriveRecord(row, metadata))
}

export function repairPartnerstackMetadataRow(row = {}) {
  const metadata = parseJsonObject(row.metadata)
  const existingCampaignId = resolveCampaignIdFromMetadata(metadata)
  const existingBrandId = cleanText(metadata.brandId || metadata.brand_id)

  const nextMetadata = { ...metadata }
  if (!existingCampaignId) {
    nextMetadata.campaignId = deriveCampaignIdFromInventoryRow(row)
  }
  if (!existingBrandId) {
    const derivedBrandId = deriveBrandIdFromInventoryRow(row)
    if (derivedBrandId) nextMetadata.brandId = derivedBrandId
  }

  const changed = JSON.stringify(metadata) !== JSON.stringify(nextMetadata)
  return {
    changed,
    metadata: nextMetadata,
    campaignId: resolveCampaignIdFromMetadata(nextMetadata),
    brandId: cleanText(nextMetadata.brandId || nextMetadata.brand_id),
  }
}

export function classifyTier(servedCount = 0) {
  const count = toSafeInteger(servedCount, 0)
  if (count >= 10) return 'hot'
  if (count >= 1) return 'warm'
  return 'cold'
}

export function resolveBudgetByTier(network = '', tier = 'cold') {
  const normalizedNetwork = resolveNetwork(network)
  const normalizedTier = cleanText(tier).toLowerCase() || 'cold'

  if (normalizedNetwork === 'house') {
    if (normalizedTier === 'hot') return { dailyBudgetUsd: 120, lifetimeBudgetUsd: 3000 }
    if (normalizedTier === 'warm') return { dailyBudgetUsd: 60, lifetimeBudgetUsd: 1200 }
    return { dailyBudgetUsd: 25, lifetimeBudgetUsd: 500 }
  }

  if (normalizedNetwork === 'partnerstack') {
    if (normalizedTier === 'hot') return { dailyBudgetUsd: 60, lifetimeBudgetUsd: 1500 }
    if (normalizedTier === 'warm') return { dailyBudgetUsd: 30, lifetimeBudgetUsd: 600 }
    return { dailyBudgetUsd: 12, lifetimeBudgetUsd: 240 }
  }

  return { dailyBudgetUsd: 20, lifetimeBudgetUsd: 400 }
}

function isBudgetLocked(metadata = {}) {
  return toSafeBoolean(metadata?.budgetLocked)
}

function normalizeExistingByCampaign(value) {
  if (value instanceof Map) return value
  const map = new Map()
  const source = value && typeof value === 'object' ? value : {}
  for (const [key, row] of Object.entries(source)) {
    map.set(cleanText(key), row)
  }
  return map
}

function normalizeServedCountByCampaign(value) {
  if (value instanceof Map) return value
  const map = new Map()
  const source = value && typeof value === 'object' ? value : {}
  for (const [key, count] of Object.entries(source)) {
    map.set(cleanText(key), toSafeInteger(count, 0))
  }
  return map
}

export function buildBudgetPlanFromInventoryRows(rows = [], options = {}) {
  const servedCountByCampaign = normalizeServedCountByCampaign(options.servedCountByCampaign)
  const existingByCampaign = normalizeExistingByCampaign(options.existingByCampaign)

  const campaignMap = new Map()
  for (const row of Array.isArray(rows) ? rows : []) {
    const campaignId = deriveCampaignIdFromInventoryRow(row)
    if (!campaignId) continue

    if (!campaignMap.has(campaignId)) {
      campaignMap.set(campaignId, {
        campaignId,
        networks: new Set(),
        offerCount: 0,
      })
    }
    const bucket = campaignMap.get(campaignId)
    bucket.networks.add(resolveNetwork(row.network))
    bucket.offerCount += 1
  }

  const plans = Array.from(campaignMap.values())
    .map((item) => {
      const existing = existingByCampaign.get(item.campaignId) || {}
      const network = item.networks.size === 1
        ? Array.from(item.networks)[0]
        : 'unknown'
      const servedCount = toSafeInteger(servedCountByCampaign.get(item.campaignId), 0)
      const tier = classifyTier(servedCount)
      const budget = resolveBudgetByTier(network, tier)
      const metadata = parseJsonObject(existing.metadata)
      const locked = isBudgetLocked(metadata)
      const hasExistingBudget = Boolean(existing.hasBudget)

      return {
        campaignId: item.campaignId,
        network,
        tier,
        servedCount,
        offerCount: item.offerCount,
        dailyBudgetUsd: budget.dailyBudgetUsd,
        lifetimeBudgetUsd: budget.lifetimeBudgetUsd,
        hasExistingBudget,
        locked,
        action: locked ? 'skip_locked' : 'upsert',
      }
    })
    .sort((a, b) => a.campaignId.localeCompare(b.campaignId))

  const tiers = { hot: 0, warm: 0, cold: 0 }
  const networks = {}
  let skipLockedCount = 0
  let upsertCount = 0
  let coveredBeforeCount = 0
  let coveredAfterProjectedCount = 0

  for (const plan of plans) {
    tiers[plan.tier] = (tiers[plan.tier] || 0) + 1
    networks[plan.network] = (networks[plan.network] || 0) + 1
    if (plan.action === 'skip_locked') {
      skipLockedCount += 1
    } else {
      upsertCount += 1
    }
    if (plan.hasExistingBudget) coveredBeforeCount += 1
    if (plan.hasExistingBudget || plan.action === 'upsert') coveredAfterProjectedCount += 1
  }

  const uncoveredBefore = plans.filter((plan) => !plan.hasExistingBudget).map((plan) => plan.campaignId)
  const uncoveredAfterProjected = plans
    .filter((plan) => !(plan.hasExistingBudget || plan.action === 'upsert'))
    .map((plan) => plan.campaignId)

  return {
    plans,
    summary: {
      totalCampaigns: plans.length,
      coveredBeforeCount,
      coveredAfterProjectedCount,
      coverageBefore: plans.length > 0 ? Number((coveredBeforeCount / plans.length).toFixed(4)) : 0,
      coverageAfterProjected: plans.length > 0 ? Number((coveredAfterProjectedCount / plans.length).toFixed(4)) : 0,
      upsertCount,
      skipLockedCount,
      tierDistribution: tiers,
      networkDistribution: networks,
      uncoveredBefore,
      uncoveredAfterProjected,
    },
  }
}

async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true })
}

function toCsv(rows = [], headers = []) {
  const columns = Array.isArray(headers) ? headers : []
  const escape = (value) => {
    const text = String(value ?? '')
    if (text.includes(',') || text.includes('"') || text.includes('\n')) {
      return `"${text.replace(/"/g, '""')}"`
    }
    return text
  }

  const body = rows.map((row) => columns.map((key) => escape(row?.[key])).join(',')).join('\n')
  return `${columns.join(',')}\n${body}${body ? '\n' : ''}`
}

async function writeReportFiles(report = {}, options = {}) {
  const mode = cleanText(options.mode) || 'dry-run'
  const tag = cleanText(options.tag) || timestampTag()
  const outputRoot = cleanText(options.outputRoot)
    ? path.resolve(PROJECT_ROOT, options.outputRoot)
    : OUTPUT_ROOT

  await ensureDir(outputRoot)

  const jsonPath = path.join(outputRoot, `campaign-budget-backfill-${mode}-${tag}.json`)
  const csvPath = path.join(outputRoot, `campaign-budget-backfill-${mode}-${tag}.csv`)
  const latestJsonPath = path.join(outputRoot, `latest-${mode}.json`)
  const latestCsvPath = path.join(outputRoot, `latest-${mode}.csv`)

  const csvRows = Array.isArray(report?.plans)
    ? report.plans.map((row) => ({
        campaign_id: row.campaignId,
        network: row.network,
        tier: row.tier,
        served_count_7d: row.servedCount,
        offer_count: row.offerCount,
        daily_budget_usd: row.dailyBudgetUsd,
        lifetime_budget_usd: row.lifetimeBudgetUsd,
        has_existing_budget: row.hasExistingBudget,
        locked: row.locked,
        action: row.action,
      }))
    : []

  const csvPayload = toCsv(csvRows, [
    'campaign_id',
    'network',
    'tier',
    'served_count_7d',
    'offer_count',
    'daily_budget_usd',
    'lifetime_budget_usd',
    'has_existing_budget',
    'locked',
    'action',
  ])

  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8')
  await fs.writeFile(csvPath, csvPayload, 'utf8')
  await fs.writeFile(latestJsonPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8')
  await fs.writeFile(latestCsvPath, csvPayload, 'utf8')

  return {
    jsonPath: path.relative(PROJECT_ROOT, jsonPath),
    csvPath: path.relative(PROJECT_ROOT, csvPath),
    latestJsonPath: path.relative(PROJECT_ROOT, latestJsonPath),
    latestCsvPath: path.relative(PROJECT_ROOT, latestCsvPath),
  }
}

async function fetchDominantScope(pool) {
  const result = await pool.query(
    `
      SELECT
        app_id,
        account_id,
        COUNT(*)::bigint AS request_count
      FROM mediation_runtime_decision_logs
      WHERE created_at >= NOW() - INTERVAL '30 days'
        AND app_id <> ''
        AND account_id <> ''
      GROUP BY app_id, account_id
      ORDER BY request_count DESC
      LIMIT 1
    `,
  )

  const row = Array.isArray(result.rows) && result.rows.length > 0 ? result.rows[0] : null
  if (!row) {
    return {
      appId: DEFAULT_SCOPE.appId,
      accountId: DEFAULT_SCOPE.accountId,
      source: 'fallback_default',
      requestCount: 0,
    }
  }

  return {
    appId: cleanText(row.app_id) || DEFAULT_SCOPE.appId,
    accountId: cleanText(row.account_id) || DEFAULT_SCOPE.accountId,
    source: 'last_30d_top_scope',
    requestCount: toSafeInteger(row.request_count, 0),
  }
}

async function fetchPartnerstackRowsNeedingRepair(pool) {
  const result = await pool.query(
    `
      SELECT
        offer_id,
        network,
        upstream_offer_id,
        title,
        target_url,
        metadata
      FROM offer_inventory_norm
      WHERE network = 'partnerstack'
        AND availability = 'active'
        AND (
          (
            COALESCE(metadata->>'campaignId', '') = ''
            AND COALESCE(metadata->>'campaign_id', '') = ''
            AND COALESCE(metadata->>'programId', '') = ''
            AND COALESCE(metadata->>'program_id', '') = ''
            AND COALESCE(metadata->>'advertiserId', '') = ''
            AND COALESCE(metadata->>'advertiser_id', '') = ''
          )
          OR (
            COALESCE(metadata->>'brandId', '') = ''
            AND COALESCE(metadata->>'brand_id', '') = ''
          )
        )
      ORDER BY offer_id ASC
    `,
  )

  return Array.isArray(result.rows) ? result.rows : []
}

async function applyPartnerstackRepairs(pool, options = {}) {
  const apply = options.apply === true
  const rows = await fetchPartnerstackRowsNeedingRepair(pool)

  let updatedCount = 0
  const changedRows = []

  for (const row of rows) {
    const repaired = repairPartnerstackMetadataRow(row)
    if (!repaired.changed) continue

    changedRows.push({
      offerId: cleanText(row.offer_id),
      campaignId: repaired.campaignId,
      brandId: repaired.brandId,
    })

    if (apply) {
      await pool.query(
        `
          UPDATE offer_inventory_norm
          SET metadata = $2::jsonb,
              updated_at = NOW()
          WHERE offer_id = $1
        `,
        [cleanText(row.offer_id), JSON.stringify(repaired.metadata)],
      )
      updatedCount += 1
    }
  }

  return {
    scannedCount: rows.length,
    changedCount: changedRows.length,
    updatedCount,
    samples: changedRows.slice(0, 30),
  }
}

async function fetchActiveInventoryRows(pool) {
  const result = await pool.query(
    `
      SELECT
        offer_id,
        network,
        upstream_offer_id,
        title,
        target_url,
        metadata
      FROM offer_inventory_norm
      WHERE availability = 'active'
      ORDER BY offer_id ASC
    `,
  )

  return Array.isArray(result.rows) ? result.rows : []
}

async function fetchServedCountsByCampaign(pool) {
  const result = await pool.query(
    `
      SELECT
        COALESCE(
          NULLIF(payload_json #>> '{runtime,winnerBid,campaignId}', ''),
          NULLIF(payload_json #>> '{runtime,winnerBid,metadata,campaignId}', ''),
          NULLIF(payload_json #>> '{runtime,winnerBid,metadata,campaign_id}', '')
        ) AS campaign_id,
        COUNT(*)::int AS served_count
      FROM mediation_runtime_decision_logs
      WHERE created_at >= NOW() - INTERVAL '7 days'
        AND result = 'served'
      GROUP BY 1
      HAVING COALESCE(
        NULLIF(payload_json #>> '{runtime,winnerBid,campaignId}', ''),
        NULLIF(payload_json #>> '{runtime,winnerBid,metadata,campaignId}', ''),
        NULLIF(payload_json #>> '{runtime,winnerBid,metadata,campaign_id}', '')
      ) IS NOT NULL
      ORDER BY campaign_id ASC
    `,
  )

  const map = new Map()
  for (const row of Array.isArray(result.rows) ? result.rows : []) {
    const campaignId = cleanText(row.campaign_id)
    if (!campaignId) continue
    map.set(campaignId, toSafeInteger(row.served_count, 0))
  }
  return map
}

async function fetchExistingCampaignRows(pool, campaignIds = []) {
  if (!Array.isArray(campaignIds) || campaignIds.length === 0) return new Map()

  const result = await pool.query(
    `
      SELECT
        c.campaign_id,
        c.metadata,
        c.account_id,
        c.app_id,
        c.status,
        CASE WHEN l.campaign_id IS NULL THEN FALSE ELSE TRUE END AS has_budget
      FROM campaigns c
      LEFT JOIN campaign_budget_limits l
        ON l.campaign_id = c.campaign_id
      WHERE c.campaign_id = ANY($1::text[])
    `,
    [campaignIds],
  )

  const map = new Map()
  for (const row of Array.isArray(result.rows) ? result.rows : []) {
    const campaignId = cleanText(row.campaign_id)
    if (!campaignId) continue
    map.set(campaignId, {
      metadata: parseJsonObject(row.metadata),
      accountId: cleanText(row.account_id),
      appId: cleanText(row.app_id),
      status: cleanText(row.status),
      hasBudget: row.has_budget === true,
    })
  }
  return map
}

async function applyBudgetPlan(pool, plans = [], scope = {}) {
  const nowAt = nowIso()
  let upsertedCount = 0

  for (const plan of Array.isArray(plans) ? plans : []) {
    if (plan.action !== 'upsert') continue

    const metadata = {
      budgetSource: 'inventory_backfill_v1',
      budgetProfile: 'balanced',
      tier: plan.tier,
      budgetLocked: false,
      network: plan.network,
      servedCount7d: plan.servedCount,
    }

    await pool.query(
      `
        INSERT INTO campaigns (
          campaign_id,
          account_id,
          app_id,
          status,
          metadata,
          created_at,
          updated_at
        )
        VALUES ($1, $2, $3, 'active', $4::jsonb, $5::timestamptz, $5::timestamptz)
        ON CONFLICT (campaign_id) DO UPDATE
        SET
          account_id = EXCLUDED.account_id,
          app_id = EXCLUDED.app_id,
          status = EXCLUDED.status,
          metadata = COALESCE(campaigns.metadata, '{}'::jsonb) || EXCLUDED.metadata,
          updated_at = EXCLUDED.updated_at
      `,
      [
        plan.campaignId,
        cleanText(scope.accountId),
        cleanText(scope.appId),
        JSON.stringify(metadata),
        nowAt,
      ],
    )

    await pool.query(
      `
        INSERT INTO campaign_budget_limits (
          campaign_id,
          daily_budget_usd,
          lifetime_budget_usd,
          currency,
          timezone,
          created_at,
          updated_at
        )
        VALUES ($1, $2, $3, 'USD', 'UTC', $4::timestamptz, $4::timestamptz)
        ON CONFLICT (campaign_id) DO UPDATE
        SET
          daily_budget_usd = EXCLUDED.daily_budget_usd,
          lifetime_budget_usd = EXCLUDED.lifetime_budget_usd,
          currency = EXCLUDED.currency,
          timezone = EXCLUDED.timezone,
          updated_at = EXCLUDED.updated_at
      `,
      [
        plan.campaignId,
        plan.dailyBudgetUsd,
        plan.lifetimeBudgetUsd,
        nowAt,
      ],
    )

    upsertedCount += 1
  }

  return {
    upsertedCount,
  }
}

export async function runCampaignBudgetBackfill(rawArgs = {}) {
  const args = rawArgs && typeof rawArgs === 'object' ? rawArgs : {}
  const mode = cleanText(args.mode || args['run-mode'] || 'dry-run').toLowerCase()
  if (!SUPPORTED_MODES.has(mode)) {
    throw new Error(`unsupported mode: ${mode}. expected one of: ${Array.from(SUPPORTED_MODES).join(', ')}`)
  }

  const apply = mode === 'apply'
  const tag = timestampTag()

  const result = await withDbPool(async (pool) => {
    const scopeFromLogs = await fetchDominantScope(pool)
    const forcedScope = {
      appId: cleanText(args['scope-app-id'] || args.appId),
      accountId: cleanText(args['scope-account-id'] || args.accountId),
    }
    const scope = {
      appId: forcedScope.appId || scopeFromLogs.appId,
      accountId: forcedScope.accountId || scopeFromLogs.accountId,
      source: forcedScope.appId || forcedScope.accountId ? 'cli_scope' : scopeFromLogs.source,
      requestCount: scopeFromLogs.requestCount,
    }

    const partnerstackRepair = await applyPartnerstackRepairs(pool, { apply })
    const inventoryRows = await fetchActiveInventoryRows(pool)
    const servedCountByCampaign = await fetchServedCountsByCampaign(pool)

    const inventoryCampaignIds = Array.from(new Set(
      inventoryRows
        .map((row) => deriveCampaignIdFromInventoryRow(row))
        .filter(Boolean),
    ))
      .sort((a, b) => a.localeCompare(b))

    const existingByCampaign = await fetchExistingCampaignRows(pool, inventoryCampaignIds)
    const planned = buildBudgetPlanFromInventoryRows(inventoryRows, {
      servedCountByCampaign,
      existingByCampaign,
    })

    let applyResult = { upsertedCount: 0 }
    if (apply) {
      applyResult = await applyBudgetPlan(pool, planned.plans, scope)
    }

    const postExistingByCampaign = apply
      ? await fetchExistingCampaignRows(pool, inventoryCampaignIds)
      : existingByCampaign
    const coveredAfterApplyCount = planned.plans
      .filter((plan) => (postExistingByCampaign.get(plan.campaignId)?.hasBudget === true))
      .length
    const uncoveredAfterApply = planned.plans
      .filter((plan) => !(postExistingByCampaign.get(plan.campaignId)?.hasBudget === true))
      .map((plan) => plan.campaignId)

    const report = {
      generatedAt: nowIso(),
      mode,
      scope,
      partnerstackRepair,
      inventory: {
        activeOffers: inventoryRows.length,
      },
      summary: {
        ...planned.summary,
        upsertedCount: applyResult.upsertedCount,
        coveredAfterApplyCount,
        coverageAfterApply: planned.summary.totalCampaigns > 0
          ? Number((coveredAfterApplyCount / planned.summary.totalCampaigns).toFixed(4))
          : 0,
        uncoveredAfterApply,
      },
      plans: planned.plans,
    }

    const outputs = await writeReportFiles(report, {
      mode,
      tag,
      outputRoot: args['output-root'],
    })

    return {
      ...report,
      outputs,
    }
  })

  return result
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const report = await runCampaignBudgetBackfill(args)
  printJson({
    ok: true,
    mode: report.mode,
    generatedAt: report.generatedAt,
    scope: report.scope,
    inventoryActiveOffers: report.inventory.activeOffers,
    totalCampaigns: report.summary.totalCampaigns,
    upsertedCount: report.summary.upsertedCount,
    skipLockedCount: report.summary.skipLockedCount,
    coverageBefore: report.summary.coverageBefore,
    coverageAfterProjected: report.summary.coverageAfterProjected,
    coverageAfterApply: report.summary.coverageAfterApply,
    uncoveredAfterApplyCount: report.summary.uncoveredAfterApply.length,
    partnerstackRepair: {
      scannedCount: report.partnerstackRepair.scannedCount,
      changedCount: report.partnerstackRepair.changedCount,
      updatedCount: report.partnerstackRepair.updatedCount,
    },
    outputs: report.outputs,
  })
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error('[budget-backfill] failed:', error instanceof Error ? error.message : error)
    process.exit(1)
  })
}

export const __budgetBackfillInternal = Object.freeze({
  deriveCampaignIdFromInventoryRow,
  repairPartnerstackMetadataRow,
  classifyTier,
  resolveBudgetByTier,
  buildBudgetPlanFromInventoryRows,
})
