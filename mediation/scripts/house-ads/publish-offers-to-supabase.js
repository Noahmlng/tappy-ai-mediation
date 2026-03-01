#!/usr/bin/env node
import { spawn } from 'node:child_process'
import fs from 'node:fs/promises'
import path from 'node:path'
import { Pool } from 'pg'
import {
  CURATED_ROOT,
  parseArgs,
  toBoolean,
  cleanText,
  readJson,
  readJsonl,
  writeJson,
  ensureDir,
  timestampTag,
} from './lib/common.js'

const OFFERS_ROOT = path.resolve(process.cwd(), 'data/house-ads/offers')
const OFFERS_CURATED_DIR = path.join(OFFERS_ROOT, 'curated')
const OFFERS_REPORT_DIR = path.join(OFFERS_ROOT, 'reports')

function splitCsv(value = '') {
  return cleanText(value)
    .split(',')
    .map((item) => cleanText(item))
    .filter(Boolean)
}

function toNumberOrNull(value) {
  const n = Number(value)
  if (!Number.isFinite(n)) return null
  return n
}

function normalizeStatus(value = '', fallback = 'active') {
  const text = cleanText(value).toLowerCase()
  if (!text) return fallback
  return text
}

function normalizeBrandStatus(value = '') {
  const text = normalizeStatus(value, 'active')
  if (['active', 'paused', 'archived', 'inactive'].includes(text)) return text
  if (text === 'suspect') return 'inactive'
  return 'active'
}

function toTimestampOrNull(value) {
  const text = cleanText(value)
  if (!text) return null
  const ms = Date.parse(text)
  if (!Number.isFinite(ms)) return null
  return new Date(ms).toISOString()
}

async function fileExists(filePath = '') {
  try {
    await fs.access(filePath)
    return true
  } catch {
    return false
  }
}

async function resolveOffersFile(args = {}) {
  const explicit = cleanText(args['offers-file'])
  if (explicit) return path.resolve(process.cwd(), explicit)

  const defaultPath = path.join(OFFERS_CURATED_DIR, 'offers.jsonl')
  if (await fileExists(defaultPath)) return defaultPath

  const candidates = [
    path.join(OFFERS_CURATED_DIR, 'latest-offers-merged.json'),
    path.join(OFFERS_CURATED_DIR, 'latest-offers-real.json'),
  ]
  for (const metaPath of candidates) {
    if (!(await fileExists(metaPath))) continue
    try {
      const payload = await readJson(metaPath, null)
      const latest = cleanText(payload?.latestJsonl)
      if (!latest) continue
      const resolved = path.resolve(process.cwd(), latest)
      if (await fileExists(resolved)) return resolved
    } catch {
      // continue
    }
  }
  throw new Error('No publishable offers file found. Run house-ads:offers:qa first or pass --offers-file.')
}

function normalizeBrandRow(row = {}) {
  return {
    brand_id: cleanText(row.brand_id),
    brand_name: cleanText(row.brand_name),
    canonical_brand_name: cleanText(row.canonical_brand_name),
    official_domain: cleanText(row.official_domain),
    vertical_l1: cleanText(row.vertical_l1),
    vertical_l2: cleanText(row.vertical_l2),
    market: cleanText(row.market) || 'US',
    status: normalizeBrandStatus(row.status),
    source_confidence: toNumberOrNull(row.source_confidence) ?? 0,
    alignment_status: cleanText(row.alignment_status),
    alignment_source: cleanText(row.alignment_source),
    strict_admitted: Boolean(row.strict_admitted),
    strong_evidence: Boolean(row.strong_evidence || row.evidence?.canonical_confirmed),
    clean_score: toNumberOrNull(row.clean_score) ?? 0,
    canonical_source: cleanText(row.canonical_source || row.evidence?.canonical_source || ''),
    checked_at: toTimestampOrNull(row.checked_at),
    evidence_json: row.evidence && typeof row.evidence === 'object' ? row.evidence : {},
  }
}

function normalizeOfferRow(row = {}) {
  const tags = Array.isArray(row.tags) ? row.tags : []
  return {
    offer_id: cleanText(row.offer_id),
    campaign_id: cleanText(row.campaign_id),
    brand_id: cleanText(row.brand_id),
    offer_type: cleanText(row.offer_type),
    vertical_l1: cleanText(row.vertical_l1),
    vertical_l2: cleanText(row.vertical_l2),
    market: cleanText(row.market) || 'US',
    title: cleanText(row.title),
    description: cleanText(row.description),
    snippet: cleanText(row.snippet),
    target_url: cleanText(row.target_url),
    image_url: cleanText(row.image_url),
    cta_text: cleanText(row.cta_text),
    status: normalizeStatus(row.status, 'active'),
    language: cleanText(row.language || 'en-US'),
    disclosure: cleanText(row.disclosure || 'Sponsored'),
    source_type: cleanText(row.source_type || 'real'),
    confidence_score: toNumberOrNull(row.confidence_score) ?? 0,
    freshness_ttl_hours: Math.max(1, Math.floor(toNumberOrNull(row.freshness_ttl_hours) ?? 48)),
    last_verified_at: toTimestampOrNull(row.last_verified_at),
    product_id: cleanText(row.product_id),
    merchant: cleanText(row.merchant),
    price: toNumberOrNull(row.price),
    original_price: toNumberOrNull(row.original_price),
    currency: cleanText(row.currency || 'USD').toUpperCase(),
    discount_pct: toNumberOrNull(row.discount_pct),
    availability: cleanText(row.availability || 'unknown'),
    tags_json: tags,
  }
}

function spawnCommand(command, args, cwd) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env: process.env,
      stdio: 'inherit',
    })
    child.on('error', reject)
    child.on('exit', (code) => {
      if (code === 0) resolve()
      else reject(new Error(`${command} ${args.join(' ')} failed with code ${code}`))
    })
  })
}

async function runPostSync(scope = 'full') {
  await spawnCommand('npm', ['run', 'inventory:sync:house'], process.cwd())
  if (scope === 'house') return
  if (scope === 'house_snapshot') {
    await spawnCommand('npm', ['run', 'inventory:snapshot'], process.cwd())
    return
  }
  await spawnCommand('npm', ['run', 'inventory:embeddings'], process.cwd())
  await spawnCommand('npm', ['run', 'inventory:snapshot'], process.cwd())
}

function createPool() {
  const dbUrl = cleanText(process.env.SUPABASE_DB_URL)
  if (!dbUrl) throw new Error('SUPABASE_DB_URL is required.')
  return new Pool({
    connectionString: dbUrl,
    ssl: dbUrl.includes('supabase.co') ? { rejectUnauthorized: false } : undefined,
  })
}

async function upsertBrands(client, brands = []) {
  let upserted = 0
  for (const row of brands) {
    await client.query(
      `
      INSERT INTO house_ads_brands (
        brand_id,
        brand_name,
        canonical_brand_name,
        official_domain,
        vertical_l1,
        vertical_l2,
        market,
        status,
        source_confidence,
        alignment_status,
        alignment_source,
        strict_admitted,
        strong_evidence,
        clean_score,
        canonical_source,
        checked_at,
        evidence_json,
        imported_at,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
        $12, $13, $14, $15, $16::timestamptz, $17::jsonb, NOW(), NOW()
      )
      ON CONFLICT (brand_id) DO UPDATE
      SET
        brand_name = EXCLUDED.brand_name,
        canonical_brand_name = EXCLUDED.canonical_brand_name,
        official_domain = EXCLUDED.official_domain,
        vertical_l1 = EXCLUDED.vertical_l1,
        vertical_l2 = EXCLUDED.vertical_l2,
        market = EXCLUDED.market,
        status = EXCLUDED.status,
        source_confidence = EXCLUDED.source_confidence,
        alignment_status = EXCLUDED.alignment_status,
        alignment_source = EXCLUDED.alignment_source,
        strict_admitted = EXCLUDED.strict_admitted,
        strong_evidence = EXCLUDED.strong_evidence,
        clean_score = EXCLUDED.clean_score,
        canonical_source = EXCLUDED.canonical_source,
        checked_at = EXCLUDED.checked_at,
        evidence_json = EXCLUDED.evidence_json,
        updated_at = NOW()
      `,
      [
        row.brand_id,
        row.brand_name,
        row.canonical_brand_name,
        row.official_domain,
        row.vertical_l1,
        row.vertical_l2,
        row.market,
        row.status,
        row.source_confidence,
        row.alignment_status,
        row.alignment_source,
        row.strict_admitted,
        row.strong_evidence,
        row.clean_score,
        row.canonical_source,
        row.checked_at,
        JSON.stringify(row.evidence_json || {}),
      ],
    )
    upserted += 1
  }
  return upserted
}

async function upsertOffers(client, offers = []) {
  let upserted = 0
  for (const row of offers) {
    await client.query(
      `
      INSERT INTO house_ads_offers (
        offer_id,
        campaign_id,
        brand_id,
        offer_type,
        vertical_l1,
        vertical_l2,
        market,
        title,
        description,
        snippet,
        target_url,
        image_url,
        cta_text,
        status,
        language,
        disclosure,
        source_type,
        confidence_score,
        freshness_ttl_hours,
        last_verified_at,
        product_id,
        merchant,
        price,
        original_price,
        currency,
        discount_pct,
        availability,
        tags_json,
        imported_at,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
        $12, $13, $14, $15, $16, $17, $18, $19, $20::timestamptz,
        $21, $22, $23, $24, $25, $26, $27, $28::jsonb, NOW(), NOW()
      )
      ON CONFLICT (offer_id) DO UPDATE
      SET
        campaign_id = EXCLUDED.campaign_id,
        brand_id = EXCLUDED.brand_id,
        offer_type = EXCLUDED.offer_type,
        vertical_l1 = EXCLUDED.vertical_l1,
        vertical_l2 = EXCLUDED.vertical_l2,
        market = EXCLUDED.market,
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        snippet = EXCLUDED.snippet,
        target_url = EXCLUDED.target_url,
        image_url = EXCLUDED.image_url,
        cta_text = EXCLUDED.cta_text,
        status = EXCLUDED.status,
        language = EXCLUDED.language,
        disclosure = EXCLUDED.disclosure,
        source_type = EXCLUDED.source_type,
        confidence_score = EXCLUDED.confidence_score,
        freshness_ttl_hours = EXCLUDED.freshness_ttl_hours,
        last_verified_at = EXCLUDED.last_verified_at,
        product_id = EXCLUDED.product_id,
        merchant = EXCLUDED.merchant,
        price = EXCLUDED.price,
        original_price = EXCLUDED.original_price,
        currency = EXCLUDED.currency,
        discount_pct = EXCLUDED.discount_pct,
        availability = EXCLUDED.availability,
        tags_json = EXCLUDED.tags_json,
        updated_at = NOW()
      `,
      [
        row.offer_id,
        row.campaign_id,
        row.brand_id,
        row.offer_type,
        row.vertical_l1,
        row.vertical_l2,
        row.market,
        row.title,
        row.description,
        row.snippet,
        row.target_url,
        row.image_url,
        row.cta_text,
        row.status,
        row.language,
        row.disclosure,
        row.source_type,
        row.confidence_score,
        row.freshness_ttl_hours,
        row.last_verified_at,
        row.product_id,
        row.merchant,
        row.price,
        row.original_price,
        row.currency,
        row.discount_pct,
        row.availability,
        JSON.stringify(row.tags_json || []),
      ],
    )
    upserted += 1
  }
  return upserted
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const tag = timestampTag()
  const dryRun = toBoolean(args['dry-run'], false)
  const syncInventory = toBoolean(args['sync-inventory'], true)
  const syncScopeRaw = cleanText(args['sync-scope'] || 'full').toLowerCase()
  const syncScope = ['full', 'house', 'house_snapshot'].includes(syncScopeRaw) ? syncScopeRaw : 'full'
  const categories = splitCsv(args['category-allowlist']).map((item) => item.toLowerCase())

  const offersFile = await resolveOffersFile(args)
  const brandsFile = path.resolve(
    process.cwd(),
    cleanText(args['brands-file']) || path.join(CURATED_ROOT, 'brands.jsonl'),
  )

  const [offersRaw, brandsRaw] = await Promise.all([readJsonl(offersFile), readJsonl(brandsFile)])
  const offersFiltered = offersRaw
    .map((row) => normalizeOfferRow(row))
    .filter((row) => row.offer_id && row.brand_id && row.title && row.target_url)
    .filter((row) => {
      if (categories.length === 0) return true
      const key = `${cleanText(row.vertical_l1).toLowerCase()}::${cleanText(row.vertical_l2).toLowerCase()}`
      return categories.includes(key)
    })

  const brandIds = new Set(offersFiltered.map((row) => row.brand_id))
  const brandsFiltered = brandsRaw
    .map((row) => normalizeBrandRow(row))
    .filter((row) => row.brand_id && row.brand_name && row.official_domain && brandIds.has(row.brand_id))

  const outputSummary = {
    generated_at: new Date().toISOString(),
    dry_run: dryRun,
    sync_inventory: syncInventory,
    sync_scope: syncScope,
    input: {
      offers_file: path.relative(process.cwd(), offersFile),
      brands_file: path.relative(process.cwd(), brandsFile),
      offers_count: offersRaw.length,
      brands_count: brandsRaw.length,
      category_allowlist: categories,
    },
    publish: {
      offers_selected: offersFiltered.length,
      brands_selected: brandsFiltered.length,
      brands_upserted: 0,
      offers_upserted: 0,
    },
  }

  if (!dryRun) {
    const pool = createPool()
    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      outputSummary.publish.brands_upserted = await upsertBrands(client, brandsFiltered)
      outputSummary.publish.offers_upserted = await upsertOffers(client, offersFiltered)
      await client.query('COMMIT')
    } catch (error) {
      await client.query('ROLLBACK')
      throw error
    } finally {
      client.release()
      await pool.end()
    }

    if (syncInventory) {
      await runPostSync(syncScope)
    }
  }

  await ensureDir(OFFERS_REPORT_DIR)
  const summaryPath = path.join(OFFERS_REPORT_DIR, `offers-publish-supabase-${tag}.summary.json`)
  const latestSummaryPath = path.join(OFFERS_REPORT_DIR, 'offers-publish-supabase-latest.summary.json')
  await writeJson(summaryPath, outputSummary)
  await writeJson(latestSummaryPath, outputSummary)

  process.stdout.write(`${JSON.stringify({
    ok: true,
    dryRun,
    offersSelected: offersFiltered.length,
    brandsSelected: brandsFiltered.length,
    brandsUpserted: outputSummary.publish.brands_upserted,
    offersUpserted: outputSummary.publish.offers_upserted,
    summaryFile: path.relative(process.cwd(), summaryPath),
  }, null, 2)}\n`)
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error('[publish-offers-to-supabase] failed:', error?.message || error)
    process.exit(1)
  })
}

export const __publishSupabaseInternal = Object.freeze({
  normalizeBrandRow,
  normalizeOfferRow,
})
