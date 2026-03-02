import { createHash } from 'node:crypto'

import { loadRuntimeConfig } from '../config/runtime-config.js'
import { createCjConnector } from '../connectors/cj/index.js'
import { createPartnerStackConnector } from '../connectors/partnerstack/index.js'
import { normalizeUnifiedOffers } from '../offers/index.js'
import { buildTextEmbedding, vectorToSqlLiteral } from './embedding.js'

function cleanText(value) {
  return String(value || '').trim().replace(/\s+/g, ' ')
}

function toFiniteNumber(value, fallback = 0) {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  return n
}

function nowIso() {
  return new Date().toISOString()
}

function createId(prefix = 'id') {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
}

function sha256(text = '') {
  return createHash('sha256').update(String(text)).digest('hex')
}

function normalizeTags(value) {
  if (!Array.isArray(value)) return []
  return Array.from(new Set(
    value
      .map((item) => cleanText(item).toLowerCase())
      .filter(Boolean),
  ))
}

function clipText(value, maxLength = 2000) {
  const text = cleanText(value)
  if (!text) return ''
  return text.length <= maxLength ? text : text.slice(0, maxLength)
}

function extractUrlTokens(url = '') {
  const raw = cleanText(url)
  if (!raw) return []
  let parsed
  try {
    parsed = new URL(raw)
  } catch {
    return []
  }
  const hostTokens = String(parsed.hostname || '')
    .toLowerCase()
    .split(/[^a-z0-9]+/g)
    .filter((item) => item.length >= 2)
  const pathTokens = String(parsed.pathname || '')
    .toLowerCase()
    .split(/[^a-z0-9]+/g)
    .filter((item) => item.length >= 2)
  return Array.from(new Set([...hostTokens, ...pathTokens]))
}

function collectMetadataTextSegments(value, out = [], dedupe = new Set(), depth = 0, limit = 64) {
  if (out.length >= limit || depth > 2) return
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    const text = cleanText(value)
    if (!text) return
    const key = text.toLowerCase()
    if (dedupe.has(key)) return
    dedupe.add(key)
    out.push(text)
    return
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      if (out.length >= limit) break
      collectMetadataTextSegments(item, out, dedupe, depth + 1, limit)
    }
    return
  }
  if (!value || typeof value !== 'object') return
  for (const [key, nested] of Object.entries(value)) {
    if (out.length >= limit) break
    if (String(key || '').toLowerCase() === 'retrievaltext') continue
    collectMetadataTextSegments(nested, out, dedupe, depth + 1, limit)
  }
}

function buildRetrievalTextFromParts(input = {}) {
  const metadata = input?.metadata && typeof input.metadata === 'object' ? input.metadata : {}
  const tags = Array.isArray(input.tags) ? input.tags : []
  const metadataDirect = [
    metadata.brand,
    metadata.brandName,
    metadata.brand_name,
    metadata.brandId,
    metadata.brand_id,
    metadata.merchant,
    metadata.merchantName,
    metadata.merchant_name,
    metadata.productName,
    metadata.product_name,
    metadata.category,
    metadata.verticalL1,
    metadata.vertical_l1,
    metadata.verticalL2,
    metadata.vertical_l2,
    metadata.useCase,
    metadata.use_case,
    metadata.solution,
  ]
  const metadataNested = []
  collectMetadataTextSegments(metadata, metadataNested, new Set(), 0, 64)
  const segments = [
    cleanText(input.title),
    cleanText(input.description),
    ...tags.map((item) => cleanText(item)),
    ...metadataDirect.map((item) => cleanText(item)),
    ...metadataNested,
    ...extractUrlTokens(input.targetUrl),
  ].filter(Boolean)
  return clipText(Array.from(new Set(segments)).join(' '), 2000)
}

function normalizeHouseLocale(value) {
  return cleanText(value).toLowerCase().replace(/_/g, '-')
}

function normalizeHouseMarket(value) {
  return cleanText(value).toUpperCase()
}

function toHouseNumber(value, fallback = 0) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function normalizeHouseAvailability(status, availability) {
  const normalizedStatus = cleanText(status).toLowerCase()
  const normalizedAvailability = cleanText(availability).toLowerCase()
  if (normalizedStatus === 'active') return 'active'
  if (normalizedAvailability === 'active') return 'active'
  return 'active'
}

function mapHouseOfferRowToUnifiedOffer(row = {}) {
  const offerId = cleanText(row.offer_id)
  const title = cleanText(row.title)
  const targetUrl = cleanText(row.target_url)
  const description = cleanText(row.snippet || row.description)
  const merchant = cleanText(row.merchant)
  const market = cleanText(row.market)
  const locale = cleanText(row.language)
  const currency = cleanText(row.currency)
  const verticalL1 = cleanText(row.vertical_l1)
  const verticalL2 = cleanText(row.vertical_l2)
  const imageUrl = cleanText(row.image_url)
  const productId = cleanText(row.product_id)
  const price = toHouseNumber(row.price, NaN)
  const originalPrice = toHouseNumber(row.original_price, NaN)
  const tags = Array.isArray(row.tags_json)
    ? row.tags_json.map((item) => cleanText(String(item || ''))).filter(Boolean)
    : []

  if (!offerId || !title || !targetUrl) return null

  return {
    sourceNetwork: 'house',
    sourceType: 'product',
    sourceId: offerId,
    offerId: `house:product:${offerId}`,
    title,
    description,
    targetUrl,
    trackingUrl: targetUrl,
    merchantName: merchant,
    productName: title,
    entityText: merchant || title,
    entityType: 'product',
    locale,
    market,
    currency,
    availability: normalizeHouseAvailability(row.status, row.availability),
    qualityScore: toHouseNumber(row.confidence_score, 0),
    bidValue: toHouseNumber(row.discount_pct, 0),
    metadata: {
      intentCardItemId: productId || offerId,
      campaignId: cleanText(row.campaign_id),
      creativeId: '',
      brandId: cleanText(row.brand_id),
      category: verticalL2 || verticalL1,
      verticalL1,
      verticalL2,
      matchTags: tags,
      sourceType: cleanText(row.source_type),
      placementKey: 'next_step.intent_card',
      disclosure: cleanText(row.disclosure),
      price: Number.isFinite(price) && price > 0 ? Number(price.toFixed(2)) : undefined,
      originalPrice:
        Number.isFinite(originalPrice) && originalPrice > 0
          ? Number(originalPrice.toFixed(2))
          : undefined,
      currency: currency || 'USD',
      price_missing: !(Number.isFinite(price) && price > 0),
      image_url: imageUrl,
      imageUrl,
    },
    raw: row,
  }
}

async function fetchHouseOffersForSync(pool, options = {}) {
  const requestedLimit = Math.max(80, Math.floor(toFiniteNumber(options.limit, 2000)))
  const batchSize = Math.max(200, Math.min(2000, requestedLimit))
  const market = normalizeHouseMarket(options.market || 'US')
  const locale = normalizeHouseLocale(options.language || 'en-US')
  const localePrefix = locale ? locale.split('-')[0] : ''

  const out = []
  let cursor = ''
  while (out.length < requestedLimit) {
    const remaining = requestedLimit - out.length
    const fetchLimit = Math.max(1, Math.min(batchSize, remaining))
    const result = await pool.query(
      `
        SELECT
          offer_id,
          campaign_id,
          brand_id,
          vertical_l1,
          vertical_l2,
          market,
          title,
          description,
          snippet,
          target_url,
          image_url,
          status,
          language,
          disclosure,
          source_type,
          confidence_score,
          product_id,
          merchant,
          currency,
          price,
          original_price,
          discount_pct,
          availability,
          tags_json
        FROM house_ads_offers
        WHERE offer_type = 'product'
          AND status = 'active'
          AND ($1::text = '' OR market = $1 OR market = '')
          AND (
            $2::text = ''
            OR lower(language) = lower($2)
            OR lower(split_part(language, '-', 1)) = lower($3)
            OR language = ''
          )
          AND ($4::text = '' OR offer_id > $4::text)
        ORDER BY offer_id ASC
        LIMIT $5
      `,
      [market, locale, localePrefix, cursor, fetchLimit],
    )
    const rows = Array.isArray(result.rows) ? result.rows : []
    if (rows.length === 0) break
    for (const row of rows) {
      const mapped = mapHouseOfferRowToUnifiedOffer(row)
      if (!mapped) continue
      out.push(mapped)
      if (out.length >= requestedLimit) break
    }
    const tail = rows[rows.length - 1]
    cursor = cleanText(tail?.offer_id)
    if (!cursor) break
  }

  return out
}

function makeRawRecordId(network, offer = {}) {
  const seed = `${network}|${cleanText(offer.offerId)}|${cleanText(offer.sourceId)}|${cleanText(offer.targetUrl)}`
  return `raw_${sha256(seed).slice(0, 24)}`
}

function toRawPayload(offer = {}) {
  return offer?.raw && typeof offer.raw === 'object' ? offer.raw : offer
}

function toNormalizedInventoryRow(network, offer = {}) {
  const sourceMetadata = offer?.metadata && typeof offer.metadata === 'object' ? offer.metadata : {}
  const metadata = { ...sourceMetadata }
  const merchantName = cleanText(offer.merchantName)
  if (!cleanText(metadata.merchant) && merchantName) {
    metadata.merchant = merchantName
  }
  if (!cleanText(metadata.merchantName) && merchantName) {
    metadata.merchantName = merchantName
  }
  const tags = normalizeTags([
    ...(Array.isArray(metadata.matchTags) ? metadata.matchTags : []),
    ...(Array.isArray(metadata.tags) ? metadata.tags : []),
    cleanText(metadata.category),
    cleanText(offer.entityText),
  ])
  const retrievalText = buildRetrievalTextFromParts({
    title: offer.title,
    description: offer.description,
    targetUrl: offer.targetUrl,
    tags,
    metadata,
  })
  if (retrievalText) {
    metadata.retrievalText = retrievalText
    metadata.retrievalTextVersion = 'v1_enriched_2026_03_02'
  }

  return {
    offerId: cleanText(offer.offerId),
    network,
    upstreamOfferId: cleanText(offer.sourceId),
    sourceType: cleanText(offer.sourceType || 'offer') || 'offer',
    title: cleanText(offer.title),
    description: cleanText(offer.description),
    targetUrl: cleanText(offer.targetUrl),
    market: cleanText(offer.market || 'US') || 'US',
    language: cleanText(offer.locale || 'en-US') || 'en-US',
    availability: cleanText(offer.availability || 'active') || 'active',
    quality: toFiniteNumber(offer.qualityScore, 0),
    bidHint: Math.max(0, toFiniteNumber(offer.bidValue, 0)),
    policyWeight: toFiniteNumber(metadata.policyWeight, 0),
    freshnessAt: cleanText(offer.updatedAt) || null,
    tags,
    metadata,
  }
}

async function upsertSyncRun(pool, row = {}) {
  await pool.query(
    `
      INSERT INTO offer_inventory_sync_runs (
        run_id,
        network,
        status,
        fetched_count,
        upserted_count,
        error_count,
        started_at,
        finished_at,
        metadata
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7::timestamptz, $8::timestamptz, $9::jsonb)
      ON CONFLICT (run_id) DO UPDATE
      SET
        status = EXCLUDED.status,
        fetched_count = EXCLUDED.fetched_count,
        upserted_count = EXCLUDED.upserted_count,
        error_count = EXCLUDED.error_count,
        finished_at = EXCLUDED.finished_at,
        metadata = EXCLUDED.metadata
    `,
    [
      cleanText(row.runId),
      cleanText(row.network),
      cleanText(row.status),
      Math.max(0, Math.floor(toFiniteNumber(row.fetchedCount, 0))),
      Math.max(0, Math.floor(toFiniteNumber(row.upsertedCount, 0))),
      Math.max(0, Math.floor(toFiniteNumber(row.errorCount, 0))),
      cleanText(row.startedAt) || nowIso(),
      cleanText(row.finishedAt) || null,
      JSON.stringify(row.metadata && typeof row.metadata === 'object' ? row.metadata : {}),
    ],
  )
}

async function upsertInventoryRows(pool, network, offers = [], options = {}) {
  const normalized = normalizeUnifiedOffers(offers)
  const rows = normalized
    .map((offer) => toNormalizedInventoryRow(network, offer))
    .filter((item) => item.offerId && item.title && item.targetUrl)

  let upsertedCount = 0
  const errors = []
  const fetchedAt = cleanText(options.fetchedAt) || nowIso()

  for (const row of rows) {
    try {
      const rawPayload = toRawPayload(normalized.find((item) => cleanText(item.offerId) === row.offerId) || row)
      const payloadDigest = sha256(JSON.stringify(rawPayload))
      const rawRecordId = makeRawRecordId(network, row)

      await pool.query(
        `
          INSERT INTO offer_inventory_raw (
            raw_record_id,
            network,
            upstream_offer_id,
            fetched_at,
            payload_digest,
            payload_json,
            created_at
          )
          VALUES ($1, $2, $3, $4::timestamptz, $5, $6::jsonb, NOW())
          ON CONFLICT (raw_record_id) DO UPDATE
          SET
            fetched_at = EXCLUDED.fetched_at,
            payload_digest = EXCLUDED.payload_digest,
            payload_json = EXCLUDED.payload_json
        `,
        [
          rawRecordId,
          network,
          row.upstreamOfferId,
          fetchedAt,
          payloadDigest,
          JSON.stringify(rawPayload),
        ],
      )

      await pool.query(
        `
          INSERT INTO offer_inventory_norm (
            offer_id,
            network,
            upstream_offer_id,
            source_type,
            title,
            description,
            target_url,
            market,
            language,
            availability,
            quality,
            bid_hint,
            policy_weight,
            freshness_at,
            tags,
            metadata,
            raw_record_id,
            imported_at,
            updated_at
          )
          VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11, $12, $13,
            $14::timestamptz, $15::text[], $16::jsonb, $17, NOW(), NOW()
          )
          ON CONFLICT (offer_id) DO UPDATE
          SET
            network = EXCLUDED.network,
            upstream_offer_id = EXCLUDED.upstream_offer_id,
            source_type = EXCLUDED.source_type,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            target_url = EXCLUDED.target_url,
            market = EXCLUDED.market,
            language = EXCLUDED.language,
            availability = EXCLUDED.availability,
            quality = EXCLUDED.quality,
            bid_hint = EXCLUDED.bid_hint,
            policy_weight = EXCLUDED.policy_weight,
            freshness_at = EXCLUDED.freshness_at,
            tags = EXCLUDED.tags,
            metadata = EXCLUDED.metadata,
            raw_record_id = EXCLUDED.raw_record_id,
            updated_at = NOW()
        `,
        [
          row.offerId,
          row.network,
          row.upstreamOfferId,
          row.sourceType,
          row.title,
          row.description,
          row.targetUrl,
          row.market,
          row.language,
          row.availability,
          row.quality,
          row.bidHint,
          row.policyWeight,
          row.freshnessAt,
          row.tags,
          JSON.stringify(row.metadata),
          rawRecordId,
        ],
      )

      upsertedCount += 1
    } catch (error) {
      errors.push({
        offerId: row.offerId,
        message: error instanceof Error ? error.message : 'inventory_upsert_failed',
      })
    }
  }

  return {
    fetchedCount: offers.length,
    normalizedCount: rows.length,
    upsertedCount,
    errorCount: errors.length,
    errors,
  }
}

async function fetchNetworkOffers(pool, network, runtimeConfig, options = {}) {
  if (network === 'partnerstack') {
    const connector = createPartnerStackConnector({
      runtimeConfig,
      timeoutMs: Math.max(1500, Math.floor(toFiniteNumber(options.timeoutMs, 8000))),
      maxRetries: 1,
    })
    const result = await connector.fetchOffers({
      limit: Math.max(20, Math.floor(toFiniteNumber(options.limit, 240))),
      limitPartnerships: Math.max(20, Math.floor(toFiniteNumber(options.limit, 240))),
      limitLinksPerPartnership: Math.max(20, Math.floor(toFiniteNumber(options.linkLimit, 40))),
      search: cleanText(options.search),
    })
    return Array.isArray(result?.offers) ? result.offers : []
  }

  if (network === 'cj') {
    const connector = createCjConnector({
      runtimeConfig,
      timeoutMs: Math.max(1500, Math.floor(toFiniteNumber(options.timeoutMs, 8000))),
      maxRetries: 1,
    })
    const result = await connector.fetchOffers({
      keywords: cleanText(options.search),
      limit: Math.max(20, Math.floor(toFiniteNumber(options.limit, 200))),
      page: 1,
    })
    return Array.isArray(result?.offers) ? result.offers : []
  }

  if (network === 'house') {
    return await fetchHouseOffersForSync(pool, options)
  }

  return []
}

async function syncOneNetwork(pool, network, options = {}) {
  const runId = createId(`invsync_${network}`)
  const startedAt = nowIso()
  await upsertSyncRun(pool, {
    runId,
    network,
    status: 'running',
    fetchedCount: 0,
    upsertedCount: 0,
    errorCount: 0,
    startedAt,
    metadata: {
      trigger: cleanText(options.trigger) || 'manual',
    },
  })

  try {
    const runtimeConfig = options.runtimeConfig || loadRuntimeConfig(process.env, { strict: false })
    const offers = await fetchNetworkOffers(pool, network, runtimeConfig, options)
    const stats = await upsertInventoryRows(pool, network, offers, {
      fetchedAt: startedAt,
    })

    const status = stats.errorCount > 0 ? 'partial' : 'success'
    await upsertSyncRun(pool, {
      runId,
      network,
      status,
      fetchedCount: stats.fetchedCount,
      upsertedCount: stats.upsertedCount,
      errorCount: stats.errorCount,
      startedAt,
      finishedAt: nowIso(),
      metadata: {
        normalizedCount: stats.normalizedCount,
        errors: stats.errors.slice(0, 25),
      },
    })

    return {
      runId,
      network,
      status,
      ...stats,
    }
  } catch (error) {
    await upsertSyncRun(pool, {
      runId,
      network,
      status: 'failed',
      fetchedCount: 0,
      upsertedCount: 0,
      errorCount: 1,
      startedAt,
      finishedAt: nowIso(),
      metadata: {
        message: error instanceof Error ? error.message : 'sync_failed',
      },
    })

    return {
      runId,
      network,
      status: 'failed',
      fetchedCount: 0,
      normalizedCount: 0,
      upsertedCount: 0,
      errorCount: 1,
      errors: [{ message: error instanceof Error ? error.message : 'sync_failed' }],
    }
  }
}

export async function syncInventoryNetworks(pool, input = {}) {
  if (!pool) {
    throw new Error('syncInventoryNetworks requires a postgres pool')
  }
  const networks = Array.isArray(input.networks) && input.networks.length > 0
    ? input.networks
    : ['partnerstack', 'cj', 'house']

  const results = []
  for (const network of networks) {
    const normalized = cleanText(network).toLowerCase()
    if (!['partnerstack', 'cj', 'house'].includes(normalized)) continue
    const result = await syncOneNetwork(pool, normalized, input)
    results.push(result)
  }

  return {
    ok: results.every((item) => item.status === 'success' || item.status === 'partial'),
    results,
    syncedAt: nowIso(),
  }
}

export async function buildInventoryEmbeddings(pool, input = {}) {
  if (!pool) {
    throw new Error('buildInventoryEmbeddings requires a postgres pool')
  }

  const normalizedOfferIds = Array.isArray(input.offerIds)
    ? Array.from(new Set(input.offerIds.map((item) => cleanText(item)).filter(Boolean)))
    : []
  const fullRebuild = input.fullRebuild === true
  const batchSize = Math.max(100, Math.floor(toFiniteNumber(input.batchSize, 5000)))
  const limit = Math.max(1, Math.floor(toFiniteNumber(input.limit, 5000)))
  const upsertBatchSize = Math.max(20, Math.floor(toFiniteNumber(
    input.upsertBatchSize ?? input.upsert_batch_size,
    200,
  )))

  async function upsertEmbeddingRows(rows = []) {
    let upserted = 0
    for (let start = 0; start < rows.length; start += upsertBatchSize) {
      const chunk = rows.slice(start, start + upsertBatchSize)
      if (chunk.length === 0) continue

      const params = []
      const valuesSql = chunk.map((row, index) => {
        const metadata = row?.metadata && typeof row.metadata === 'object' ? row.metadata : {}
        const retrievalText = buildRetrievalTextFromParts({
          title: row.title,
          description: row.description,
          targetUrl: row.target_url,
          tags: Array.isArray(row.tags) ? row.tags : [],
          metadata,
        })
        const embedding = buildTextEmbedding({
          title: row.title,
          description: row.description,
          tags: Array.isArray(row.tags) ? row.tags : [],
          retrievalText,
        })
        const base = index * 3
        params.push(
          cleanText(row.offer_id),
          embedding.model,
          vectorToSqlLiteral(embedding.vector),
        )
        return `($${base + 1}, $${base + 2}, $${base + 3}::vector, NOW(), NOW())`
      }).join(',\n            ')

      await pool.query(
        `
          INSERT INTO offer_inventory_embeddings (
            offer_id,
            embedding_model,
            embedding,
            embedding_updated_at,
            created_at
          )
          VALUES
            ${valuesSql}
          ON CONFLICT (offer_id) DO UPDATE
          SET
            embedding_model = EXCLUDED.embedding_model,
            embedding = EXCLUDED.embedding,
            embedding_updated_at = NOW()
        `,
        params,
      )
      upserted += chunk.length
    }
    return upserted
  }

  if (normalizedOfferIds.length > 0) {
    const result = await pool.query(
      `
        SELECT offer_id, title, description, tags, target_url, metadata
        FROM offer_inventory_norm
        WHERE availability = 'active'
          AND offer_id = ANY($1::text[])
      `,
      [normalizedOfferIds],
    )
    const rows = Array.isArray(result.rows) ? result.rows : []
    const upserted = await upsertEmbeddingRows(rows)
    return {
      ok: true,
      mode: 'offer_ids',
      scanned: rows.length,
      upserted,
      embeddedAt: nowIso(),
    }
  }

  if (fullRebuild) {
    let cursor = cleanText(input.cursor)
    let scanned = 0
    let upserted = 0
    let batches = 0

    while (true) {
      const result = await pool.query(
        `
          SELECT offer_id, title, description, tags, target_url, metadata
          FROM offer_inventory_norm
          WHERE availability = 'active'
            AND ($1::text = '' OR offer_id > $1::text)
          ORDER BY offer_id ASC
          LIMIT $2
        `,
        [cursor, batchSize],
      )
      const rows = Array.isArray(result.rows) ? result.rows : []
      if (rows.length === 0) break
      batches += 1
      scanned += rows.length
      upserted += await upsertEmbeddingRows(rows)
      cursor = cleanText(rows[rows.length - 1]?.offer_id)
      if (!cursor) break
    }

    return {
      ok: true,
      mode: 'full_rebuild',
      scanned,
      upserted,
      batches,
      embeddedAt: nowIso(),
    }
  }

  const result = await pool.query(
    `
      SELECT offer_id, title, description, tags, target_url, metadata
      FROM offer_inventory_norm
      WHERE availability = 'active'
      ORDER BY updated_at DESC
      LIMIT $1
    `,
    [limit],
  )
  const rows = Array.isArray(result.rows) ? result.rows : []
  const upserted = await upsertEmbeddingRows(rows)

  return {
    ok: true,
    mode: 'recent_limit',
    scanned: rows.length,
    upserted,
    embeddedAt: nowIso(),
  }
}

export async function materializeServingSnapshot(pool) {
  if (!pool) {
    throw new Error('materializeServingSnapshot requires a postgres pool')
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS offer_inventory_serving_snapshot (
      offer_id TEXT PRIMARY KEY,
      network TEXT NOT NULL,
      title TEXT NOT NULL,
      description TEXT NOT NULL,
      target_url TEXT NOT NULL,
      market TEXT NOT NULL,
      language TEXT NOT NULL,
      availability TEXT NOT NULL,
      quality NUMERIC(8, 4) NOT NULL,
      bid_hint NUMERIC(12, 6) NOT NULL,
      policy_weight NUMERIC(8, 4) NOT NULL,
      tags TEXT[] NOT NULL DEFAULT '{}'::text[],
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      embedding_model TEXT NOT NULL DEFAULT '',
      embedding vector(512),
      refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `)

  await pool.query('BEGIN')
  try {
    await pool.query('TRUNCATE TABLE offer_inventory_serving_snapshot')
    await pool.query(`
      INSERT INTO offer_inventory_serving_snapshot (
        offer_id,
        network,
        title,
        description,
        target_url,
        market,
        language,
        availability,
        quality,
        bid_hint,
        policy_weight,
        tags,
        metadata,
        embedding_model,
        embedding,
        refreshed_at
      )
      SELECT
        n.offer_id,
        n.network,
        n.title,
        n.description,
        n.target_url,
        n.market,
        n.language,
        n.availability,
        n.quality,
        n.bid_hint,
        n.policy_weight,
        n.tags,
        n.metadata,
        coalesce(e.embedding_model, ''),
        e.embedding,
        NOW()
      FROM offer_inventory_norm n
      LEFT JOIN offer_inventory_embeddings e ON e.offer_id = n.offer_id
      WHERE n.availability = 'active'
    `)
    await pool.query('COMMIT')
  } catch (error) {
    await pool.query('ROLLBACK')
    throw error
  }

  const countResult = await pool.query('SELECT COUNT(*)::int AS count FROM offer_inventory_serving_snapshot')
  const count = Number(countResult.rows?.[0]?.count || 0)

  return {
    ok: true,
    rows: count,
    refreshedAt: nowIso(),
  }
}

export async function getInventoryStatus(pool) {
  if (!pool) {
    return {
      ok: false,
      mode: 'inventory_store_unavailable',
      counts: [],
      latestRuns: [],
    }
  }

  const [countResult, runResult] = await Promise.all([
    pool.query(`
      SELECT network, COUNT(*)::int AS offer_count
      FROM offer_inventory_norm
      GROUP BY network
      ORDER BY network ASC
    `),
    pool.query(`
      SELECT run_id, network, status, fetched_count, upserted_count, error_count, started_at, finished_at, metadata
      FROM offer_inventory_sync_runs
      ORDER BY started_at DESC
      LIMIT 20
    `),
  ])

  return {
    ok: true,
    mode: 'postgres',
    counts: Array.isArray(countResult.rows) ? countResult.rows : [],
    latestRuns: Array.isArray(runResult.rows) ? runResult.rows : [],
    checkedAt: nowIso(),
  }
}

export const __inventorySyncInternal = {
  mapHouseOfferRowToUnifiedOffer,
  fetchHouseOffersForSync,
}
