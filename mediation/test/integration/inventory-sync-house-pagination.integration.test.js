import assert from 'node:assert/strict'
import test from 'node:test'

import { syncInventoryNetworks } from '../../src/runtime/inventory-sync.js'

function makeHouseRow(index) {
  const id = String(index + 1).padStart(5, '0')
  return {
    offer_id: `house_offer_${id}`,
    campaign_id: `campaign_${id}`,
    brand_id: `brand_${id}`,
    vertical_l1: 'electronics',
    vertical_l2: 'audio',
    market: 'US',
    title: `Offer ${id}`,
    description: `Description ${id}`,
    snippet: `Snippet ${id}`,
    target_url: `https://example.com/${id}`,
    image_url: `https://cdn.example.com/${id}.png`,
    status: 'active',
    language: 'en',
    disclosure: 'Sponsored',
    source_type: 'crawler',
    confidence_score: 0.8,
    product_id: `product_${id}`,
    merchant: `Merchant ${id}`,
    price: 129.99,
    original_price: 159.99,
    currency: 'USD',
    discount_pct: 20,
    availability: 'in_stock',
    tags_json: ['electronics'],
  }
}

function createMockPool(rows = []) {
  let rawUpserts = 0
  let normUpserts = 0
  const syncRuns = []
  const normMetadata = []
  return {
    getStats() {
      return { rawUpserts, normUpserts, syncRuns, normMetadata }
    },
    async query(sql, params = []) {
      const text = String(sql || '')
      if (text.includes('FROM house_ads_offers')) {
        const cursor = String(params[3] || '')
        const limit = Number(params[4] || 0)
        const scoped = rows
          .filter((row) => String(row.offer_id) > cursor)
          .sort((a, b) => String(a.offer_id).localeCompare(String(b.offer_id)))
          .slice(0, limit)
        return { rows: scoped }
      }
      if (text.includes('INSERT INTO offer_inventory_sync_runs')) {
        syncRuns.push({
          runId: params[0],
          network: params[1],
          status: params[2],
        })
        return { rows: [] }
      }
      if (text.includes('INSERT INTO offer_inventory_raw')) {
        rawUpserts += 1
        return { rows: [] }
      }
      if (text.includes('INSERT INTO offer_inventory_norm')) {
        normUpserts += 1
        try {
          normMetadata.push(JSON.parse(String(params[15] || '{}')))
        } catch {
          normMetadata.push({})
        }
        return { rows: [] }
      }
      return { rows: [] }
    },
  }
}

test('syncInventoryNetworks house supports multi-page sync beyond 500 records', async () => {
  const rows = Array.from({ length: 2300 }, (_, index) => makeHouseRow(index))
  const pool = createMockPool(rows)

  const result = await syncInventoryNetworks(pool, {
    networks: ['house'],
    limit: 2300,
    market: 'US',
    language: 'en-US',
    trigger: 'test_house_pagination',
  })

  assert.equal(result.ok, true)
  assert.equal(result.results.length, 1)
  assert.equal(result.results[0].network, 'house')
  assert.equal(result.results[0].status, 'success')
  assert.equal(result.results[0].fetchedCount, 2300)
  assert.equal(result.results[0].upsertedCount, 2300)
  assert.equal(result.results[0].errorCount, 0)

  const stats = pool.getStats()
  assert.equal(stats.rawUpserts, 2300)
  assert.equal(stats.normUpserts, 2300)
  assert.equal(stats.normMetadata.length, 2300)
  assert.equal(typeof stats.normMetadata[0]?.price_missing, 'boolean')
  assert.equal(stats.normMetadata[0]?.price, 129.99)
  assert.equal(stats.normMetadata[0]?.originalPrice, 159.99)
  assert.equal(stats.normMetadata[0]?.currency, 'USD')
})
