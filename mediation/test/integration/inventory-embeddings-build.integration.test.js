import assert from 'node:assert/strict'
import test from 'node:test'

import { buildInventoryEmbeddings } from '../../src/runtime/inventory-sync.js'
import { buildTextEmbedding, vectorToSqlLiteral } from '../../src/runtime/embedding.js'

function createMockPool(rows = []) {
  const upserts = []
  return {
    upserts,
    async query(sql, params = []) {
      const normalizedSql = String(sql || '')
      if (normalizedSql.includes('INSERT INTO offer_inventory_embeddings')) {
        const count = Math.max(0, Math.floor(params.length / 3))
        for (let i = 0; i < count; i += 1) {
          const base = i * 3
          upserts.push({
            offerId: params[base],
            model: params[base + 1],
            vectorLiteral: params[base + 2],
          })
        }
        return { rows: [] }
      }

      if (normalizedSql.includes('offer_id = ANY')) {
        const ids = Array.isArray(params[0]) ? params[0] : []
        return {
          rows: rows.filter((row) => ids.includes(row.offer_id)),
        }
      }

      if (normalizedSql.includes('offer_id > $1::text')) {
        const cursor = String(params[0] || '')
        const limit = Number(params[1] || 0)
        const scoped = rows
          .filter((row) => row.offer_id > cursor)
          .sort((a, b) => String(a.offer_id).localeCompare(String(b.offer_id)))
          .slice(0, limit)
        return { rows: scoped }
      }

      if (normalizedSql.includes('ORDER BY updated_at DESC')) {
        const limit = Number(params[0] || 0)
        return {
          rows: rows.slice(0, limit),
        }
      }

      return { rows: [] }
    },
  }
}

test('buildInventoryEmbeddings supports offerIds incremental mode', async () => {
  const pool = createMockPool([
    {
      offer_id: 'offer_a',
      title: 'Offer A',
      description: 'Description A',
      tags: ['tag-a'],
    },
    {
      offer_id: 'offer_b',
      title: 'Offer B',
      description: 'Description B',
      tags: ['tag-b'],
    },
  ])

  const result = await buildInventoryEmbeddings(pool, {
    offerIds: ['offer_b'],
  })

  assert.equal(result.ok, true)
  assert.equal(result.mode, 'offer_ids')
  assert.equal(result.scanned, 1)
  assert.equal(result.upserted, 1)
  assert.equal(pool.upserts.length, 1)
  assert.equal(pool.upserts[0].offerId, 'offer_b')
})

test('buildInventoryEmbeddings supports full rebuild mode with paging', async () => {
  const pool = createMockPool([
    {
      offer_id: 'offer_001',
      title: 'Offer 1',
      description: 'Description 1',
      tags: ['tag-1'],
    },
    {
      offer_id: 'offer_002',
      title: 'Offer 2',
      description: 'Description 2',
      tags: ['tag-2'],
    },
    {
      offer_id: 'offer_003',
      title: 'Offer 3',
      description: 'Description 3',
      tags: ['tag-3'],
    },
  ])

  const result = await buildInventoryEmbeddings(pool, {
    fullRebuild: true,
    batchSize: 2,
  })

  assert.equal(result.ok, true)
  assert.equal(result.mode, 'full_rebuild')
  assert.equal(result.scanned, 3)
  assert.equal(result.upserted, 3)
  assert.equal(result.batches, 1)
  assert.equal(pool.upserts.length, 3)
})

test('buildInventoryEmbeddings enriches embedding text with metadata/url retrieval context', async () => {
  const pool = createMockPool([
    {
      offer_id: 'offer_meta_001',
      title: 'AI Dubbing Tool',
      description: 'Fast workflow',
      tags: ['video'],
      target_url: 'https://murf.ai/tools/video-dubbing',
      metadata: {
        brand: 'Murf AI',
        useCase: 'Chinese to English YouTube dubbing',
      },
    },
  ])

  const result = await buildInventoryEmbeddings(pool, {
    offerIds: ['offer_meta_001'],
  })

  assert.equal(result.ok, true)
  assert.equal(result.upserted, 1)
  assert.equal(pool.upserts.length, 1)

  const baselineLiteral = vectorToSqlLiteral(buildTextEmbedding({
    title: 'AI Dubbing Tool',
    description: 'Fast workflow',
    tags: ['video'],
  }).vector)
  assert.notEqual(pool.upserts[0].vectorLiteral, baselineLiteral)
})
