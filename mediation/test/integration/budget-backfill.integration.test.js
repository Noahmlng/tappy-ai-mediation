import assert from 'node:assert/strict'
import test from 'node:test'

import { __budgetBackfillInternal } from '../../scripts/budget/backfill-campaign-budgets.js'

const {
  deriveCampaignIdFromInventoryRow,
  repairPartnerstackMetadataRow,
  classifyTier,
  resolveBudgetByTier,
  buildBudgetPlanFromInventoryRows,
} = __budgetBackfillInternal

test('budget backfill: classifyTier and resolveBudgetByTier follow balanced profile', () => {
  assert.equal(classifyTier(0), 'cold')
  assert.equal(classifyTier(1), 'warm')
  assert.equal(classifyTier(10), 'hot')

  assert.deepEqual(resolveBudgetByTier('house', 'hot'), { dailyBudgetUsd: 120, lifetimeBudgetUsd: 3000 })
  assert.deepEqual(resolveBudgetByTier('house', 'warm'), { dailyBudgetUsd: 60, lifetimeBudgetUsd: 1200 })
  assert.deepEqual(resolveBudgetByTier('house', 'cold'), { dailyBudgetUsd: 25, lifetimeBudgetUsd: 500 })

  assert.deepEqual(resolveBudgetByTier('partnerstack', 'hot'), { dailyBudgetUsd: 60, lifetimeBudgetUsd: 1500 })
  assert.deepEqual(resolveBudgetByTier('partnerstack', 'warm'), { dailyBudgetUsd: 30, lifetimeBudgetUsd: 600 })
  assert.deepEqual(resolveBudgetByTier('partnerstack', 'cold'), { dailyBudgetUsd: 12, lifetimeBudgetUsd: 240 })

  assert.deepEqual(resolveBudgetByTier('unknown', 'cold'), { dailyBudgetUsd: 20, lifetimeBudgetUsd: 400 })
})

test('budget backfill: repairPartnerstackMetadataRow fills missing campaignId/brandId', () => {
  const row = {
    offer_id: 'partner_offer_001',
    network: 'partnerstack',
    upstream_offer_id: 'ps_key_100',
    title: 'Partner Offer',
    target_url: 'https://partner.example.com/deal',
    metadata: {
      partnershipKey: 'ps_key_100',
      stackKey: 'stk_100',
      destinationUrl: 'https://partner.example.com/deal',
    },
  }

  const repaired = repairPartnerstackMetadataRow(row)
  assert.equal(repaired.changed, true)
  assert.equal(repaired.campaignId, 'campaign_partnerstack_ps_key_100')
  assert.equal(repaired.brandId, 'brand_partnerstack_partner_example_com')
  assert.equal(repaired.metadata.campaignId, 'campaign_partnerstack_ps_key_100')
  assert.equal(repaired.metadata.brandId, 'brand_partnerstack_partner_example_com')
})

test('budget backfill: buildBudgetPlanFromInventoryRows reaches full projected coverage when unlocked', () => {
  const inventoryRows = [
    {
      offer_id: 'house_offer_1',
      network: 'house',
      upstream_offer_id: 'h_1',
      target_url: 'https://house.example.com/1',
      metadata: { campaignId: 'campaign_house_hot_1' },
    },
    {
      offer_id: 'ps_offer_1',
      network: 'partnerstack',
      upstream_offer_id: 'ps_key_1',
      title: 'Partner A',
      target_url: 'https://partner-a.example.com/path',
      metadata: {
        partnershipKey: 'ps_key_1',
        destinationUrl: 'https://partner-a.example.com/path',
      },
    },
    {
      offer_id: 'cj_offer_1',
      network: 'cj',
      upstream_offer_id: 'cj_1',
      target_url: 'https://cj.example.com/offer',
      metadata: { advertiserId: 'campaign_cj_1' },
    },
  ]

  const servedCountByCampaign = {
    campaign_house_hot_1: 18,
    campaign_partnerstack_ps_key_1: 3,
    campaign_cj_1: 0,
  }

  const existingByCampaign = {
    campaign_house_hot_1: {
      hasBudget: true,
      metadata: {},
    },
    campaign_partnerstack_ps_key_1: {
      hasBudget: false,
      metadata: {},
    },
    campaign_cj_1: {
      hasBudget: false,
      metadata: {},
    },
  }

  const built = buildBudgetPlanFromInventoryRows(inventoryRows, {
    servedCountByCampaign,
    existingByCampaign,
  })

  assert.equal(built.summary.totalCampaigns, 3)
  assert.equal(built.summary.coveredBeforeCount, 1)
  assert.equal(built.summary.coveredAfterProjectedCount, 3)
  assert.equal(built.summary.uncoveredAfterProjected.length, 0)

  const housePlan = built.plans.find((item) => item.campaignId === 'campaign_house_hot_1')
  assert.equal(housePlan.tier, 'hot')
  assert.equal(housePlan.dailyBudgetUsd, 120)
  assert.equal(housePlan.lifetimeBudgetUsd, 3000)

  const psPlan = built.plans.find((item) => item.campaignId === 'campaign_partnerstack_ps_key_1')
  assert.equal(psPlan.tier, 'warm')
  assert.equal(psPlan.dailyBudgetUsd, 30)
  assert.equal(psPlan.lifetimeBudgetUsd, 600)

  const cjPlan = built.plans.find((item) => item.campaignId === 'campaign_cj_1')
  assert.equal(cjPlan.tier, 'cold')
  assert.equal(cjPlan.dailyBudgetUsd, 20)
  assert.equal(cjPlan.lifetimeBudgetUsd, 400)
})

test('budget backfill: locked campaigns are skipped and stay uncovered in projected report', () => {
  const inventoryRows = [
    {
      offer_id: 'ps_offer_locked',
      network: 'partnerstack',
      upstream_offer_id: 'ps_locked_key',
      title: 'Locked Partner',
      target_url: 'https://locked.example.com',
      metadata: {
        partnershipKey: 'ps_locked_key',
        destinationUrl: 'https://locked.example.com',
      },
    },
  ]

  const campaignId = deriveCampaignIdFromInventoryRow(inventoryRows[0])

  const built = buildBudgetPlanFromInventoryRows(inventoryRows, {
    servedCountByCampaign: {
      [campaignId]: 2,
    },
    existingByCampaign: {
      [campaignId]: {
        hasBudget: false,
        metadata: {
          budgetLocked: true,
        },
      },
    },
  })

  assert.equal(built.summary.totalCampaigns, 1)
  assert.equal(built.summary.skipLockedCount, 1)
  assert.equal(built.summary.coveredAfterProjectedCount, 0)
  assert.equal(built.summary.uncoveredAfterProjected.length, 1)
  assert.equal(built.plans[0].action, 'skip_locked')
})
