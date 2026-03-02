import assert from 'node:assert/strict'
import test from 'node:test'

import {
  derivePartnerStackBrandId,
  derivePartnerStackCampaignId,
  mapPartnerStackPartnershipToUnifiedOffer,
} from '../../src/offers/network-mappers.js'

test('partnerstack mapping: derives stable campaignId/brandId from partnership key and destination', () => {
  const record = {
    key: 'AWeSome-Partner_001',
    company: {
      name: 'Example Partner',
    },
    link: {
      destination: 'https://www.example.com/pricing?utm_source=test',
      stack_key: 'stk_001',
    },
  }

  const campaignId = derivePartnerStackCampaignId(record)
  const brandId = derivePartnerStackBrandId(record)
  const mapped = mapPartnerStackPartnershipToUnifiedOffer(record)

  assert.equal(campaignId, 'campaign_partnerstack_awesome_partner_001')
  assert.equal(brandId, 'brand_partnerstack_example_com')
  assert.equal(mapped.metadata.campaignId, campaignId)
  assert.equal(mapped.metadata.brandId, brandId)
})

test('partnerstack mapping: uses hash fallback when key/domain are missing', () => {
  const record = {
    company: {
      name: 'No Key Partner',
    },
    title: 'Fallback Campaign',
    destination_url: 'not_a_valid_url',
    link: {
      destination: 'not_a_valid_url',
      stack_key: 'stack_fallback_001',
    },
  }

  const firstCampaignId = derivePartnerStackCampaignId(record)
  const secondCampaignId = derivePartnerStackCampaignId(record)
  const firstBrandId = derivePartnerStackBrandId(record)
  const secondBrandId = derivePartnerStackBrandId(record)

  assert.equal(firstCampaignId.startsWith('campaign_partnerstack_'), true)
  assert.equal(firstBrandId.startsWith('brand_partnerstack_'), true)
  assert.equal(firstCampaignId, secondCampaignId)
  assert.equal(firstBrandId, secondBrandId)
})
