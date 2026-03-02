import assert from 'node:assert/strict'
import test from 'node:test'

import { __crawlOffersInternal } from '../../scripts/house-ads/crawl-offers.js'

test('extractMetaImage resolves og:image and normalizes absolute URL', () => {
  const html = `
    <html>
      <head>
        <meta property="og:image" content="/assets/hero.jpg" />
      </head>
    </html>
  `
  const imageUrl = __crawlOffersInternal.extractMetaImage(html, 'https://example.com/shop')
  assert.equal(imageUrl, 'https://example.com/assets/hero.jpg')
})

test('extractMetaImage falls back to img src when meta image is not an image URL', () => {
  const html = `
    <html>
      <head>
        <meta property="og:image" content="/share" />
      </head>
      <body>
        <img src="/images/card.webp" />
      </body>
    </html>
  `
  const imageUrl = __crawlOffersInternal.extractMetaImage(html, 'https://example.com/home')
  assert.equal(imageUrl, 'https://example.com/images/card.webp')
})

test('parseOfferLinks attaches page image to link candidates', () => {
  const html = `
    <a href="/sale">Big Summer Sale</a>
    <a href="/about">About Us</a>
  `
  const rows = __crawlOffersInternal.parseOfferLinks(
    html,
    'https://example.com',
    'https://example.com/images/og.jpg',
  )

  assert.equal(rows.length, 1)
  assert.equal(rows[0].target_url, 'https://example.com/sale')
  assert.equal(rows[0].image_url, 'https://example.com/images/og.jpg')
})
