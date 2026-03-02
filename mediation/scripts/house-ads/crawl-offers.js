#!/usr/bin/env node
import path from 'node:path'
import {
  parseArgs,
  toInteger,
  readJson,
  readJsonl,
  writeJson,
  writeJsonl,
  ensureDir,
  cleanText,
  normalizeUrl,
  fetchWithTimeout,
  extractHtmlTitle,
  extractMetaDescription,
  extractH1,
  stripHtmlText,
  asyncPool,
  hashId,
  timestampTag,
} from './lib/common.js'

const OFFERS_ROOT = path.resolve(process.cwd(), 'data/house-ads/offers')
const OFFER_JOBS_DIR = path.join(OFFERS_ROOT, 'raw', 'offer-jobs')
const OFFER_SIGNALS_DIR = path.join(OFFERS_ROOT, 'raw', 'offer-signals')

const OFFER_HINTS = [
  'sale',
  'deal',
  'offer',
  'discount',
  'buy',
  'shop',
  'new arrival',
  'promotion',
  'coupon',
  'clearance',
  'bundle',
  'free shipping',
]
const IMAGE_EXT_RE = /\.(png|jpe?g|webp|gif|svg|avif|bmp|ico)(\?|#|$)/i

function toNumber(value, fallback = 0) {
  const num = Number(value)
  return Number.isFinite(num) ? num : fallback
}

function compactWhitespace(value) {
  return cleanText(String(value || '').replace(/\s+/g, ' '))
}

function safeUrl(urlText, baseUrl = '') {
  try {
    const parsed = new URL(urlText, baseUrl || undefined)
    if (!['http:', 'https:'].includes(parsed.protocol)) return ''
    return normalizeUrl(parsed.toString())
  } catch {
    return ''
  }
}

function normalizeAvailability(value = '') {
  const text = compactWhitespace(value).toLowerCase()
  if (!text) return 'unknown'
  if (text.includes('in stock')) return 'in_stock'
  if (text.includes('limited')) return 'limited'
  if (text.includes('preorder') || text.includes('pre-order')) return 'preorder'
  if (text.includes('out of stock') || text.includes('sold out')) return 'out_of_stock'
  return 'unknown'
}

function extractPrice(raw = '') {
  const text = compactWhitespace(raw)
  if (!text) return null
  const m = text.match(/(\d{1,6}(?:[.,]\d{1,2})?)/)
  if (!m) return null
  const normalized = m[1].replace(/,/g, '.')
  const value = Number(normalized)
  if (!Number.isFinite(value) || value <= 0) return null
  return Number(value.toFixed(2))
}

function extractCurrency(raw = '') {
  const text = compactWhitespace(raw).toUpperCase()
  if (!text) return 'USD'
  if (text.includes('$') || text.includes('USD')) return 'USD'
  if (text.includes('EUR') || text.includes('€')) return 'EUR'
  if (text.includes('GBP') || text.includes('£')) return 'GBP'
  if (text.includes('JPY') || text.includes('¥')) return 'JPY'
  if (text.includes('CNY') || text.includes('RMB')) return 'CNY'
  const code = text.match(/\b[A-Z]{3}\b/)
  return code ? code[0] : 'USD'
}

function decodeHtmlEntities(value = '') {
  return String(value || '')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
}

function extractLinks(html = '', baseUrl = '') {
  const rows = []
  const regex = /<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi
  let match = regex.exec(html)
  while (match) {
    const href = safeUrl(match[1] || '', baseUrl)
    if (!href) {
      match = regex.exec(html)
      continue
    }
    const text = compactWhitespace(decodeHtmlEntities((match[2] || '').replace(/<[^>]+>/g, ' ')))
    if (!text) {
      match = regex.exec(html)
      continue
    }
    rows.push({
      target_url: href,
      anchor_text: text,
    })
    match = regex.exec(html)
  }
  return rows
}

function looksLikeImageUrl(url = '') {
  return IMAGE_EXT_RE.test(cleanText(url))
}

function collectMetaImageCandidates(html = '', pageUrl = '') {
  const patterns = [
    /<meta[^>]+(?:property|name)=["']og:image["'][^>]+content=["']([^"']+)["'][^>]*>/gi,
    /<meta[^>]+(?:property|name)=["']og:image:url["'][^>]+content=["']([^"']+)["'][^>]*>/gi,
    /<meta[^>]+(?:property|name)=["']twitter:image["'][^>]+content=["']([^"']+)["'][^>]*>/gi,
    /<meta[^>]+(?:property|name)=["']twitter:image:src["'][^>]+content=["']([^"']+)["'][^>]*>/gi,
    /<link[^>]+rel=["']image_src["'][^>]+href=["']([^"']+)["'][^>]*>/gi,
  ]
  const candidates = []
  for (const pattern of patterns) {
    let match = pattern.exec(html)
    while (match) {
      const candidate = safeUrl(match?.[1] || '', pageUrl)
      if (candidate) candidates.push(candidate)
      match = pattern.exec(html)
    }
    pattern.lastIndex = 0
  }
  return candidates
}

function collectImageTagCandidates(html = '', pageUrl = '') {
  const candidates = []
  const regex = /<img[^>]+src=["']([^"']+)["'][^>]*>/gi
  let match = regex.exec(html)
  while (match) {
    const candidate = safeUrl(match[1] || '', pageUrl)
    if (candidate) candidates.push(candidate)
    match = regex.exec(html)
  }
  return candidates
}

function extractMetaImage(html = '', pageUrl = '') {
  const metaCandidates = collectMetaImageCandidates(html, pageUrl)
  const tagCandidates = collectImageTagCandidates(html, pageUrl)
  const candidates = [...metaCandidates, ...tagCandidates]
  const preferred = candidates.find((item) => looksLikeImageUrl(item))
  if (preferred) return preferred
  return candidates[0] || ''
}

function extractJsonLdBlocks(html = '') {
  const payloads = []
  const matches = html.matchAll(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)
  for (const match of matches) {
    const text = compactWhitespace(match[1] || '')
    if (!text) continue
    payloads.push(text)
  }
  return payloads
}

function iterNodes(root) {
  const queue = [root]
  const out = []
  while (queue.length > 0) {
    const node = queue.shift()
    if (!node) continue
    if (Array.isArray(node)) {
      for (const item of node) queue.push(item)
      continue
    }
    if (typeof node !== 'object') continue
    out.push(node)
    if (Array.isArray(node['@graph'])) {
      for (const item of node['@graph']) queue.push(item)
    }
    for (const value of Object.values(node)) {
      if (value && typeof value === 'object') queue.push(value)
    }
  }
  return out
}

function parseJsonLdProducts(html = '', pageUrl = '') {
  const payloads = extractJsonLdBlocks(html)
  const products = []
  for (const payload of payloads) {
    try {
      const parsed = JSON.parse(payload)
      const nodes = iterNodes(parsed)
      for (const node of nodes) {
        const typeValue = node['@type']
        const types = Array.isArray(typeValue) ? typeValue : [typeValue]
        const isProduct = types.some((type) => String(type || '').toLowerCase().includes('product'))
        if (!isProduct) continue

        const name = compactWhitespace(node.name || '')
        const productUrl = safeUrl(node.url || '', pageUrl) || safeUrl(pageUrl)
        if (!name || !productUrl) continue

        const offers = Array.isArray(node.offers) ? node.offers[0] : node.offers || {}
        const price = extractPrice(offers.price || node.price || '')
        const currency = extractCurrency(offers.priceCurrency || node.priceCurrency || '')
        const availability = normalizeAvailability(offers.availability || node.availability || '')
        const imageUrl = Array.isArray(node.image)
          ? safeUrl(node.image[0] || '', pageUrl)
          : safeUrl(node.image || '', pageUrl)

        products.push({
          offer_type: 'product',
          title: name.slice(0, 120),
          snippet: compactWhitespace(node.description || '').slice(0, 220),
          target_url: productUrl,
          image_url: imageUrl,
          price: price ?? undefined,
          currency,
          availability,
          extraction_method: 'jsonld_product',
          confidence: 0.93,
        })
      }
    } catch {
      // ignore malformed json-ld
    }
  }
  return products
}

function parseOfferLinks(html = '', pageUrl = '', pageImageUrl = '') {
  const links = extractLinks(html, pageUrl)
  const candidates = []
  for (const link of links) {
    const lower = link.anchor_text.toLowerCase()
    const hasHint = OFFER_HINTS.some((hint) => lower.includes(hint))
    if (!hasHint) continue
    candidates.push({
      offer_type: 'link',
      title: link.anchor_text.slice(0, 120),
      description: `Discover ${link.anchor_text}`.slice(0, 220),
      target_url: link.target_url,
      image_url: pageImageUrl,
      cta_text: 'View Offer',
      extraction_method: 'anchor_offer_link',
      confidence: 0.74,
    })
  }
  return candidates
}

function parseFallbackLink(page) {
  const title = compactWhitespace(page.title || page.h1 || '')
  if (!title) return null
  const fallback = {
    offer_type: 'link',
    title: title.slice(0, 120),
    description: compactWhitespace(page.description || `Explore offers from ${page.brand_name || 'this brand'}`).slice(0, 220),
    target_url: page.url,
    cta_text: 'Shop Now',
    extraction_method: 'page_title_fallback',
    confidence: 0.58,
  }
  const imageUrl = safeUrl(page.image_url || '', page.url)
  if (imageUrl) fallback.image_url = imageUrl
  return fallback
}

function dedupeCandidates(candidates = []) {
  const seen = new Set()
  const out = []
  for (const item of candidates) {
    const key = [
      cleanText(item.offer_type),
      cleanText(item.target_url).toLowerCase(),
      cleanText(item.title).toLowerCase(),
    ].join('|')
    if (!key || seen.has(key)) continue
    seen.add(key)
    out.push(item)
  }
  return out
}

function selectTopCandidates(candidates = [], maxCandidates = 3) {
  const deduped = dedupeCandidates(candidates)
  deduped.sort((a, b) => toNumber(b.confidence) - toNumber(a.confidence))
  return deduped.slice(0, Math.max(1, maxCandidates))
}

function categoryTags(job) {
  return [cleanText(job.vertical_l1), cleanText(job.vertical_l2)]
    .filter(Boolean)
    .slice(0, 4)
}

async function loadJobs(args) {
  const explicit = cleanText(args['jobs-file'])
  if (explicit) {
    return readJsonl(path.resolve(process.cwd(), explicit))
  }
  const latestMetaPath = path.join(OFFER_JOBS_DIR, 'latest-offer-jobs.json')
  const latestMeta = await readJson(latestMetaPath, null)
  if (!latestMeta?.latestJsonl) {
    throw new Error('No latest offer jobs metadata found. Run build-offer-jobs first.')
  }
  return readJsonl(path.resolve(process.cwd(), latestMeta.latestJsonl))
}

function buildTargets(job, maxAttemptsPerBrand) {
  const rawTargets = Array.isArray(job.crawl_targets) ? job.crawl_targets : []
  const deduped = []
  const seen = new Set()
  for (const target of rawTargets) {
    const normalized = safeUrl(target)
    if (!normalized) continue
    if (seen.has(normalized)) continue
    seen.add(normalized)
    deduped.push(normalized)
  }

  if (deduped.length === 0 && cleanText(job.official_domain)) {
    const host = cleanText(job.official_domain).toLowerCase()
    const defaults = ['/', '/shop', '/products', '/collections', '/sale'].map((p) => safeUrl(`https://${host}${p}`))
    for (const item of defaults) {
      if (!item || seen.has(item)) continue
      seen.add(item)
      deduped.push(item)
    }
  }
  return deduped.slice(0, Math.max(1, maxAttemptsPerBrand))
}

async function crawlJob(job, options) {
  const now = new Date().toISOString()
  const maxPages = Math.max(1, options.maxPagesPerBrand)
  const maxAttempts = Math.max(1, options.maxAttemptsPerBrand)
  const targets = buildTargets(job, maxAttempts)
  const pages = []
  const errors = []
  const candidates = []
  let attempts = 0

  for (const url of targets) {
    if (attempts >= maxAttempts || pages.length >= maxPages) break
    attempts += 1
    try {
      const response = await fetchWithTimeout(url, { timeoutMs: options.timeoutMs })
      if (!response.ok && response.status >= 500) {
        errors.push(`http_${response.status}:${url}`)
        continue
      }
      const contentType = cleanText(response.headers.get('content-type') || '').toLowerCase()
      if (!contentType.includes('text/html')) {
        errors.push(`non_html:${url}`)
        continue
      }
      const html = await response.text()
      const title = extractHtmlTitle(html).slice(0, 180)
      const description = extractMetaDescription(html).slice(0, 280)
      const h1 = extractH1(html).slice(0, 180)
      const textExcerpt = stripHtmlText(html).slice(0, 320)
      const pageImageUrl = extractMetaImage(html, url)

      const page = {
        url,
        status_code: response.status,
        title: cleanText(title),
        description: cleanText(description),
        h1: cleanText(h1),
        text_excerpt: cleanText(textExcerpt),
        image_url: cleanText(pageImageUrl),
        fetched_at: new Date().toISOString(),
      }
      pages.push(page)

      const productCandidates = parseJsonLdProducts(html, url).map((item) => ({
        ...item,
        source_url: url,
      }))
      const linkCandidates = parseOfferLinks(html, url, pageImageUrl).map((item) => ({
        ...item,
        source_url: url,
      }))
      candidates.push(...productCandidates, ...linkCandidates)

      if (productCandidates.length === 0 && linkCandidates.length === 0) {
        const fallback = parseFallbackLink({
          ...page,
          brand_name: job.brand_name,
        })
        if (fallback) {
          candidates.push({
            ...fallback,
            source_url: url,
          })
        }
      }
    } catch {
      errors.push(`network_error:${url}`)
    }
  }

  const picked = selectTopCandidates(candidates, options.maxCandidatesPerBrand).map((item, idx) => {
    const base = {
      candidate_id: `offer_candidate_${hashId(`${job.job_id}|${item.target_url}|${item.title}|${idx}`, 10)}`,
      offer_type: item.offer_type,
      title: cleanText(item.title).slice(0, 120),
      target_url: cleanText(item.target_url),
      source_url: cleanText(item.source_url),
      extraction_method: cleanText(item.extraction_method) || 'unknown',
      confidence: Number(toNumber(item.confidence, 0.5).toFixed(4)),
      tags: categoryTags(job),
    }
    if (item.offer_type === 'product') {
      return {
        ...base,
        snippet: cleanText(item.snippet).slice(0, 220),
        image_url: cleanText(item.image_url),
        price: item.price ?? undefined,
        currency: cleanText(item.currency || 'USD') || 'USD',
        availability: cleanText(item.availability || 'unknown') || 'unknown',
      }
    }
    return {
      ...base,
      description: cleanText(item.description).slice(0, 220),
      image_url: cleanText(item.image_url),
      cta_text: cleanText(item.cta_text || 'View Offer').slice(0, 40),
    }
  })

  let status = 'ok'
  let reason = ''
  if (pages.length === 0) {
    status = 'empty'
    reason = 'crawl_no_pages'
  } else if (picked.length === 0) {
    status = 'empty'
    reason = 'crawl_no_candidates'
  }

  return {
    signal_id: `offer_signal_${hashId(job.job_id || `${job.brand_id}|${now}`, 12)}`,
    job_id: cleanText(job.job_id),
    brand_id: cleanText(job.brand_id),
    brand_name: cleanText(job.brand_name),
    official_domain: cleanText(job.official_domain).toLowerCase(),
    vertical_l1: cleanText(job.vertical_l1),
    vertical_l2: cleanText(job.vertical_l2),
    market: cleanText(job.market) || 'US',
    queue_index: toInteger(job.queue_index, 0),
    crawl_attempts: attempts,
    crawled_pages: pages.length,
    candidate_count: picked.length,
    status,
    reason,
    candidates: picked,
    pages: pages.map((page) => ({
      url: page.url,
      status_code: page.status_code,
      title: page.title,
      description: page.description,
      h1: page.h1,
      fetched_at: page.fetched_at,
    })),
    errors: errors.slice(0, 20),
    updated_at: now,
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const maxBrands = toInteger(args['max-brands'], 0)
  const concurrency = toInteger(args.concurrency, 12)
  const maxPagesPerBrand = toInteger(args['max-pages-per-brand'], 3)
  const maxAttemptsPerBrand = toInteger(args['max-attempts-per-brand'], 5)
  const timeoutMs = toInteger(args['timeout-ms'], 7000)
  const maxCandidatesPerBrand = toInteger(args['max-candidates-per-brand'], 3)
  const tag = timestampTag()

  const jobs = await loadJobs(args)
  const scopedJobs = maxBrands > 0 ? jobs.slice(0, Math.max(1, maxBrands)) : jobs
  await ensureDir(OFFER_SIGNALS_DIR)

  const signals = await asyncPool(
    Math.max(1, concurrency),
    scopedJobs,
    (job) =>
      crawlJob(job, {
        maxPagesPerBrand,
        maxAttemptsPerBrand,
        timeoutMs,
        maxCandidatesPerBrand,
      }),
  )

  const signalsPath = path.join(OFFER_SIGNALS_DIR, `offer-signals-${tag}.jsonl`)
  const summaryPath = path.join(OFFER_SIGNALS_DIR, `offer-signals-${tag}.summary.json`)
  const latestPath = path.join(OFFER_SIGNALS_DIR, 'latest-offer-signals.json')

  const nonEmpty = signals.filter((row) => row.candidate_count > 0)
  const empty = signals.length - nonEmpty.length
  const perBrandCandidateCounts = signals.map((row) => row.candidate_count)
  const summary = {
    generatedAt: new Date().toISOString(),
    inputJobs: scopedJobs.length,
    outputSignals: signals.length,
    nonEmptySignals: nonEmpty.length,
    emptySignals: empty,
    avgCandidatesPerBrand: Number(
      (perBrandCandidateCounts.reduce((sum, n) => sum + n, 0) / Math.max(1, perBrandCandidateCounts.length)).toFixed(4),
    ),
    maxCandidatesPerBrand: Math.max(0, ...perBrandCandidateCounts),
    minCandidatesPerBrand: Math.min(...perBrandCandidateCounts, 0),
    output: path.relative(process.cwd(), signalsPath),
  }

  await writeJsonl(signalsPath, signals)
  await writeJson(summaryPath, summary)
  await writeJson(latestPath, {
    generatedAt: new Date().toISOString(),
    latestJsonl: path.relative(process.cwd(), signalsPath),
    latestSummary: path.relative(process.cwd(), summaryPath),
  })

  console.log(
    JSON.stringify(
      {
        ok: true,
        inputJobs: scopedJobs.length,
        outputSignals: signals.length,
        nonEmptySignals: nonEmpty.length,
        emptySignals: empty,
        signalsFile: path.relative(process.cwd(), signalsPath),
        summaryFile: path.relative(process.cwd(), summaryPath),
      },
      null,
      2,
    ),
  )
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error('[crawl-offers] failed:', error?.message || error)
    process.exit(1)
  })
}

export const __crawlOffersInternal = Object.freeze({
  extractMetaImage,
  parseOfferLinks,
})
