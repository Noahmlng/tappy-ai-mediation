#!/usr/bin/env node
import path from 'node:path'
import {
  RAW_ROOT,
  CURATED_ROOT,
  parseArgs,
  toInteger,
  toBoolean,
  timestampTag,
  readJson,
  readJsonl,
  writeJson,
  writeJsonl,
  findLatestFile,
  ensureDir,
  cleanText,
  registrableDomain,
  domainToBrandName,
  fetchWithTimeout,
  sleep,
  hashId,
} from './lib/common.js'
import { VERTICAL_TAXONOMY } from './lib/vertical-taxonomy.js'

const SEARCH_JOBS_DIR = path.join(RAW_ROOT, 'search-jobs')
const SEARCH_RESULTS_DIR = path.join(RAW_ROOT, 'search-results')
const BRAND_SEEDS_DIR = path.join(RAW_ROOT, 'brand-seeds')
const DEFAULT_MANUAL_SEEDS_FILE = path.join(CURATED_ROOT, 'manual-brand-seeds.jsonl')

const BLOCKED_DOMAINS = new Set([
  'duckduckgo.com',
  'google.com',
  'bing.com',
  'yahoo.com',
  'wikipedia.org',
  'wikidata.org',
  'linkedin.com',
  'facebook.com',
  'instagram.com',
  'x.com',
  'twitter.com',
  'youtube.com',
  'tiktok.com',
  'reddit.com',
  'amazon.com',
  'ebay.com',
  'walmart.com',
])

function parseHtmlResults(html = '') {
  const rows = []
  const regex = /<a[^>]+class="[^"]*result__a[^"]*"[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi
  let match = regex.exec(html)
  while (match) {
    const rawHref = cleanText(match[1] || '')
    const title = cleanText((match[2] || '').replace(/<[^>]+>/g, ' '))
    if (rawHref && title) rows.push({ rawHref, title })
    match = regex.exec(html)
  }
  return rows
}

function resolveDuckHref(rawHref) {
  const href = cleanText(rawHref)
  if (!href) return ''
  try {
    const parsed = new URL(href, 'https://duckduckgo.com')
    const redirect = parsed.searchParams.get('uddg')
    if (redirect) return decodeURIComponent(redirect)
    return parsed.toString()
  } catch {
    return ''
  }
}

function brandFromTitle(title, fallbackDomain = '') {
  const raw = cleanText(title)
  if (!raw) return domainToBrandName(fallbackDomain)
  const candidate = cleanText(
    raw
      .split(/\||-|–|—|·|\u2022/g)[0]
      .replace(/\b(official site|official|home|homepage|官网)\b/gi, ' ')
      .replace(/\s+/g, ' '),
  )
  if (candidate && candidate.length >= 2) return candidate
  return domainToBrandName(fallbackDomain)
}

function selectMostFrequent(entries = []) {
  const counter = new Map()
  for (const item of entries) {
    const key = cleanText(item)
    if (!key) continue
    counter.set(key, (counter.get(key) || 0) + 1)
  }
  let best = ''
  let bestCount = -1
  for (const [key, count] of counter.entries()) {
    if (count > bestCount) {
      best = key
      bestCount = count
    }
  }
  return best
}

async function loadJobs(args) {
  const explicitFile = cleanText(args['jobs-file'])
  if (explicitFile) return readJsonl(path.resolve(process.cwd(), explicitFile))

  const latestMetaPath = path.join(SEARCH_JOBS_DIR, 'latest-search-jobs.json')
  const latestMeta = await readJson(latestMetaPath, null)
  if (latestMeta?.latestJsonl) {
    return readJsonl(path.resolve(process.cwd(), latestMeta.latestJsonl))
  }
  const latestFile = await findLatestFile(SEARCH_JOBS_DIR, '.jsonl')
  if (!latestFile) throw new Error('No search jobs found. Run build-search-jobs first.')
  return readJsonl(latestFile)
}

async function fetchDuckSearch(query, maxResults = 12) {
  const url = `https://duckduckgo.com/html/?q=${encodeURIComponent(query)}&kl=us-en`
  const response = await fetchWithTimeout(url, { timeoutMs: 12000 })
  if (!response.ok) {
    throw new Error(`duck_search_http_${response.status}`)
  }
  const html = await response.text()
  const parsed = parseHtmlResults(html).slice(0, maxResults)
  const rows = []
  for (const [idx, item] of parsed.entries()) {
    const resolved = resolveDuckHref(item.rawHref)
    if (!resolved) continue
    rows.push({
      rank: idx + 1,
      title: item.title,
      rawUrl: item.rawHref,
      resolvedUrl: resolved,
    })
  }
  return rows
}

function parseRssItems(xml = '') {
  const items = []
  const itemRegex = /<item>([\s\S]*?)<\/item>/gi
  let itemMatch = itemRegex.exec(xml)
  while (itemMatch) {
    const body = itemMatch[1]
    const titleMatch = body.match(/<title>([\s\S]*?)<\/title>/i)
    const linkMatch = body.match(/<link>([\s\S]*?)<\/link>/i)
    const title = cleanText((titleMatch?.[1] || '').replace(/<!\[CDATA\[|\]\]>/g, ''))
    const link = cleanText((linkMatch?.[1] || '').replace(/<!\[CDATA\[|\]\]>/g, ''))
    if (title && link) items.push({ title, link })
    itemMatch = itemRegex.exec(xml)
  }
  return items
}

async function fetchBingRssSearch(query, maxResults = 12) {
  const url = `https://www.bing.com/search?q=${encodeURIComponent(query)}&format=rss`
  const response = await fetchWithTimeout(url, {
    timeoutMs: 12000,
    accept: 'application/rss+xml, application/xml, text/xml',
  })
  if (!response.ok) {
    throw new Error(`bing_rss_http_${response.status}`)
  }
  const xml = await response.text()
  return parseRssItems(xml)
    .slice(0, maxResults)
    .map((item, idx) => ({
      rank: idx + 1,
      title: item.title,
      rawUrl: item.link,
      resolvedUrl: item.link,
    }))
}

async function fetchMajesticFallbackDomains(limit = 5000) {
  const url = 'https://downloads.majestic.com/majestic_million.csv'
  const response = await fetchWithTimeout(url, {
    timeoutMs: 25000,
    accept: 'text/csv, text/plain',
  })
  if (!response.ok || !response.body) {
    throw new Error(`majestic_fetch_failed_${response.status}`)
  }

  const decoder = new TextDecoder()
  const reader = response.body.getReader()
  const domains = []
  const seen = new Set()
  let buffered = ''

  while (domains.length < limit) {
    const { done, value } = await reader.read()
    if (done) break
    buffered += decoder.decode(value, { stream: true })
    const lines = buffered.split('\n')
    buffered = lines.pop() || ''
    for (const line of lines) {
      const cleanLine = cleanText(line)
      if (!cleanLine || cleanLine.startsWith('GlobalRank,')) continue
      const parts = cleanLine.split(',')
      if (parts.length < 3) continue
      const domain = registrableDomain(parts[2])
      if (!domain || seen.has(domain) || BLOCKED_DOMAINS.has(domain)) continue
      seen.add(domain)
      domains.push(domain)
      if (domains.length >= limit) break
    }
  }
  reader.cancel().catch(() => {})
  return domains
}

function buildFallbackSeed(domain, index) {
  const vertical = VERTICAL_TAXONOMY[index % VERTICAL_TAXONOMY.length]
  return {
    seed_id: `seed_${hashId(`majestic|${domain}`)}`,
    brand_name: domainToBrandName(domain),
    candidate_domains: [domain],
    vertical_l1: vertical?.vertical_l1 || 'unknown',
    vertical_l2: vertical?.vertical_l2 || 'unknown',
    market: 'US',
    source_confidence: 0.42,
    search_hit_count: 1,
    evidence_queries: ['majestic_million_fallback'],
    evidence_urls: [`https://${domain}`],
    status: 'pending_verification',
  }
}

function normalizeResultRecord(job, result, provider) {
  const resolvedUrl = cleanText(result.resolvedUrl)
  if (!resolvedUrl) return null
  let domain = ''
  try {
    domain = registrableDomain(new URL(resolvedUrl).hostname)
  } catch {
    return null
  }
  if (!domain || BLOCKED_DOMAINS.has(domain)) return null
  return {
    jobId: job.id,
    query: job.query,
    vertical_l1: job.vertical_l1,
    vertical_l2: job.vertical_l2,
    rank: Number(result.rank) || 999,
    title: cleanText(result.title),
    resolvedUrl,
    domain,
    fetchedAt: new Date().toISOString(),
    provider: cleanText(provider) || 'unknown',
  }
}

function sourceConfidenceFromRank(rank, query = '') {
  const safeRank = Math.max(1, Number(rank) || 50)
  const rankScore = Math.max(0, 1 - (safeRank - 1) / 20)
  const queryBoost = /\bofficial\b/i.test(query) ? 0.08 : 0
  return Math.min(0.95, 0.35 + rankScore * 0.5 + queryBoost)
}

function mergeToBrandSeeds(resultRecords = []) {
  const byDomain = new Map()
  for (const record of resultRecords) {
    const key = record.domain
    if (!key) continue
    if (!byDomain.has(key)) {
      byDomain.set(key, {
        domain: key,
        names: [],
        verticalL1Hits: [],
        verticalL2Hits: [],
        confidenceSamples: [],
        evidenceQueries: new Set(),
        evidenceUrls: new Set(),
        searchHitCount: 0,
      })
    }
    const bucket = byDomain.get(key)
    bucket.names.push(brandFromTitle(record.title, key))
    bucket.verticalL1Hits.push(record.vertical_l1)
    bucket.verticalL2Hits.push(record.vertical_l2)
    bucket.confidenceSamples.push(sourceConfidenceFromRank(record.rank, record.query))
    bucket.evidenceQueries.add(record.query)
    bucket.evidenceUrls.add(record.resolvedUrl)
    bucket.searchHitCount += 1
  }

  const output = []
  for (const bucket of byDomain.values()) {
    const avgConfidence =
      bucket.confidenceSamples.length === 0
        ? 0
        : bucket.confidenceSamples.reduce((sum, value) => sum + value, 0) / bucket.confidenceSamples.length
    output.push({
      seed_id: `seed_${hashId(bucket.domain)}`,
      brand_name: selectMostFrequent(bucket.names) || domainToBrandName(bucket.domain),
      candidate_domains: [bucket.domain],
      vertical_l1: selectMostFrequent(bucket.verticalL1Hits),
      vertical_l2: selectMostFrequent(bucket.verticalL2Hits),
      market: 'US',
      source_confidence: Number(avgConfidence.toFixed(4)),
      search_hit_count: bucket.searchHitCount,
      evidence_queries: [...bucket.evidenceQueries].slice(0, 8),
      evidence_urls: [...bucket.evidenceUrls].slice(0, 12),
      status: 'pending_verification',
    })
  }

  return output.sort((a, b) => {
    if (b.source_confidence !== a.source_confidence) return b.source_confidence - a.source_confidence
    return b.search_hit_count - a.search_hit_count
  })
}

function normalizeManualSeed(seed = {}) {
  const brandName = cleanText(seed.brand_name || seed.name)
  const candidateDomains = Array.isArray(seed.candidate_domains)
    ? seed.candidate_domains
    : [seed.official_domain || seed.domain]
  const domain = candidateDomains
    .map((item) => registrableDomain(item))
    .find(Boolean)
  const verticalL1 = cleanText(seed.vertical_l1)
  const verticalL2 = cleanText(seed.vertical_l2)

  if (!brandName || !domain || !verticalL1 || !verticalL2) return null

  return {
    seed_id: cleanText(seed.seed_id || `seed_manual_${hashId(`${domain}|${brandName}`)}`),
    brand_name: brandName,
    candidate_domains: [domain],
    vertical_l1: verticalL1,
    vertical_l2: verticalL2,
    market: cleanText(seed.market) || 'US',
    source_confidence: Number.isFinite(Number(seed.source_confidence))
      ? Number(seed.source_confidence)
      : 0.9,
    search_hit_count: Number.isFinite(Number(seed.search_hit_count))
      ? Math.max(1, Number(seed.search_hit_count))
      : 1,
    evidence_queries: Array.isArray(seed.evidence_queries) ? seed.evidence_queries.slice(0, 8) : ['manual_seed'],
    evidence_urls: Array.isArray(seed.evidence_urls) ? seed.evidence_urls.slice(0, 12) : [`https://${domain}`],
    status: 'pending_verification',
  }
}

async function loadManualSeeds(args = {}) {
  const explicit = cleanText(args['manual-seeds-file'])
  const filePath = explicit || DEFAULT_MANUAL_SEEDS_FILE
  try {
    const rows = await readJsonl(path.resolve(process.cwd(), filePath))
    return rows.map((row) => normalizeManualSeed(row)).filter(Boolean)
  } catch {
    return []
  }
}

function mergeSeedsWithManual(discoveredSeeds = [], manualSeeds = []) {
  const out = [...discoveredSeeds]
  const seen = new Set(
    discoveredSeeds.map((seed) => [
      cleanText(seed.candidate_domains?.[0]),
      cleanText(seed.vertical_l1).toLowerCase(),
      cleanText(seed.vertical_l2).toLowerCase(),
    ].join('|')),
  )

  for (const seed of manualSeeds) {
    const key = [
      cleanText(seed.candidate_domains?.[0]),
      cleanText(seed.vertical_l1).toLowerCase(),
      cleanText(seed.vertical_l2).toLowerCase(),
    ].join('|')
    if (!key || seen.has(key)) continue
    seen.add(key)
    out.push(seed)
  }

  return out
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const doFetch = toBoolean(args.fetch, true)
  const provider = cleanText(args.provider || 'bing_rss').toLowerCase()
  const maxJobs = toInteger(args['max-jobs'], 220)
  const maxResultsPerQuery = toInteger(args['max-results-per-query'], 12)
  const sleepMs = toInteger(args['sleep-ms'], 120)
  const seedLimit = toInteger(args['seed-limit'], 5000)
  const minSeeds = toInteger(args['min-seeds'], 500)
  const enableMajesticFallback = toBoolean(args['majestic-fallback'], true)
  const tag = timestampTag()

  await ensureDir(SEARCH_RESULTS_DIR)
  await ensureDir(BRAND_SEEDS_DIR)

  const jobs = (await loadJobs(args)).slice(0, Math.max(1, maxJobs))
  const rawResultRows = []

  if (doFetch) {
    let okCount = 0
    let errorCount = 0
    for (const [index, job] of jobs.entries()) {
      try {
        let fetched = []
        if (provider === 'duckduckgo_html') {
          fetched = await fetchDuckSearch(job.query, maxResultsPerQuery)
        } else if (provider === 'bing_rss') {
          fetched = await fetchBingRssSearch(job.query, maxResultsPerQuery)
        } else {
          throw new Error(`unsupported_provider_${provider}`)
        }
        for (const item of fetched) {
          const normalized = normalizeResultRecord(job, item, provider)
          if (normalized) rawResultRows.push(normalized)
        }
        okCount += 1
      } catch (error) {
        errorCount += 1
        rawResultRows.push({
          jobId: job.id,
          query: job.query,
          vertical_l1: job.vertical_l1,
          vertical_l2: job.vertical_l2,
          error: cleanText(error?.message || String(error)),
          fetchedAt: new Date().toISOString(),
          provider,
        })
      }
      if ((index + 1) % 20 === 0) {
        console.log(`[merge-search-results] progress ${index + 1}/${jobs.length}`)
      }
      await sleep(sleepMs)
    }
    console.log(`[merge-search-results] fetch completed provider=${provider}: ok=${okCount}, error=${errorCount}`)
  } else {
    const explicitRaw = cleanText(args['raw-file'])
    const latestRaw = explicitRaw
      ? path.resolve(process.cwd(), explicitRaw)
      : await findLatestFile(SEARCH_RESULTS_DIR, '.jsonl')
    if (!latestRaw) throw new Error('No raw search results found.')
    const raw = await readJsonl(latestRaw)
    rawResultRows.push(...raw)
  }

  const rawResultPath = path.join(SEARCH_RESULTS_DIR, `search-results-${tag}.jsonl`)
  await writeJsonl(rawResultPath, rawResultRows)

  const cleanResults = rawResultRows.filter((row) => row.domain && row.resolvedUrl)
  let brandSeeds = mergeToBrandSeeds(cleanResults)
  const manualSeeds = await loadManualSeeds(args)
  if (manualSeeds.length > 0) {
    brandSeeds = mergeSeedsWithManual(brandSeeds, manualSeeds)
    console.log(`[merge-search-results] merged ${manualSeeds.length} manual seeds`)
  }

  if (enableMajesticFallback && brandSeeds.length < minSeeds) {
    const needed = minSeeds - brandSeeds.length
    const fallbackDomains = await fetchMajesticFallbackDomains(Math.max(needed * 8, 3000))
    const existing = new Set(brandSeeds.map((item) => cleanText(item.candidate_domains?.[0])))
    let added = 0
    for (const [idx, domain] of fallbackDomains.entries()) {
      if (existing.has(domain)) continue
      brandSeeds.push(buildFallbackSeed(domain, idx))
      existing.add(domain)
      added += 1
      if (brandSeeds.length >= minSeeds) break
    }
    console.log(`[merge-search-results] fallback added ${added} seeds from majestic`)
  }

  brandSeeds = brandSeeds.slice(0, Math.max(1, seedLimit))

  const seedsPath = path.join(BRAND_SEEDS_DIR, `brand-seeds-${tag}.jsonl`)
  const summaryPath = path.join(BRAND_SEEDS_DIR, `brand-seeds-${tag}.summary.json`)
  const latestPath = path.join(BRAND_SEEDS_DIR, 'latest-brand-seeds.json')

  await writeJsonl(seedsPath, brandSeeds)
  await writeJson(summaryPath, {
    generatedAt: new Date().toISOString(),
    sourceJobs: jobs.length,
    rawRecords: rawResultRows.length,
    mergedSeeds: brandSeeds.length,
    manualSeeds: manualSeeds.length,
    minSeeds,
    enableMajesticFallback,
    output: path.relative(process.cwd(), seedsPath),
  })
  await writeJson(latestPath, {
    generatedAt: new Date().toISOString(),
    latestSeedsJsonl: path.relative(process.cwd(), seedsPath),
    latestSummary: path.relative(process.cwd(), summaryPath),
    latestRawResults: path.relative(process.cwd(), rawResultPath),
  })

  console.log(
    JSON.stringify(
      {
        ok: true,
        jobs: jobs.length,
        rawRecords: rawResultRows.length,
        brandSeeds: brandSeeds.length,
        seedsPath: path.relative(process.cwd(), seedsPath),
      },
      null,
      2,
    ),
  )
}

main().catch((error) => {
  console.error('[merge-search-results] failed:', error?.message || error)
  process.exit(1)
})
