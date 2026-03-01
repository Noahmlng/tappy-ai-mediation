#!/usr/bin/env node
import fs from 'node:fs/promises'
import path from 'node:path'
import {
  RAW_ROOT,
  CURATED_ROOT,
  parseArgs,
  toInteger,
  toBoolean,
  readJson,
  readJsonl,
  writeJson,
  writeJsonl,
  findLatestFile,
  ensureDir,
  cleanText,
  slugify,
  hashId,
  fetchWithTimeout,
  extractHtmlTitle,
  asyncPool,
  registrableDomain,
  timestampTag,
  domainToBrandName,
} from './lib/common.js'

const BRAND_SEEDS_DIR = path.join(RAW_ROOT, 'brand-seeds')
const KNOWLEDGE_ROOT = path.join(RAW_ROOT, 'knowledge')
const DEFAULT_KB_FILES = {
  whitelist: path.join(KNOWLEDGE_ROOT, 'brand-whitelist.jsonl'),
  wikidata: path.join(KNOWLEDGE_ROOT, 'wikidata-brand-index.jsonl'),
  opencorporates: path.join(KNOWLEDGE_ROOT, 'opencorporates-brand-index.jsonl'),
}

function brandId(domain, brandName) {
  const slug = slugify(domain || brandName || 'brand')
  return `brand_${slug}_${hashId(`${domain}|${brandName}`, 8)}`
}

function pickSeedDomain(seed) {
  const candidates = Array.isArray(seed?.candidate_domains) ? seed.candidate_domains : []
  for (const item of candidates) {
    const domain = registrableDomain(item)
    if (domain) return domain
  }
  return ''
}

function tokenMatchScore(brandName, title) {
  const brandTokens = cleanText(brandName)
    .toLowerCase()
    .split(/[^a-z0-9]+/g)
    .filter((token) => token.length >= 3)
  if (brandTokens.length === 0) return 0
  const titleLower = cleanText(title).toLowerCase()
  const matched = brandTokens.filter((token) => titleLower.includes(token))
  return matched.length / brandTokens.length
}

function sourceTitleLooksWeak(sourceTitle = '') {
  const value = cleanText(sourceTitle).toLowerCase()
  if (!value) return true
  const weakHints = [
    '什么意思',
    '是什么意思',
    '怎么读',
    '翻译',
    '用法',
    '例句',
    'forum',
    'thread',
    'rating',
    'guide',
    'tips',
    'review',
    'hidden',
    'future of',
    'popular with',
    '百度知道',
  ]
  if (weakHints.some((hint) => value.includes(hint))) return true
  if (value.length > 48) return true
  if (value.split(/\s+/g).filter(Boolean).length > 6) return true
  return false
}

function detectSourceTitlePollution(sourceTitle = '') {
  const raw = cleanText(sourceTitle)
  const value = raw.toLowerCase()
  if (!value) return { invalid: true, reason: 'empty_source_title' }

  const rules = [
    { pattern: /是什么意思|什么意思|怎么读|翻译|例句|用法/, reason: 'dictionary_query' },
    { pattern: /百度知道|知乎|问答|问一问|qa\b|q&a/, reason: 'qa_content' },
    { pattern: /教程|指南|攻略|guide|tutorial|how to|tips/, reason: 'tutorial_content' },
    { pattern: /论坛|贴吧|forum|thread|帖子|讨论/, reason: 'forum_thread' },
    { pattern: /下载|资源|合集|破解|网盘/, reason: 'resource_post' },
  ]
  for (const rule of rules) {
    if (rule.pattern.test(value)) return { invalid: true, reason: rule.reason }
  }

  const noisySeparators = (raw.match(/[_|｜-]/g) || []).length
  if (raw.length >= 36 && noisySeparators >= 3) {
    return { invalid: true, reason: 'seo_like_title' }
  }
  return { invalid: false, reason: '' }
}

function canonicalBrandNameFromDomain(domain = '') {
  const candidate = cleanText(domainToBrandName(domain))
  if (!candidate) return ''
  return candidate
}

function isValidDomainSyntax(domain = '') {
  const host = cleanText(domain).toLowerCase()
  if (!host) return false
  if (host.length > 253) return false
  const labels = host.split('.').filter(Boolean)
  if (labels.length < 2) return false
  const tld = labels[labels.length - 1]
  if (!/^[a-z]{2,24}$/.test(tld)) return false
  return labels.every((label) => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label))
}

function normalizeBrandKey(name = '') {
  return cleanText(name)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
}

function titleBrandSegment(title = '') {
  const cleaned = cleanText(title)
  if (!cleaned) return ''
  const parts = cleaned
    .split(/\s*[|\-:·•]\s*/g)
    .map((part) => cleanText(part))
    .filter(Boolean)
  if (parts.length === 0) return cleaned
  return parts.reduce((picked, current) => {
    if (!picked) return current
    const currentScore = current.length
    const pickedScore = picked.length
    return currentScore < pickedScore ? current : picked
  }, '')
}

function extractMetaContent(html = '', propertyOrName = '') {
  if (!html || !propertyOrName) return ''
  const escaped = propertyOrName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const patterns = [
    new RegExp(
      `<meta[^>]+(?:property|name)=["']${escaped}["'][^>]+content=["']([^"']+)["'][^>]*>`,
      'i',
    ),
    new RegExp(
      `<meta[^>]+content=["']([^"']+)["'][^>]+(?:property|name)=["']${escaped}["'][^>]*>`,
      'i',
    ),
  ]
  for (const pattern of patterns) {
    const match = html.match(pattern)
    if (match) return cleanText(match[1])
  }
  return ''
}

function extractSchemaOrgOrganizationNames(html = '') {
  if (!html) return []
  const names = []
  const scriptMatches = html.matchAll(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)
  for (const match of scriptMatches) {
    const payload = cleanText(match[1])
    if (!payload) continue
    try {
      const parsed = JSON.parse(payload)
      const queue = Array.isArray(parsed) ? [...parsed] : [parsed]
      while (queue.length > 0) {
        const node = queue.shift()
        if (!node || typeof node !== 'object') continue
        const graph = node['@graph']
        if (Array.isArray(graph)) {
          for (const item of graph) queue.push(item)
        }
        const typeValue = node['@type']
        const types = Array.isArray(typeValue) ? typeValue : [typeValue]
        const isOrganization = types.some((type) => String(type || '').toLowerCase().includes('organization'))
        if (isOrganization && typeof node.name === 'string') {
          names.push(cleanText(node.name))
        }
      }
    } catch {
      // ignore malformed json-ld
    }
  }
  return [...new Set(names.filter(Boolean))]
}

function extractLogoAltCandidate(html = '') {
  if (!html) return ''
  const logoTagMatch = html.match(/<img[^>]+(?:id|class)=["'][^"']*logo[^"']*["'][^>]*>/i)
  if (logoTagMatch) {
    const altMatch = logoTagMatch[0].match(/\salt=["']([^"']+)["']/i)
    if (altMatch) return cleanText(altMatch[1])
  }
  const firstAlt = html.match(/<img[^>]+\salt=["']([^"']+)["'][^>]*>/i)
  if (firstAlt) return cleanText(firstAlt[1])
  return ''
}

function resolveCanonicalFromSiteSignals(probe, domain) {
  const signals = {
    og_site_name: cleanText(probe.siteSignals?.og_site_name),
    schema_org_name: cleanText(probe.siteSignals?.schema_org_name),
    title_brand_segment: cleanText(probe.siteSignals?.title_brand_segment),
    logo_alt: cleanText(probe.siteSignals?.logo_alt),
  }
  const buckets = new Map()
  for (const [source, value] of Object.entries(signals)) {
    const normalized = normalizeBrandKey(value)
    if (!normalized || normalized.length < 2) continue
    if (!buckets.has(normalized)) buckets.set(normalized, { sources: [], value })
    buckets.get(normalized).sources.push(source)
    if (value.length < buckets.get(normalized).value.length) buckets.get(normalized).value = value
  }
  const consensus = [...buckets.values()].sort((a, b) => b.sources.length - a.sources.length)[0]
  const canonicalBrand = consensus && consensus.sources.length >= 2 ? consensus.value : ''
  const canonicalConfirmed = Boolean(canonicalBrand)
  return {
    canonicalBrand,
    canonicalConfirmed,
    canonicalSources: canonicalConfirmed ? consensus.sources : [],
    domainFallbackBrand: canonicalBrandNameFromDomain(domain) || domain,
    signals,
  }
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath)
    return true
  } catch {
    return false
  }
}

async function loadKnowledgeFile(filePath) {
  if (!filePath) return []
  const resolved = path.resolve(process.cwd(), filePath)
  if (!(await fileExists(resolved))) return []
  if (resolved.endsWith('.jsonl')) return readJsonl(resolved)
  const payload = await readJson(resolved, [])
  return Array.isArray(payload) ? payload : []
}

function normalizeKnowledgeRecord(row, sourceType) {
  if (!row || typeof row !== 'object') return null
  const domain = registrableDomain(row.domain || row.official_domain || row.website || row.homepage || '')
  const canonical = cleanText(row.canonical_name || row.brand_name || row.name || row.legal_name || '')
  const identifier = cleanText(row.id || row.wikidata_id || row.opencorporates_id || '')
  if (!domain) return null
  return {
    domain,
    canonical_name: canonical,
    canonical_key: normalizeBrandKey(canonical),
    source: cleanText(row.source || sourceType) || sourceType,
    id: identifier,
  }
}

async function loadKnowledgeBase(args) {
  const kbPaths = {
    whitelist: cleanText(args['kb-whitelist-file']) || DEFAULT_KB_FILES.whitelist,
    wikidata: cleanText(args['kb-wikidata-file']) || DEFAULT_KB_FILES.wikidata,
    opencorporates: cleanText(args['kb-opencorporates-file']) || DEFAULT_KB_FILES.opencorporates,
  }
  const [whitelistRows, wikidataRows, opencorporatesRows] = await Promise.all([
    loadKnowledgeFile(kbPaths.whitelist),
    loadKnowledgeFile(kbPaths.wikidata),
    loadKnowledgeFile(kbPaths.opencorporates),
  ])
  const allRecords = [
    ...whitelistRows.map((row) => normalizeKnowledgeRecord(row, 'whitelist')),
    ...wikidataRows.map((row) => normalizeKnowledgeRecord(row, 'wikidata')),
    ...opencorporatesRows.map((row) => normalizeKnowledgeRecord(row, 'opencorporates')),
  ].filter(Boolean)
  const byDomain = new Map()
  for (const record of allRecords) {
    if (!byDomain.has(record.domain)) byDomain.set(record.domain, [])
    byDomain.get(record.domain).push(record)
  }
  return {
    byDomain,
    counts: {
      whitelist: whitelistRows.length,
      wikidata: wikidataRows.length,
      opencorporates: opencorporatesRows.length,
      total: allRecords.length,
    },
    hasAnyRecords: allRecords.length > 0,
  }
}

function alignBrandWithKnowledge(domain, canonicalCandidate, fallbackBrandName, knowledgeBase) {
  if (!knowledgeBase?.hasAnyRecords) {
    return {
      matched: false,
      matched_source: '',
      matched_id: '',
      matched_canonical_name: '',
      mode: 'kb_disabled',
      reason: 'knowledge_base_empty',
    }
  }
  const records = knowledgeBase.byDomain.get(domain) || []
  if (records.length === 0) {
    return {
      matched: false,
      matched_source: '',
      matched_id: '',
      matched_canonical_name: '',
      mode: 'none',
      reason: 'no_domain_match',
    }
  }

  const candidate = cleanText(canonicalCandidate || fallbackBrandName)
  const candidateKey = normalizeBrandKey(candidate)
  if (candidateKey) {
    const exact = records.find((record) => record.canonical_key && record.canonical_key === candidateKey)
    if (exact) {
      return {
        matched: true,
        matched_source: exact.source,
        matched_id: exact.id,
        matched_canonical_name: exact.canonical_name || candidate,
        mode: 'domain_name_exact',
        reason: '',
      }
    }
  }

  const nonEmptyNamed = records.filter((record) => record.canonical_key)
  const uniqueNameKeys = [...new Set(nonEmptyNamed.map((record) => record.canonical_key))]
  if (!candidateKey && uniqueNameKeys.length === 1) {
    const target = nonEmptyNamed.find((record) => record.canonical_key === uniqueNameKeys[0]) || nonEmptyNamed[0]
    return {
      matched: true,
      matched_source: target.source,
      matched_id: target.id,
      matched_canonical_name: target.canonical_name,
      mode: 'domain_unique_name',
      reason: '',
    }
  }

  if (candidateKey && uniqueNameKeys.length === 1) {
    return {
      matched: false,
      matched_source: '',
      matched_id: '',
      matched_canonical_name: '',
      mode: 'none',
      reason: 'domain_match_name_mismatch',
    }
  }

  return {
    matched: false,
    matched_source: '',
    matched_id: '',
    matched_canonical_name: '',
    mode: 'none',
    reason: 'domain_match_ambiguous_name',
  }
}

function shouldMarkSuspect(knowledgeAlignment, resolvedCanonical, sourceNameCheck, probe) {
  const evidenceInsufficient =
    !resolvedCanonical.canonicalConfirmed || sourceNameCheck.invalid || !probe.reachable
  return Boolean(!knowledgeAlignment.matched && evidenceInsufficient)
}

async function probeDomain(domain, timeoutMs) {
  if (!domain) return { reachable: false, protocol: '', url: '', title: '', siteSignals: {}, status_code: 0 }
  const candidates = [`https://${domain}`, `http://${domain}`]
  for (const url of candidates) {
    try {
      const response = await fetchWithTimeout(url, { timeoutMs })
      const statusCode = Number(response.status) || 0
      if (statusCode >= 500) continue
      if (statusCode >= 400 && ![401, 403].includes(statusCode)) continue
      const html = await response.text().catch(() => '')
      const title = extractHtmlTitle(html)
      const finalUrl = cleanText(response.url || '')
      const finalDomain = registrableDomain(finalUrl)
      const redirected = Boolean(finalUrl && finalUrl !== url)
      const validRedirect =
        redirected
        && Boolean(finalDomain)
        && finalDomain !== domain
        && /^https?:\/\//i.test(finalUrl)
      const ogSiteName = extractMetaContent(html, 'og:site_name')
      const schemaNames = extractSchemaOrgOrganizationNames(html)
      const siteSignals = {
        og_site_name: ogSiteName,
        schema_org_name: schemaNames[0] || '',
        title_brand_segment: titleBrandSegment(title),
        logo_alt: extractLogoAltCandidate(html),
      }
      return {
        reachable: true,
        protocol: url.startsWith('https') ? 'https' : 'http',
        url,
        title,
        siteSignals,
        status_code: statusCode,
        final_url: finalUrl,
        final_domain: finalDomain,
        redirected,
        valid_redirect: validRedirect,
      }
    } catch {
      // continue probing fallback protocol
    }
  }
  return {
    reachable: false,
    protocol: '',
    url: '',
    title: '',
    siteSignals: {},
    status_code: 0,
    final_url: '',
    final_domain: '',
    redirected: false,
    valid_redirect: false,
  }
}

function scoreSeed(seed, probe, resolvedCanonical, knowledgeAlignment, knowledgeBaseEnabled) {
  const base = Number(seed.source_confidence) || 0
  const availabilityBoost = probe.reachable ? 0.2 : -0.15
  const canonicalHint = resolvedCanonical.canonicalBrand || resolvedCanonical.domainFallbackBrand
  const titleMatch = tokenMatchScore(canonicalHint, probe.title || '') * 0.2
  const canonicalConsensusBoost = resolvedCanonical.canonicalConfirmed ? 0.08 : -0.05
  const knowledgeBoost = knowledgeAlignment.matched ? 0.12 : (knowledgeBaseEnabled ? -0.06 : 0)
  const httpsBoost = probe.protocol === 'https' ? 0.05 : 0
  return Number(
    Math.max(0, Math.min(1, base + availabilityBoost + titleMatch + httpsBoost + canonicalConsensusBoost + knowledgeBoost)).toFixed(4),
  )
}

function roundRobinSelect(items, maxBrands) {
  const byVertical = new Map()
  for (const item of items) {
    const key = cleanText(item.vertical_l1) || 'unknown'
    if (!byVertical.has(key)) byVertical.set(key, [])
    byVertical.get(key).push(item)
  }
  const buckets = [...byVertical.entries()]
    .map(([vertical, rows]) => ({
      vertical,
      rows: rows.sort((a, b) => b.source_confidence - a.source_confidence),
      idx: 0,
    }))
    .sort((a, b) => a.vertical.localeCompare(b.vertical))

  const selected = []
  while (selected.length < maxBrands) {
    let pickedInRound = 0
    for (const bucket of buckets) {
      if (selected.length >= maxBrands) break
      if (bucket.idx >= bucket.rows.length) continue
      selected.push(bucket.rows[bucket.idx])
      bucket.idx += 1
      pickedInRound += 1
    }
    if (pickedInRound === 0) break
  }
  return selected
}

async function loadSeeds(args) {
  const explicit = cleanText(args['seed-file'])
  if (explicit) return readJsonl(path.resolve(process.cwd(), explicit))
  const latestMetaPath = path.join(BRAND_SEEDS_DIR, 'latest-brand-seeds.json')
  const latestMeta = await readJson(latestMetaPath, null)
  if (latestMeta?.latestSeedsJsonl) {
    return readJsonl(path.resolve(process.cwd(), latestMeta.latestSeedsJsonl))
  }
  const latestFile = await findLatestFile(BRAND_SEEDS_DIR, '.jsonl')
  if (!latestFile) throw new Error('No brand seeds found. Run merge-search-results first.')
  return readJsonl(latestFile)
}

async function main() {
  const args = parseArgs(process.argv.slice(2))
  const maxBrands = toInteger(args['max-brands'], 500)
  const concurrency = toInteger(args.concurrency, 16)
  const timeoutMs = toInteger(args['timeout-ms'], 8000)
  const skipNetwork = toBoolean(args['skip-network'], false)
  const strictReachable = toBoolean(args['strict-reachable'], false)
  const tag = timestampTag()

  await ensureDir(CURATED_ROOT)
  const seeds = await loadSeeds(args)
  const knowledgeBase = await loadKnowledgeBase(args)

  const inspected = await asyncPool(concurrency, seeds, async (seed) => {
    const domain = pickSeedDomain(seed)
    const domainSyntaxValid = isValidDomainSyntax(domain)
    const probe = skipNetwork
      ? {
          reachable: true,
          protocol: 'https',
          url: `https://${domain}`,
          title: '',
          siteSignals: {},
          status_code: 200,
          final_url: `https://${domain}`,
          final_domain: domain,
          redirected: false,
          valid_redirect: false,
        }
      : await probeDomain(domain, timeoutMs)
    const resolvedCanonical = resolveCanonicalFromSiteSignals(probe, domain)
    const knowledgeAlignment = alignBrandWithKnowledge(
      domain,
      resolvedCanonical.canonicalBrand,
      resolvedCanonical.domainFallbackBrand,
      knowledgeBase,
    )
    const confidence = scoreSeed(seed, probe, resolvedCanonical, knowledgeAlignment, knowledgeBase.hasAnyRecords)
    return {
      seed,
      domain,
      domainSyntaxValid,
      probe,
      resolvedCanonical,
      knowledgeAlignment,
      confidence,
    }
  })

  const domainGateOut = inspected.filter((item) => {
    if (!item.domain) return true
    if (!item.domainSyntaxValid) return true
    if (!(item.probe.reachable || item.probe.valid_redirect)) return true
    if (strictReachable && !item.probe.reachable) return true
    return false
  })

  const candidates = inspected
    .filter((item) => !domainGateOut.includes(item))
    .map((item) => {
      const sourceTitle = cleanText(item.seed.brand_name)
      const sourceNameCheck = detectSourceTitlePollution(sourceTitle)
      const canonicalBrandName = cleanText(
        item.resolvedCanonical.canonicalBrand || item.knowledgeAlignment.matched_canonical_name || '',
      )
      const brandName = canonicalBrandName || item.resolvedCanonical.domainFallbackBrand
      const suspect = shouldMarkSuspect(
        item.knowledgeAlignment,
        item.resolvedCanonical,
        sourceNameCheck,
        item.probe,
      )
      return {
        brand_id: brandId(item.domain, brandName),
        brand_name: brandName,
        canonical_brand_name: canonicalBrandName,
        source_title: sourceTitle,
        vertical_l1: cleanText(item.seed.vertical_l1) || 'unknown',
        vertical_l2: cleanText(item.seed.vertical_l2) || 'unknown',
        market: cleanText(item.seed.market) || 'US',
        official_domain: item.domain,
        source_confidence: item.confidence,
        status: suspect ? 'suspect' : 'active',
        alignment_status: item.knowledgeAlignment.matched ? 'aligned' : 'unaligned',
        alignment_source: item.knowledgeAlignment.matched_source || '',
        evidence: {
          seed_id: item.seed.seed_id || '',
          search_hit_count: Number(item.seed.search_hit_count) || 0,
          source_title: sourceTitle,
          source_title_is_weak: sourceTitleLooksWeak(sourceTitle),
          source_name_invalid: sourceNameCheck.invalid,
          source_name_invalid_reason: sourceNameCheck.reason,
          source_name_trusted: !sourceNameCheck.invalid,
          canonical_confirmed: Boolean(canonicalBrandName),
          canonical_sources: item.resolvedCanonical.canonicalSources,
          site_signals: item.resolvedCanonical.signals,
          kb_alignment: {
            matched: item.knowledgeAlignment.matched,
            source: item.knowledgeAlignment.matched_source,
            id: item.knowledgeAlignment.matched_id,
            mode: item.knowledgeAlignment.mode,
            reason: item.knowledgeAlignment.reason,
            canonical_name: item.knowledgeAlignment.matched_canonical_name,
          },
          suspect_reason: suspect ? 'kb_unaligned_and_evidence_insufficient' : '',
          verified_reachable: Boolean(item.probe.reachable),
          valid_redirect: Boolean(item.probe.valid_redirect),
          redirect_final_url: item.probe.final_url,
          redirect_final_domain: item.probe.final_domain,
          homepage_title: cleanText(item.probe.title).slice(0, 180),
          homepage_url: item.probe.url,
        },
      }
    })
    .sort((a, b) => b.source_confidence - a.source_confidence)

  const selected = roundRobinSelect(candidates, Math.max(1, maxBrands))
  const brandsPath = path.join(CURATED_ROOT, 'brands.jsonl')
  const summaryPath = path.join(CURATED_ROOT, `brands-${tag}.summary.json`)

  await writeJsonl(brandsPath, selected)
  await writeJson(summaryPath, {
    generatedAt: new Date().toISOString(),
    inspectedSeeds: seeds.length,
    domainGateOut: domainGateOut.length,
    domainGateOutByReason: {
      missing_domain: domainGateOut.filter((item) => !item.domain).length,
      invalid_domain_syntax: domainGateOut.filter((item) => item.domain && !item.domainSyntaxValid).length,
      unreachable_or_invalid_redirect: domainGateOut.filter(
        (item) => item.domain && item.domainSyntaxValid && !(item.probe.reachable || item.probe.valid_redirect),
      ).length,
      strict_reachable_failed: domainGateOut.filter(
        (item) =>
          strictReachable
          && item.domain
          && item.domainSyntaxValid
          && !item.probe.reachable
          && item.probe.valid_redirect,
      ).length,
    },
    candidateBrands: candidates.length,
    selectedBrands: selected.length,
    selectedAlignedBrands: selected.filter((item) => item.alignment_status === 'aligned').length,
    selectedSuspectBrands: selected.filter((item) => item.status === 'suspect').length,
    maxBrands,
    skipNetwork,
    strictReachable,
    knowledgeBase: knowledgeBase.counts,
    output: path.relative(process.cwd(), brandsPath),
  })

  console.log(
    JSON.stringify(
      {
        ok: true,
        inspectedSeeds: seeds.length,
        selectedBrands: selected.length,
        output: path.relative(process.cwd(), brandsPath),
      },
      null,
      2,
    ),
  )
}

main().catch((error) => {
  console.error('[verify-brand-domain] failed:', error?.message || error)
  process.exit(1)
})
