const DEFAULT_DIMENSION = 512

function cleanText(value) {
  return String(value || '').trim().replace(/\s+/g, ' ')
}

function tokenize(value = '') {
  return cleanText(value)
    .toLowerCase()
    .split(/[^a-z0-9\u4e00-\u9fff]+/g)
    .filter((token) => token.length >= 2)
}

function hash32(input = '') {
  let hash = 2166136261
  const text = String(input)
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }
  return hash >>> 0
}

function toFiniteNumber(value, fallback = 0) {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  return n
}

function normalizeVector(vector = []) {
  let norm = 0
  for (const value of vector) {
    norm += value * value
  }
  if (norm <= 0) return vector
  const invNorm = 1 / Math.sqrt(norm)
  return vector.map((value) => Number((value * invNorm).toFixed(8)))
}

export function buildTextEmbedding(input = {}, options = {}) {
  const dimension = Math.max(8, Math.min(2048, Math.floor(Number(options.dimension) || DEFAULT_DIMENSION)))
  const vector = new Array(dimension).fill(0)

  const title = cleanText(input.title)
  const description = cleanText(input.description)
  const retrievalText = cleanText(input.retrievalText)
  const tags = Array.isArray(input.tags) ? input.tags : []
  const baseTokens = retrievalText
    ? tokenize(retrievalText)
    : [
        ...tokenize(title),
        ...tokenize(description),
      ]
  const tokens = [...baseTokens, ...tags.flatMap((item) => tokenize(item))]

  for (const token of tokens) {
    const hash = hash32(token)
    const index = hash % dimension
    const sign = (hash & 1) === 0 ? 1 : -1
    const weight = token.length >= 6 ? 1.2 : 1
    vector[index] += sign * weight
  }

  const normalized = normalizeVector(vector)
  return {
    model: 'hash-embedding-v1',
    dimension,
    vector: normalized,
  }
}

export function buildQueryEmbedding(query = '', options = {}) {
  return buildTextEmbedding({ title: cleanText(query), description: '', tags: [] }, options)
}

export function vectorToSqlLiteral(vector = []) {
  const values = Array.isArray(vector)
    ? vector.map((value) => toFiniteNumber(value, 0).toFixed(8))
    : []
  return `[${values.join(',')}]`
}

export function cosineSimilarity(left = [], right = []) {
  if (!Array.isArray(left) || !Array.isArray(right) || left.length === 0 || right.length === 0) return 0
  const size = Math.min(left.length, right.length)
  let dot = 0
  let normL = 0
  let normR = 0
  for (let i = 0; i < size; i += 1) {
    const a = toFiniteNumber(left[i], 0)
    const b = toFiniteNumber(right[i], 0)
    dot += a * b
    normL += a * a
    normR += b * b
  }
  if (normL <= 0 || normR <= 0) return 0
  const score = dot / (Math.sqrt(normL) * Math.sqrt(normR))
  return Math.max(-1, Math.min(1, score))
}

export { DEFAULT_DIMENSION }
