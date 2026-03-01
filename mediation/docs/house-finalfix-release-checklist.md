# House Finalfix Release Checklist

This checklist publishes the `finalfix` Meyka/DeepAI library into House curated artifacts.

## 0) Inputs (fixed for this release)

- Offers: `output/inventory-audit/product-libraries/meyka-deepai-offers-finalfix-qa-accepted-20260301.jsonl`
- Brands: `output/inventory-audit/product-libraries/meyka-deepai-brands-combined-20260301.jsonl`
- Link health report: `output/inventory-audit/link-health-finalfix-meyka-deepai-20260301.json`
- Image health report: `output/inventory-audit/image-health-finalfix-meyka-deepai-20260301.json`
- Coverage report: `output/inventory-audit/product-coverage-finalfix-meyka-deepai-20260301.json`
- Dialogue report: `output/product-dialogue/product-dialogue-report-finalfix-20260301.json`

## 1) Preflight checks

Run from repo root:

```bash
cd mediation
```

### 1.1 File existence + row counts

```bash
test -f output/inventory-audit/product-libraries/meyka-deepai-offers-finalfix-qa-accepted-20260301.jsonl
test -f output/inventory-audit/product-libraries/meyka-deepai-brands-combined-20260301.jsonl
wc -l output/inventory-audit/product-libraries/meyka-deepai-offers-finalfix-qa-accepted-20260301.jsonl
wc -l output/inventory-audit/product-libraries/meyka-deepai-brands-combined-20260301.jsonl
```

### 1.2 Health/coverage gate (hard fail on threshold miss)

```bash
node - <<'NODE'
const fs = require('fs')
const j = (p) => JSON.parse(fs.readFileSync(p, 'utf8'))
const link = j('output/inventory-audit/link-health-finalfix-meyka-deepai-20260301.json')
const image = j('output/inventory-audit/image-health-finalfix-meyka-deepai-20260301.json')
const coverage = j('output/inventory-audit/product-coverage-finalfix-meyka-deepai-20260301.json')
const dialogue = j('output/product-dialogue/product-dialogue-report-finalfix-20260301.json')

const linkOkRate = link.summary.ok_count / Math.max(1, link.summary.total_rows)
const imageValidRate = image.summary.valid_image_count / Math.max(1, image.summary.total_rows)
const meykaHits = Number(coverage.summary.brand_hits_by_product?.meyka || 0)
const deepaiHits = Number(coverage.summary.brand_hits_by_product?.deepai || 0)
const served = Number(dialogue.summary.combined_outcomes?.served || 0)

const failures = []
if (linkOkRate < 0.65) failures.push(`link_ok_rate_lt_0.65:${linkOkRate.toFixed(4)}`)
if (imageValidRate < 0.90) failures.push(`image_valid_rate_lt_0.90:${imageValidRate.toFixed(4)}`)
if (meykaHits < 1) failures.push(`meyka_hits_lt_1:${meykaHits}`)
if (deepaiHits < 1) failures.push(`deepai_hits_lt_1:${deepaiHits}`)
if (served < 10) failures.push(`served_lt_10:${served}`)

if (failures.length) {
  console.error(JSON.stringify({ ok: false, failures }, null, 2))
  process.exit(1)
}

console.log(
  JSON.stringify(
    {
      ok: true,
      link_ok_rate: Number(linkOkRate.toFixed(4)),
      image_valid_rate: Number(imageValidRate.toFixed(4)),
      meyka_hits: meykaHits,
      deepai_hits: deepaiHits,
      served,
    },
    null,
    2,
  ),
)
NODE
```

### 1.3 Re-run QA on finalfix offers

```bash
node scripts/house-ads/qa-offers.js \
  --offers-file=output/inventory-audit/product-libraries/meyka-deepai-offers-finalfix-qa-accepted-20260301.jsonl \
  --brands-file=output/inventory-audit/product-libraries/meyka-deepai-brands-combined-20260301.jsonl \
  --output-file=output/inventory-audit/product-libraries/meyka-deepai-offers-finalfix-prepublish-qa-accepted.jsonl
```

## 2) Publish commands

### 2.1 Backup current House curated files

```bash
mkdir -p data/house-ads/curated/.backup
ts=$(date +%Y%m%d-%H%M%S)
[ -f data/house-ads/curated/brands.jsonl ] && cp data/house-ads/curated/brands.jsonl data/house-ads/curated/.backup/brands.$ts.jsonl || true
[ -f data/house-ads/curated/link-creatives.jsonl ] && cp data/house-ads/curated/link-creatives.jsonl data/house-ads/curated/.backup/link-creatives.$ts.jsonl || true
[ -f data/house-ads/curated/product-creatives.jsonl ] && cp data/house-ads/curated/product-creatives.jsonl data/house-ads/curated/.backup/product-creatives.$ts.jsonl || true
```

### 2.2 Stage brands + publish offer creatives

```bash
cp output/inventory-audit/product-libraries/meyka-deepai-brands-combined-20260301.jsonl data/house-ads/curated/brands.jsonl

node scripts/house-ads/publish-offer-creatives.js \
  --offers-file=output/inventory-audit/product-libraries/meyka-deepai-offers-finalfix-qa-accepted-20260301.jsonl
```

### 2.3 Bridge to `qa-and-publish` schema (required)

`publish-offer-creatives.js` writes into `data/house-ads/offers/curated/`; `qa-and-publish.js` reads from `data/house-ads/curated/`.

```bash
cp data/house-ads/offers/curated/link-offers.jsonl data/house-ads/curated/link-creatives.jsonl
cp data/house-ads/offers/curated/product-offers.jsonl data/house-ads/curated/product-creatives.jsonl
```

### 2.4 Snapshot + publish gate

This finalfix library is product-heavy; use `--required-completeness=0` for this release gate.

```bash
node scripts/house-ads/qa-and-publish.js --min-brands=60 --required-completeness=0
```

## 3) Post-publish verification

```bash
cat data/house-ads/snapshots/latest-snapshot.json
latest_dir=$(node -e "const fs=require('fs');const j=JSON.parse(fs.readFileSync('data/house-ads/snapshots/latest-snapshot.json','utf8'));process.stdout.write(j.latestSnapshotDir)")
cat "$latest_dir/manifest.json"
```

## 4) Rollback

```bash
latest_backup=$(ls -1t data/house-ads/curated/.backup/brands.*.jsonl | head -n 1)
cp "$latest_backup" data/house-ads/curated/brands.jsonl

latest_link_backup=$(ls -1t data/house-ads/curated/.backup/link-creatives.*.jsonl | head -n 1)
cp "$latest_link_backup" data/house-ads/curated/link-creatives.jsonl

latest_product_backup=$(ls -1t data/house-ads/curated/.backup/product-creatives.*.jsonl | head -n 1)
cp "$latest_product_backup" data/house-ads/curated/product-creatives.jsonl
```
