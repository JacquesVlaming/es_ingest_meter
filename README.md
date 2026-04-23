# ES Ingest Meter

A toolset for accurately measuring daily data ingest volume on an Elasticsearch cluster, used to right-size Elastic deployments.

## Tools

| Tool | Description |
|---|---|
| `es_ingest_meter.py` | Reads `_stats` from all (or filtered) indices, groups by date, and computes rolling average daily ingest with storage projections |
| `es_data_gen.py` | Ingests a configurable volume of synthetic data to cross-validate what the meter reports |
| `es_logger.py` | Shared ECS logging handler — ships structured run events to an `es-ingest-meter-logs-*` index |

## How it works

The meter reads `primaries.store.size_in_bytes` and `docs.count` from the `_stats` API to compute an average document size per index. It then runs a `date_histogram` aggregation on the timestamp field to get the actual document count per day, and multiplies by the average doc size to estimate daily storage — regardless of whether the index name contains a date.

For indices with no data in the timestamp field within the window (e.g. no `@timestamp` field), the meter falls back to spreading the index's total size evenly across days since its creation date.

The current day will always show partial data.

## Assumptions and requirements

- **Timestamp field**: Indices must contain a date field for accurate per-day bucketing. Defaults to `@timestamp`. Override with `--timestamp-field`. Indices without this field fall back to creation-date spreading.
- **`_stats` API access**: The credentials used must have `monitor` or `manage` privileges on the target indices.
- **Aggregations enabled**: The timestamp field must be mapped as `date` type and not disabled for aggregations.
- **Average doc size is uniform**: Storage per day is estimated as `avg_doc_size × docs_that_day`. If document size varies significantly over time, estimates may be skewed.
- **Today's data is partial**: The current day's row reflects ingest so far, not a full day. Exclude today from averages when comparing full days.
- **Replicas counted separately**: The `With Replicas` column includes all replica copies. The replica multiplier shown is observed from the cluster — adjust your sizing targets accordingly.

## Quickstart

### Prerequisites

```bash
pip install requests
```

Or use Docker (recommended).

### Measure ingest

```bash
python es_ingest_meter.py \
  --host https://your-cluster.es.io:443 \
  --api-key your-base64-api-key \
  --days 30
```

### Cross-validate with synthetic data

```bash
python es_data_gen.py \
  --host https://your-cluster.es.io:443 \
  --api-key your-base64-api-key \
  --target-mb 50
```

## Docker

Two separate images — one for each tool.

### Build

```bash
docker compose build
```

### Configure

Copy `.env.example` to `.env` and fill in your cluster details:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `ES_HOST` | — | Elasticsearch base URL |
| `ES_API_KEY` | — | Base64-encoded API key |
| `ES_USERNAME` | — | Basic auth username (alternative to API key) |
| `ES_PASSWORD` | — | Basic auth password |
| `TARGET_MB` | `10` | MB of synthetic data per scheduler run |
| `DOC_SIZE_KB` | `1` | Approximate document size in KB |
| `BATCH_SIZE` | `500` | Documents per bulk request |
| `INTERVAL_HOURS` | `24` | Hours between scheduler runs |
| `KEEP_INDEX` | — | Set to `1` to retain test indices after each run |
| `PARALLEL` | `1` | Number of concurrent bulk indexing threads |

### Run the meter (one-shot)

```bash
docker compose run --rm meter
```

### Run the scheduler (24h loop)

```bash
docker compose up -d ingest-scheduler
docker compose logs -f ingest-scheduler
```

### Export to CSV

```bash
docker compose run --rm meter --csv /output/report.csv
# or directly:
python es_ingest_meter.py --host ... --api-key ... --csv report.csv
```

## Output

```
╔══════════════════════════════════════════════════════════════╗
║  Elasticsearch Ingest Meter                                  ║
╠══════════════════════════════════════════════════════════════╣
║  Cluster : my-cluster                                        ║
║  Version : 9.3.2                                             ║
║  Pattern : *                                                 ║
║  Window  : 30 days requested, 14 days with data              ║
╚══════════════════════════════════════════════════════════════╝

  Date               Primary   With Replicas       Documents  Relative
  ────────────────────────────────────────────────────────────────────────
  2026-03-14         1.20 GB         2.41 GB       2,100,000  ███████░░░░░░░░░░░
  2026-03-15         1.85 GB         3.70 GB       3,240,000  ██████████████████
  ...

  ┌─ Rolling Averages (14 days) ─────────────────────────────────────┐
  │  Avg primary / day   : 1.52 GB                               │
  │  Avg total / day     : 3.04 GB                               │
  │  Avg documents / day : 2,670,000                             │
  │  Observed replica ×  : 2.00x                                 │
  ├─ Projections ────────────────────────────────────────────────────┤
  │  30-day primary      : 44.60 GB                              │
  │  30-day total        : 89.20 GB                              │
  │  90-day primary      : 133.80 GB                             │
  │  365-day primary     : 554.80 GB                             │
  └──────────────────────────────────────────────────────────────────┘
```

## Runtime logs in Elasticsearch

Both tools ship structured ECS log events to `es-ingest-meter-logs-YYYY.MM.DD`. Use Kibana to build dashboards tracking ingest rate trends across scheduler runs.

Example events:

```json
{ "message": "Meter run completed", "labels": { "avg_primary_bytes_per_day": 1632428032, "avg_docs_per_day": 2670000, "projected_30d_primary_bytes": 48972840960 } }
{ "message": "Generator run completed", "labels": { "sent_docs": 10240, "sent_bytes": 10813440, "elapsed_s": 28.4, "rate_kb_s": 372.1 } }
```

## Authentication

| Method | Flag |
|---|---|
| API key | `--api-key BASE64` |
| Basic auth | `-u elastic -p changeme` |
| Unauthenticated | _(omit both)_ |
| Skip TLS verification | `--insecure` |
