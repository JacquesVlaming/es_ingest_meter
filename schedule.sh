#!/bin/sh
set -e

INTERVAL=${INTERVAL_HOURS:-24}
INTERVAL_SECS=$((INTERVAL * 3600))

echo "=== ES Data Generator — scheduled every ${INTERVAL}h ==="
echo "    Host    : ${ES_HOST}"
echo "    Target  : ${TARGET_MB:-10} MB per run"
echo ""

run() {
    echo ">>> $(date -u '+%Y-%m-%dT%H:%M:%SZ') — starting ingest run"
    python es_data_gen.py \
        --host        "${ES_HOST}" \
        ${ES_USERNAME:+--username "${ES_USERNAME}"} \
        ${ES_PASSWORD:+--password "${ES_PASSWORD}"} \
        ${ES_API_KEY:+--api-key  "${ES_API_KEY}"}  \
        --target-mb   "${TARGET_MB:-10}"            \
        --doc-size-kb "${DOC_SIZE_KB:-1}"           \
        --batch-size  "${BATCH_SIZE:-500}"          \
        --parallel    "${PARALLEL:-1}"              \
        ${KEEP_INDEX:+--keep}                       \
        ${INSECURE:+--insecure}
    echo "<<< $(date -u '+%Y-%m-%dT%H:%M:%SZ') — run complete, sleeping ${INTERVAL}h"
}

# Run immediately on start, then on the interval
run
while true; do
    sleep "${INTERVAL_SECS}"
    run
done
