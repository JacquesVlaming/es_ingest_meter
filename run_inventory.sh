#!/bin/sh
set -e

exec python es_ilm_inventory.py \
    --host "${ES_HOST}" \
    ${ES_API_KEY:+--api-key  "${ES_API_KEY}"}  \
    ${ES_USERNAME:+--username "${ES_USERNAME}"} \
    ${ES_PASSWORD:+--password "${ES_PASSWORD}"} \
    --days "${DAYS:-30}"                        \
    ${INSECURE:+--insecure}                     \
    "$@"
