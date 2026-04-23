#!/usr/bin/env python3
"""
es_data_gen.py

Ingests a target volume of synthetic data into an Elasticsearch cluster,
then re-runs the ingest meter to verify what was recorded on disk.

Usage:
  python es_data_gen.py --host ... --api-key ... --target-mb 100
"""

import argparse
import json
import math
import random
import string
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    sys.exit("Missing dependency: pip install requests")

import es_logger

TEMPLATE_NAME    = "es-ingest-meter-indices"
TEMPLATE_PATTERN = "es-ingest-meter-test-*"
ILM_POLICY_NAME  = "ingest-meter-policy"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest a target volume of data into ES, then verify with es_ingest_meter.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest 50 MB using an API key
  python es_data_gen.py --host https://... --api-key BASE64 --target-mb 50

  # Ingest 200 docs of ~4 KB each (basic auth)
  python es_data_gen.py --host https://... -u elastic -p secret --docs 200 --doc-size-kb 4

  # Skip the meter re-check after ingestion
  python es_data_gen.py --host https://... --api-key BASE64 --target-mb 10 --no-verify
""",
    )
    p.add_argument("--host", default="http://localhost:9200")
    p.add_argument("-u", "--username")
    p.add_argument("-p", "--password")
    p.add_argument("--api-key", metavar="BASE64")
    p.add_argument("--insecure", action="store_true")

    # Volume controls (mutually exclusive)
    vol = p.add_mutually_exclusive_group()
    vol.add_argument("--target-mb", type=float, default=10.0,
                     help="Target uncompressed ingest volume in MB (default: 10)")
    vol.add_argument("--docs", type=int,
                     help="Exact number of documents to ingest (overrides --target-mb)")

    p.add_argument("--doc-size-kb", type=float, default=1.0,
                   help="Approximate size of each document in KB (default: 1.0)")
    p.add_argument("--batch-size", type=int, default=500,
                   help="Documents per bulk request (default: 500)")
    p.add_argument("--index", default=None,
                   help="Target index name (default: es-ingest-meter-test-YYYY.MM.DD)")
    p.add_argument("--parallel", type=int, default=1, metavar="N",
                   help="Number of concurrent bulk indexing threads (default: 1)")
    p.add_argument("--no-verify", action="store_true",
                   help="Skip re-running es_ingest_meter.py after ingestion")
    p.add_argument("--keep", action="store_true",
                   help="Keep the test index after the run (default: delete it)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# ES client (same minimal pattern as es_ingest_meter.py)
# ---------------------------------------------------------------------------

class ESClient:
    def __init__(self, host, username=None, password=None, api_key=None, verify=True):
        self.host = host.rstrip("/")
        self.verify = verify
        self.session = requests.Session()
        if username and password:
            self.session.auth = HTTPBasicAuth(username, password)
        if api_key:
            self.session.headers["Authorization"] = f"ApiKey {api_key}"

    def get(self, path: str, **params) -> dict:
        r = self.session.get(f"{self.host}{path}", params=params or None,
                             verify=self.verify, timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, data: str, content_type="application/x-ndjson") -> dict:
        r = self.session.post(
            f"{self.host}{path}",
            data=data,
            headers={"Content-Type": content_type},
            verify=self.verify,
            timeout=60,
        )
        r.raise_for_status()
        return r.json()

    def delete(self, path: str) -> dict:
        r = self.session.delete(f"{self.host}{path}", verify=self.verify, timeout=30)
        r.raise_for_status()
        return r.json()

    def put(self, path: str, body: dict) -> dict:
        r = self.session.put(f"{self.host}{path}", json=body,
                             verify=self.verify, timeout=30)
        r.raise_for_status()
        return r.json()


# ---------------------------------------------------------------------------
# Synthetic document generator
# ---------------------------------------------------------------------------

_LOG_LEVELS   = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"]
_HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
_HTTP_PATHS   = ["/api/users", "/api/orders", "/api/products", "/health", "/metrics"]
_SERVICES     = ["auth-service", "order-service", "product-api", "gateway", "worker"]


def _rand_ip() -> str:
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


def _rand_str(length: int) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def make_document(target_bytes: int) -> dict:
    """Generate a realistic-ish log/event document of approximately target_bytes."""
    doc = {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "service":    random.choice(_SERVICES),
        "log": {
            "level":   random.choice(_LOG_LEVELS),
            "message": f"Request processed in {random.randint(1, 5000)} ms",
        },
        "http": {
            "method":       random.choice(_HTTP_METHODS),
            "path":         random.choice(_HTTP_PATHS),
            "status_code":  random.choice([200, 200, 200, 201, 400, 404, 500]),
            "response_ms":  random.randint(1, 5000),
        },
        "client": {
            "ip":       _rand_ip(),
            "user_id":  random.randint(1000, 999999),
        },
        "host": {
            "name": f"node-{random.randint(1, 20):02d}",
        },
        "trace_id": _rand_str(32),
    }

    # Pad with a filler field to hit the target byte size
    current = len(json.dumps(doc).encode())
    padding = max(0, target_bytes - current - 12)  # 12 ≈ key + quotes overhead
    if padding > 0:
        doc["_pad"] = _rand_str(padding)

    return doc


def bulk_body(docs: list[dict], index: str) -> str:
    lines = []
    meta = json.dumps({"index": {"_index": index}})
    for doc in docs:
        lines.append(meta)
        lines.append(json.dumps(doc))
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Progress display
# ---------------------------------------------------------------------------

def progress_bar(done: int, total: int, width: int = 30) -> str:
    pct   = done / total if total else 0
    filled = int(pct * width)
    bar   = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {pct*100:5.1f}%"


def human_bytes(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(n) < 1024:
            return f"{n:,.2f} {unit}"
        n /= 1024
    return f"{n:,.2f} GB"


# ---------------------------------------------------------------------------
# ILM policy + Index template
# ---------------------------------------------------------------------------

def ensure_ilm_policy(client: ESClient) -> None:
    """Create or update the ILM policy for test indices."""
    body = {
        "policy": {
            "phases": {
                "hot": {
                    "actions": {}
                },
                "delete": {
                    "min_age": "30d",
                    "actions": {
                        "delete": {}
                    }
                }
            }
        }
    }
    client.put(f"/_ilm/policy/{ILM_POLICY_NAME}", body)


def ensure_index_template(client: ESClient) -> None:
    """Create or update the composable index template for test indices."""
    body = {
        "index_patterns": [TEMPLATE_PATTERN],
        "priority": 100,
        "template": {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "index.lifecycle.name": ILM_POLICY_NAME,
            },
            "mappings": {
                "_size": {"enabled": True},
                "dynamic": True,
                "properties": {
                    "@timestamp": {"type": "date"},
                    "service":    {"type": "keyword"},
                    "trace_id":   {"type": "keyword"},
                    "log": {"properties": {
                        "level":   {"type": "keyword"},
                        "message": {"type": "text"},
                    }},
                    "http": {"properties": {
                        "method":      {"type": "keyword"},
                        "path":        {"type": "keyword"},
                        "status_code": {"type": "integer"},
                        "response_ms": {"type": "long"},
                    }},
                    "client": {"properties": {
                        "ip":      {"type": "ip"},
                        "user_id": {"type": "long"},
                    }},
                    "host": {"properties": {
                        "name": {"type": "keyword"},
                    }},
                },
            },
        },
    }
    client.put(f"/_index_template/{TEMPLATE_NAME}", body)


# ---------------------------------------------------------------------------
# Main ingestion logic
# ---------------------------------------------------------------------------

def ingest(client: ESClient, index: str, total_docs: int, doc_bytes: int,
           batch_size: int, workers: int = 1) -> dict:

    print(f"\n  Target  : {total_docs:,} docs × ~{human_bytes(doc_bytes)} = "
          f"~{human_bytes(total_docs * doc_bytes)} uncompressed")
    print(f"  Index   : {index}")
    print(f"  Batch   : {batch_size} docs/request")
    print(f"  Threads : {workers}\n")

    # Shared state
    lock         = threading.Lock()
    docs_remaining = total_docs
    sent_docs    = 0
    sent_bytes   = 0
    errors       = 0
    t_start      = time.time()

    def worker() -> None:
        nonlocal docs_remaining, sent_docs, sent_bytes, errors
        while True:
            with lock:
                if docs_remaining <= 0:
                    return
                count = min(batch_size, docs_remaining)
                docs_remaining -= count

            docs = [make_document(doc_bytes) for _ in range(count)]
            body = bulk_body(docs, index)
            body_bytes = len(body.encode())

            resp = client.post("/_bulk", body)
            bulk_errors = 0
            if resp.get("errors"):
                bulk_errors = sum(
                    1 for item in resp["items"]
                    if item.get("index", {}).get("error")
                )

            with lock:
                sent_docs  += count
                sent_bytes += body_bytes
                errors     += bulk_errors

    # Launch workers and print progress from main thread
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker) for _ in range(workers)]

        while any(not f.done() for f in futures):
            with lock:
                d, b = sent_docs, sent_bytes
            elapsed = time.time() - t_start
            rate    = b / elapsed if elapsed > 0 else 0
            print(f"\r  {progress_bar(d, total_docs)}  "
                  f"{d:>7,}/{total_docs:,} docs  "
                  f"{human_bytes(b):>10} sent  "
                  f"{human_bytes(rate)}/s  ", end="", flush=True)
            time.sleep(0.3)

        # Raise any worker exceptions
        for f in futures:
            f.result()

    elapsed = time.time() - t_start
    print()  # newline after progress bar
    return {"sent_docs": sent_docs, "sent_bytes": sent_bytes,
            "errors": errors, "elapsed_s": elapsed}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    client = ESClient(
        host=args.host,
        username=args.username,
        password=args.password,
        api_key=args.api_key,
        verify=not args.insecure,
    )

    log = es_logger.setup(
        "es-ingest-scheduler",
        host=args.host,
        username=args.username,
        password=args.password,
        api_key=args.api_key,
        verify=not args.insecure,
    )

    # Confirm connectivity
    info = client.get("/")
    cluster = info.get("cluster_name", "unknown")
    version = info.get("version", {}).get("number", "?")
    print(f"\nConnected to cluster '{cluster}' (ES {version})")

    # Resolve index name
    today = datetime.now(timezone.utc).strftime("%Y.%m.%d")
    index = args.index or f"es-ingest-meter-test-{today}"

    # Resolve doc count
    doc_bytes  = max(1, int(args.doc_size_kb * 1024))
    if args.docs:
        total_docs = args.docs
    else:
        total_docs = max(1, int((args.target_mb * 1024 * 1024) / doc_bytes))

    log.info("Generator run started", extra={
        "cluster": cluster,
        "es_version": version,
        "target_index": index,
        "target_docs": total_docs,
        "doc_size_bytes": doc_bytes,
        "target_mb": args.target_mb if not args.docs else None,
    })

    # Ensure ILM policy and index template are in place before first bulk request
    for label, fn in [
        (f"ILM policy '{ILM_POLICY_NAME}'", ensure_ilm_policy),
        (f"Index template '{TEMPLATE_NAME}' (pattern: {TEMPLATE_PATTERN})", ensure_index_template),
    ]:
        try:
            fn(client)
            print(f"{label} ready")
        except requests.exceptions.HTTPError as e:
            body = ""
            try:
                body = e.response.json()
            except Exception:
                body = e.response.text
            log.warning(f"Could not apply {label} — continuing with cluster defaults",
                        extra={"error": str(e), "response": body})
            print(f"  Warning: {label} unavailable ({e.response.status_code}): {body}")

    # Ingest
    try:
        result = ingest(client, index, total_docs, doc_bytes, args.batch_size, args.parallel)
    except Exception as e:
        log.error("Generator run failed during ingest", extra={"index": index, "error": str(e)})
        raise

    # Summary
    elapsed = result["elapsed_s"]
    rate    = result["sent_bytes"] / elapsed if elapsed > 0 else 0
    print(f"\n  ✓ Ingested {result['sent_docs']:,} docs in {elapsed:.1f}s "
          f"({human_bytes(rate)}/s)")
    if result["errors"]:
        print(f"  ⚠ {result['errors']} bulk errors — check index mapping or shard health")

    log.info("Generator run completed", extra={
        "cluster": cluster,
        "target_index": index,
        "sent_docs": result["sent_docs"],
        "sent_bytes": result["sent_bytes"],
        "bulk_errors": result["errors"],
        "elapsed_s": round(elapsed, 2),
        "rate_kb_s": round(rate / 1024, 2),
    })

    # Force a refresh so stats are visible immediately
    print("  Refreshing index... ", end="", flush=True)
    client.post(f"/{index}/_refresh", "")
    print("done")

    # Wait briefly for store stats to settle
    time.sleep(2)

    # Re-run the meter
    if not args.no_verify:
        print("\n" + "─" * 68)
        print("  Re-running es_ingest_meter.py to verify on-disk size...")
        print("─" * 68)

        meter_cmd = [
            sys.executable, "es_ingest_meter.py",
            "--host", args.host,
            "--pattern", "es-ingest-meter-test-*",
            "--days", "1",
        ]
        if args.username:
            meter_cmd += ["-u", args.username, "-p", args.password]
        if args.api_key:
            meter_cmd += ["--api-key", args.api_key]
        if args.insecure:
            meter_cmd.append("--insecure")

        subprocess.run(meter_cmd)

        # Compression ratio note
        print(f"  Uncompressed sent : {human_bytes(result['sent_bytes'])}")
        print(f"  (Compare to 'Primary' above to see Lucene compression ratio)\n")

    # Cleanup
    if not args.keep:
        client.delete(f"/{index}")
        print(f"  Test index '{index}' deleted.  (Use --keep to retain it)\n")


if __name__ == "__main__":
    main()
