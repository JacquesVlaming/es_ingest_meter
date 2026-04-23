#!/usr/bin/env python3
"""
es_ilm_inventory.py

Inventories all logical data sources on an Elasticsearch cluster and their
ILM (Index Lifecycle Management) configuration. Designed for capacity planning:
shows rollover trigger, retention tier ages, index count, and current storage
for every data stream and ILM-managed index family.

Usage:
  python es_ilm_inventory.py --host https://... --api-key BASE64KEY
  python es_ilm_inventory.py --host https://... -u elastic -p secret --csv out.csv
"""

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    sys.exit("Missing dependency: pip install requests")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Inventory data sources and ILM policies for capacity planning.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python es_ilm_inventory.py --host https://my-cluster.es.io:9243 --api-key BASE64KEY
  python es_ilm_inventory.py --host https://... -u elastic -p secret --csv ilm_inventory.csv
  python es_ilm_inventory.py --include-system
""",
    )
    p.add_argument("--host", default="http://localhost:9200",
                   help="Elasticsearch base URL (default: http://localhost:9200)")
    p.add_argument("-u", "--username", help="Username for basic auth")
    p.add_argument("-p", "--password", help="Password for basic auth")
    p.add_argument("--api-key", metavar="BASE64",
                   help="API key value (base64-encoded id:key string)")
    p.add_argument("--insecure", action="store_true",
                   help="Disable TLS certificate verification")
    p.add_argument("--include-system", action="store_true",
                   help="Include system indices/data streams (starting with '.')")
    p.add_argument("--days", type=int, default=30,
                   help="Rolling window for daily ingest averages (default: 30)")
    p.add_argument("--csv", metavar="FILE",
                   help="Export inventory to CSV file")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Elasticsearch client
# ---------------------------------------------------------------------------

class ESClient:
    def __init__(
        self,
        host: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        api_key: Optional[str] = None,
        verify: bool = True,
    ):
        self.host = host.rstrip("/")
        self.verify = verify
        self.session = requests.Session()

        if username and password:
            self.session.auth = HTTPBasicAuth(username, password)
        if api_key:
            self.session.headers["Authorization"] = f"ApiKey {api_key}"

    def get(self, path: str, **params) -> dict:
        url = f"{self.host}{path}"
        try:
            resp = self.session.get(url, params=params or None, verify=self.verify, timeout=60)
        except requests.exceptions.ConnectionError:
            sys.exit(f"Connection refused — is Elasticsearch running at {self.host}?")
        except requests.exceptions.Timeout:
            sys.exit("Request timed out.")

        if resp.status_code == 401:
            sys.exit("Authentication failed. Check --username/--password or --api-key.")
        if resp.status_code == 403:
            sys.exit("Authorization failed. The credentials lack the required privileges.")
        resp.raise_for_status()
        return resp.json()

    def safe_get(self, path: str, **params) -> Optional[dict]:
        """Returns the parsed JSON body, None on error, or raises PermissionError on 403."""
        url = f"{self.host}{path}"
        try:
            resp = self.session.get(url, params=params or None, verify=self.verify, timeout=60)
            if resp.status_code == 403:
                raise PermissionError(path)
            if not resp.ok:
                return None
            return resp.json()
        except PermissionError:
            raise
        except Exception:
            return None

    def msearch(self, bodies: list[tuple[dict, dict]]) -> list[dict]:
        """
        Run multiple searches in one round-trip.
        bodies is a list of (header, query) pairs.
        Returns a list of response dicts (one per query), empty dict on failure.
        """
        lines = []
        for header, query in bodies:
            lines.append(json.dumps(header))
            lines.append(json.dumps(query))
        payload = "\n".join(lines) + "\n"
        url = f"{self.host}/_msearch"
        try:
            resp = self.session.post(
                url, data=payload,
                headers={"Content-Type": "application/x-ndjson"},
                verify=self.verify, timeout=120,
            )
            if not resp.ok:
                return [{} for _ in bodies]
            return resp.json().get("responses", [{} for _ in bodies])
        except Exception:
            return [{} for _ in bodies]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]


def human_bytes(n: float) -> str:
    if n == 0:
        return "0 B"
    for unit in _UNITS:
        if abs(n) < 1024:
            return f"{n:,.2f} {unit}"
        n /= 1024
    return f"{n:,.2f} PB"


def format_rollover(hot_actions: dict) -> str:
    rollover = hot_actions.get("rollover", {})
    if not rollover:
        return "—"
    parts = []
    size = rollover.get("max_primary_shard_size") or rollover.get("max_size")
    if size:
        parts.append(size)
    if rollover.get("max_age"):
        parts.append(rollover["max_age"])
    docs = rollover.get("max_primary_shard_docs") or rollover.get("max_docs")
    if docs:
        parts.append(f"{docs:,}d")
    return " / ".join(parts) if parts else "—"


def format_phase_age(phases: dict, phase_name: str) -> str:
    phase = phases.get(phase_name)
    if not phase:
        return "—"
    age = phase.get("min_age", "")
    age_str = "0d" if not age or age in ("0ms", "0s") else age
    if "searchable_snapshot" in phase.get("actions", {}):
        age_str += " SS"
    return age_str


# Strip date/generation suffixes to derive the logical index family name.
# Handles:
#   logs-myapp-2024.03.27-000001  →  logs-myapp
#   filebeat-8.0.0-2024.03.27    →  filebeat-8.0.0
#   myindex-000042               →  myindex
_SUFFIX_RE = re.compile(
    r"[-_](\d{4}[.\-]\d{2}[.\-]\d{2}(-\d{6})?|\d{4}[.\-]\d{2}|\d{6}|\d{1,6})$"
)


def index_family(name: str) -> str:
    """Derive the logical family name by stripping date/generation suffixes."""
    # Unwrap data stream backing index prefix
    bare = re.sub(r"^\.ds-", "", name)
    result = _SUFFIX_RE.sub("", bare)
    # If nothing was stripped, return as-is
    return result if result != bare else bare


# ---------------------------------------------------------------------------
# Data gathering
# ---------------------------------------------------------------------------

def fetch_index_sizes(client: ESClient) -> dict[str, dict]:
    """Return {index_name: {primary_bytes, total_bytes, doc_count}} for all indices."""
    raw = client.safe_get("/*/_stats/store,docs", level="indices",
                          expand_wildcards="all,hidden")
    if not raw:
        return {}
    result = {}
    for name, stats in raw.get("indices", {}).items():
        result[name] = {
            "primary_bytes": stats["primaries"]["store"]["size_in_bytes"],
            "total_bytes":   stats["total"]["store"]["size_in_bytes"],
            "doc_count":     stats["primaries"]["docs"]["count"],
        }
    return result


def fetch_ilm_policies(client: ESClient, policy_names: set[str]) -> tuple[dict[str, dict], bool]:
    """
    Return ({policy_name: phases_dict}, phases_available).

    phases_available is False when the credentials lack read_ilm privilege,
    meaning policy names are known but phase details could not be retrieved.
    """
    if not policy_names:
        return {}, True
    try:
        raw = client.safe_get(f"/_ilm/policy/{','.join(policy_names)}") or {}
    except PermissionError:
        return {}, False
    return {
        name: data.get("policy", {}).get("phases", {})
        for name, data in raw.items()
    }, True


def fetch_data_streams(client: ESClient, include_system: bool) -> list[dict]:
    """Return a list of data stream descriptors."""
    try:
        raw = client.safe_get("/_data_stream/*", expand_wildcards="all,hidden") or {}
    except PermissionError:
        return []
    streams = raw.get("data_streams", [])
    if not include_system:
        streams = [s for s in streams if not s["name"].startswith(".")]
    return streams


def fetch_ilm_explain(client: ESClient) -> dict[str, dict]:
    """
    Return {index_name: {"managed": True, "policy": name}} for all ILM-managed indices.

    Tries _ilm/explain first (requires manage_ilm or view_index_metadata privilege).
    Falls back to reading index.lifecycle.name from _settings when that fails —
    which needs only monitor/view_index_metadata on the index level.
    """
    try:
        raw = client.safe_get("/*/_ilm/explain", only_managed="true",
                              expand_wildcards="all") or {}
    except PermissionError:
        raw = {}
    result = raw.get("indices", {})
    if result:
        return result

    # Fallback: derive managed status from index settings
    settings_raw = client.safe_get("/*/_settings/index.lifecycle.name",
                                   expand_wildcards="all") or {}
    fallback = {}
    for idx_name, idx_data in settings_raw.items():
        policy = (idx_data.get("settings", {})
                          .get("index", {})
                          .get("lifecycle", {})
                          .get("name", ""))
        if policy:
            fallback[idx_name] = {"managed": True, "policy": policy}
    return fallback


# ---------------------------------------------------------------------------
# Daily ingest per source
# ---------------------------------------------------------------------------

def fetch_daily_ingest_per_source(client: ESClient, rows: list[dict], days: int) -> None:
    """
    Populate avg_daily_bytes and avg_daily_source_bytes on each row in-place
    using a single msearch.

    avg_daily_bytes       — store estimate: avg bytes/doc × daily doc count
    avg_daily_source_bytes — sum of _size field per day (only when mapper-size
                             plugin is enabled on the index; otherwise stays 0)
    """
    today  = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=days)

    query_template = {
        "size": 0,
        "query": {"range": {"@timestamp": {
            "gte": cutoff.isoformat(),
            "lte": today.isoformat(),
        }}},
        "aggs": {"by_day": {
            "date_histogram": {
                "field": "@timestamp",
                "calendar_interval": "day",
                "min_doc_count": 1,
            },
            "aggs": {"source_bytes": {"sum": {"field": "_size"}}},
        }},
    }

    # Build msearch bodies — one request per row
    bodies = []
    for rec in rows:
        target = rec["name"] if rec["type"] == "Data Stream" else f"{rec['name']}*"
        bodies.append(({"index": target}, query_template))

    responses = client.msearch(bodies)

    for rec, resp in zip(rows, responses):
        rec["avg_daily_bytes"] = 0
        rec["avg_daily_source_bytes"] = 0
        if rec["doc_count"] == 0 or rec["primary_bytes"] == 0:
            continue
        buckets = (resp.get("aggregations") or {}).get("by_day", {}).get("buckets", [])
        if not buckets:
            continue

        avg_per_doc = rec["primary_bytes"] / rec["doc_count"]
        total_store  = sum(b["doc_count"] * avg_per_doc for b in buckets)
        total_source = sum((b.get("source_bytes") or {}).get("value") or 0 for b in buckets)

        rec["avg_daily_bytes"] = int(total_store / len(buckets))
        if total_source > 0:
            rec["avg_daily_source_bytes"] = int(total_source / len(buckets))


# ---------------------------------------------------------------------------
# Inventory assembly
# ---------------------------------------------------------------------------

def build_inventory(client: ESClient, include_system: bool, days: int = 30) -> tuple[list[dict], bool]:
    """
    Returns a list of data source records, each containing:
        name, type, policy, phases, index_count, primary_bytes, total_bytes, doc_count, avg_daily_bytes
    """
    sizes       = fetch_index_sizes(client)
    ds_list     = fetch_data_streams(client, include_system)
    ilm_explain = fetch_ilm_explain(client)

    # Track which indices are backing a data stream so we don't double-count them
    ds_backing_indices: set[str] = set()
    ds_map: dict[str, dict] = {}   # ds_name → descriptor

    for ds in ds_list:
        ds_name = ds["name"]
        backing = [idx["index_name"] for idx in ds.get("indices", [])]
        ds_backing_indices.update(backing)

        primary = sum(sizes.get(i, {}).get("primary_bytes", 0) for i in backing)
        total   = sum(sizes.get(i, {}).get("total_bytes",   0) for i in backing)
        docs    = sum(sizes.get(i, {}).get("doc_count",     0) for i in backing)

        # ILM policy: use the write index's explain entry, or the template field
        policy = ds.get("ilm_policy") or ""
        if not policy:
            write_idx = ds.get("indices", [{}])[-1].get("index_name", "")
            policy = ilm_explain.get(write_idx, {}).get("policy", "")

        ds_map[ds_name] = {
            "name":                    ds_name,
            "type":                    "Data Stream",
            "policy":                  policy,
            "phases":                  {},
            "index_count":             len(backing),
            "primary_bytes":           primary,
            "total_bytes":             total,
            "doc_count":               docs,
            "avg_daily_bytes":         0,
            "avg_daily_source_bytes":  0,
        }

    # Group non-data-stream ILM-managed indices by family
    family_map: dict[str, dict] = defaultdict(lambda: {
        "policy": "", "phases": {}, "index_count": 0,
        "primary_bytes": 0, "total_bytes": 0, "doc_count": 0,
        "avg_daily_bytes": 0, "avg_daily_source_bytes": 0, "type": "Index (ILM)",
    })

    for idx_name, explain in ilm_explain.items():
        if idx_name in ds_backing_indices:
            continue
        if not include_system and idx_name.startswith("."):
            continue
        family = index_family(idx_name)
        rec    = family_map[family]
        rec["policy"]        = explain.get("policy", "")
        rec["index_count"]  += 1
        rec["primary_bytes"] += sizes.get(idx_name, {}).get("primary_bytes", 0)
        rec["total_bytes"]   += sizes.get(idx_name, {}).get("total_bytes",   0)
        rec["doc_count"]     += sizes.get(idx_name, {}).get("doc_count",     0)

    # Unmanaged indices (not in any ILM policy, not a data stream backing index)
    managed_indices = set(ilm_explain.keys()) | ds_backing_indices
    unmanaged_map: dict[str, dict] = defaultdict(lambda: {
        "policy": "", "phases": {}, "index_count": 0,
        "primary_bytes": 0, "total_bytes": 0, "doc_count": 0,
        "avg_daily_bytes": 0, "avg_daily_source_bytes": 0, "type": "Index (unmanaged)",
    })

    for idx_name, sz in sizes.items():
        if idx_name in managed_indices:
            continue
        if not include_system and idx_name.startswith("."):
            continue
        family = index_family(idx_name)
        rec    = unmanaged_map[family]
        rec["index_count"]  += 1
        rec["primary_bytes"] += sz.get("primary_bytes", 0)
        rec["total_bytes"]   += sz.get("total_bytes",   0)
        rec["doc_count"]     += sz.get("doc_count",     0)

    # Collect all unique policy names and fetch their phase definitions
    all_policies: set[str] = set()
    for rec in ds_map.values():
        if rec["policy"]:
            all_policies.add(rec["policy"])
    for rec in family_map.values():
        if rec["policy"]:
            all_policies.add(rec["policy"])

    policy_phases, phases_available = fetch_ilm_policies(client, all_policies)

    # Attach phases to records
    for rec in ds_map.values():
        rec["phases"] = policy_phases.get(rec["policy"], {})
    for rec in family_map.values():
        rec["phases"] = policy_phases.get(rec["policy"], {})

    # Merge everything and sort by type then name
    rows = []
    rows += [{"name": k, **v} for k, v in sorted(ds_map.items())]
    rows += [{"name": k, **v} for k, v in sorted(family_map.items())]
    rows += [{"name": k, **v} for k, v in sorted(unmanaged_map.items())]

    fetch_daily_ingest_per_source(client, rows, days)

    return rows, phases_available


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

W_NAME     = 42
W_TYPE     = 15
W_POLICY   = 26
W_ROLLOVER = 18
W_AGE      = 9
W_IDX      = 4
W_SIZE     = 11
W_DAILY    = 11
W_SOURCE   = 11


def _row_fields(rec: dict) -> tuple:
    phases   = rec["phases"]
    hot      = phases.get("hot", {}).get("actions", {})
    rollover = format_rollover(hot)
    warm     = format_phase_age(phases, "warm")
    cold     = format_phase_age(phases, "cold")
    frozen   = format_phase_age(phases, "frozen")
    delete   = format_phase_age(phases, "delete")
    policy   = rec["policy"] or "—"
    return rollover, warm, cold, frozen, delete, policy


def print_inventory(rows: list[dict], cluster_name: str, es_version: str,
                    phases_available: bool = True, days: int = 30) -> None:
    inner = (2 + W_NAME + 2 + W_TYPE + 2 + W_POLICY + 2 + W_ROLLOVER
             + 2 + W_AGE + 2 + W_AGE + 2 + W_AGE + 2 + W_AGE + 2 + W_IDX + 2 + W_SIZE + 1)

    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print(f"║  Elasticsearch ILM Inventory                                 ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  Cluster : {cluster_name:<51}║")
    print(f"║  Version : {es_version:<51}║")
    print(f"║  Sources : {len(rows):<51}║")
    print(f"║  Avg/day : {f'{days}-day rolling window':<51}║")
    print("╚══════════════════════════════════════════════════════════════╝")

    if not rows:
        print("\n  No data sources found.\n")
        return

    sep = "─"
    print()
    hdr = (f"  {'Data Source':<{W_NAME}}  {'Type':<{W_TYPE}}  {'ILM Policy':<{W_POLICY}}"
           f"  {'Rollover':<{W_ROLLOVER}}  {'Warm':>{W_AGE}}  {'Cold':>{W_AGE}}"
           f"  {'Frozen':>{W_AGE}}  {'Delete':>{W_AGE}}  {'Idx':>{W_IDX}}"
           f"  {'Avg/day':>{W_DAILY}}  {'_size/day':>{W_SOURCE}}  {'Primary':>{W_SIZE}}")
    print(hdr)
    print("  " + sep * (len(hdr) - 2))

    last_type = None
    for rec in rows:
        if rec["type"] != last_type:
            if last_type is not None:
                print()
            last_type = rec["type"]

        rollover, warm, cold, frozen, delete, policy = _row_fields(rec)
        daily  = human_bytes(rec["avg_daily_bytes"])        if rec["avg_daily_bytes"]        else "—"
        source = human_bytes(rec["avg_daily_source_bytes"]) if rec["avg_daily_source_bytes"] else "—"

        name = rec["name"]
        if len(name) > W_NAME:
            name = name[:W_NAME - 1] + "…"
        policy_col = policy if len(policy) <= W_POLICY else policy[:W_POLICY - 1] + "…"

        print(
            f"  {name:<{W_NAME}}  {rec['type']:<{W_TYPE}}  {policy_col:<{W_POLICY}}"
            f"  {rollover:<{W_ROLLOVER}}  {warm:>{W_AGE}}  {cold:>{W_AGE}}"
            f"  {frozen:>{W_AGE}}  {delete:>{W_AGE}}  {rec['index_count']:>{W_IDX}}"
            f"  {daily:>{W_DAILY}}  {source:>{W_SOURCE}}  {human_bytes(rec['primary_bytes']):>{W_SIZE}}"
        )

    if not phases_available:
        print("  Note: phase details (Rollover/Warm/Cold/Frozen/Delete) are unavailable.")
        print("        The credentials need the 'read_ilm' cluster privilege to fetch policy definitions.\n")

    print()


def write_csv(path: str, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "data_source", "type", "ilm_policy",
            "rollover", "warm", "cold", "frozen", "delete",
            "index_count", "avg_daily_bytes", "avg_daily_source_bytes",
            "primary_bytes", "total_bytes", "doc_count",
        ])
        for rec in rows:
            rollover, warm, cold, frozen, delete, policy = _row_fields(rec)
            writer.writerow([
                rec["name"],
                rec["type"],
                rec["policy"] or "",
                rollover if rollover != "—" else "",
                warm     if warm     != "—" else "",
                cold     if cold     != "—" else "",
                frozen   if frozen   != "—" else "",
                delete   if delete   != "—" else "",
                rec["index_count"],
                rec["avg_daily_bytes"],
                rec["avg_daily_source_bytes"],
                rec["primary_bytes"],
                rec["total_bytes"],
                rec["doc_count"],
            ])
    print(f"  CSV written to: {path}\n")


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

    info         = client.get("/")
    cluster_name = info.get("cluster_name", "unknown")
    es_version   = info.get("version", {}).get("number", "unknown")

    rows, phases_available = build_inventory(client, args.include_system, args.days)

    print_inventory(rows, cluster_name, es_version, phases_available, args.days)

    if args.csv:
        write_csv(args.csv, rows)


if __name__ == "__main__":
    main()
