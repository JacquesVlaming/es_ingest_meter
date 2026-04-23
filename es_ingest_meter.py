#!/usr/bin/env python3
"""
es_ingest_meter.py

Measures daily data ingest volume for an Elasticsearch cluster.
Groups time-based indices by date, computes rolling averages, and projects
storage requirements — useful for right-sizing Elastic deployments.

Supports:
  - Basic auth, API key, and unauthenticated clusters
  - Standard indices and data stream backing indices (.ds-*)
  - Configurable index pattern and rolling window
  - CSV export for spreadsheet analysis
"""

import argparse
import csv
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Optional

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    sys.exit("Missing dependency: pip install requests")

import es_logger


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Measure daily ingest volume for Elasticsearch cluster sizing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local cluster, all indices, 30-day window
  python es_ingest_meter.py

  # Cloud / Basic auth
  python es_ingest_meter.py --host https://my-cluster.es.io:9243 -u elastic -p secret

  # API key auth, only logs data streams, 14-day window
  python es_ingest_meter.py --host https://... --api-key BASE64KEY --pattern 'logs-*' --days 14

  # Export to CSV
  python es_ingest_meter.py --csv report.csv
""",
    )
    p.add_argument("--host", default="http://localhost:9200",
                   help="Elasticsearch base URL (default: http://localhost:9200)")
    p.add_argument("-u", "--username", help="Username for basic auth")
    p.add_argument("-p", "--password", help="Password for basic auth")
    p.add_argument("--api-key", metavar="BASE64",
                   help="API key value (base64-encoded id:key string)")
    p.add_argument("--pattern", default="*", metavar="INDEX_PATTERN",
                   help="Index pattern to measure (default: *)")
    p.add_argument("--days", type=int, default=30,
                   help="Rolling window in days (default: 30)")
    p.add_argument("--include-system", action="store_true",
                   help="Include system indices (starting with '.')")
    p.add_argument("--csv", metavar="FILE",
                   help="Export daily breakdown to CSV file")
    p.add_argument("--insecure", action="store_true",
                   help="Disable TLS certificate verification")
    p.add_argument("--timestamp-field", default="@timestamp", metavar="FIELD",
                   help="Date field used for per-day bucketing (default: @timestamp)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Elasticsearch client (thin wrapper around requests)
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
            resp = self.session.get(url, params=params or None, verify=self.verify, timeout=30)
        except requests.exceptions.ConnectionError:
            sys.exit(f"Connection refused — is Elasticsearch running at {self.host}?")
        except requests.exceptions.Timeout:
            sys.exit("Request timed out. Try again or increase the timeout.")

        if resp.status_code == 401:
            sys.exit("Authentication failed. Check --username/--password or --api-key.")
        if resp.status_code == 403:
            sys.exit("Authorization failed. The credentials lack the required privileges.")
        resp.raise_for_status()
        return resp.json()

    def safe_get(self, path: str, **params) -> Optional[dict]:
        """Like get() but returns None on any error instead of exiting."""
        url = f"{self.host}{path}"
        try:
            resp = self.session.get(url, params=params or None, verify=self.verify, timeout=30)
            if not resp.ok:
                return None
            return resp.json()
        except Exception:
            return None

    def search(self, index: str, body: dict) -> dict:
        url = f"{self.host}/{index}/_search"
        try:
            resp = self.session.post(url, json=body, verify=self.verify, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return {}

    def get_creation_dates(self, pattern: str) -> dict[str, date]:
        """Return {index_name: creation_date} for all indices matching pattern."""
        url = f"{self.host}/{pattern}/_cat/indices"
        try:
            resp = self.session.get(
                url, params={"h": "index,creation.date", "format": "json"},
                verify=self.verify, timeout=30,
            )
            resp.raise_for_status()
            return {
                row["index"]: date.fromtimestamp(int(row["creation.date"]) / 1000)
                for row in resp.json()
                if row.get("creation.date")
            }
        except Exception:
            return {}


# ---------------------------------------------------------------------------
# Date extraction from index names
# ---------------------------------------------------------------------------

# Ordered from most to least specific. Data stream backing indices look like:
#   .ds-logs-myapp-default-2024.03.27-000001
# ILM-rolled indices: logs-myapp-2024.03.27-000001
# Filebeat/Metricbeat: filebeat-8.0.0-2024.03.27
_DATE_PATTERNS = [
    (r"(\d{4}[.\-]\d{2}[.\-]\d{2})", "%Y-%m-%d"),   # YYYY.MM.DD / YYYY-MM-DD
    (r"(\d{8})",                        "%Y%m%d"),    # YYYYMMDD
    (r"(\d{4}[.\-]\d{2})(?!\d)",        "%Y-%m"),    # YYYY.MM / YYYY-MM (month only)
]


def extract_date(index_name: str) -> Optional[date]:
    """Return the date embedded in an index name, or None."""
    # Strip data stream prefix so the embedded date is found correctly
    name = re.sub(r"^\.ds-", "", index_name)

    for pattern, fmt in _DATE_PATTERNS:
        m = re.search(pattern, name)
        if m:
            raw = m.group(1).replace(".", "-").replace("_", "-")
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
    return None


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]


def human_bytes(n: float) -> str:
    for unit in _UNITS:
        if abs(n) < 1024:
            return f"{n:,.2f} {unit}"
        n /= 1024
    return f"{n:,.2f} PB"


def bar(value: float, max_value: float, width: int = 20) -> str:
    if max_value == 0:
        return " " * width
    filled = int(round(value / max_value * width))
    return "█" * filled + "░" * (width - filled)


def _format_rollover(hot_actions: dict) -> str:
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
        parts.append(f"{docs:,} docs")
    return " or ".join(parts) if parts else "—"


def _format_phase_age(phases: dict, phase_name: str) -> str:
    phase = phases.get(phase_name)
    if not phase:
        return "—"
    age = phase.get("min_age", "")
    return "0d" if not age or age in ("0ms", "0s") else age


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def collect_stats(client: ESClient, pattern: str, include_system: bool,
                  window_days: int, timestamp_field: str):
    """
    Fetch index stats and bucket them by date.

    For each index runs a date histogram with two sizing strategies in parallel:

      1. Store estimate  – avg_doc_size (primary_bytes / doc_count) × docs_that_day.
                          Always available; reflects on-disk primary shard bytes.
      2. _size sum       – sum of the mapper-size _size field per bucket.
                          Only populated when the mapper-size plugin is installed
                          AND the index mapping has "_size": {"enabled": true}.

    Both are accumulated independently so the report can show them side-by-side.
    Indices with no timestamp data in the window are classified as static and
    excluded from daily averages — reported separately.

    Returns:
        daily_primary   – {date: bytes} store-estimate primary bytes per day
        daily_total     – {date: bytes} store-estimate total (primary + replica) per day
        daily_docs      – {date: int}   document count per day
        daily_source    – {date: bytes} _size sum per day (empty if mapper not available)
        static_indices  – list of (name, primary_bytes, total_bytes, doc_count, created)
        skipped_count   – number of indices excluded (system indices)
    """
    raw = client.get(f"/{pattern}/_stats", level="indices")

    today  = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=window_days)

    daily_primary: dict[date, int] = defaultdict(int)
    daily_total:   dict[date, int] = defaultdict(int)
    daily_docs:    dict[date, int] = defaultdict(int)
    daily_source:  dict[date, int] = defaultdict(int)
    static_indices: list           = []
    skipped_count:  int            = 0

    index_stats: list = []

    for name, stats in raw.get("indices", {}).items():
        if name.startswith(".") and not name.startswith(".ds-") and not include_system:
            skipped_count += 1
            continue

        primaries     = stats["primaries"]
        primary_bytes = primaries["store"]["size_in_bytes"]
        total_bytes   = stats["total"]["store"]["size_in_bytes"]
        doc_count     = primaries["docs"]["count"]
        index_stats.append((name, primary_bytes, total_bytes, doc_count))

    creation_dates = client.get_creation_dates(pattern)

    for name, primary_bytes, total_bytes, doc_count in index_stats:
        if doc_count == 0:
            continue

        avg_primary_per_doc = primary_bytes / doc_count
        avg_total_per_doc   = total_bytes   / doc_count

        # Query date histogram; always include _size sum sub-agg
        resp = client.search(name, {
            "size": 0,
            "query": {
                "range": {
                    timestamp_field: {
                        "gte": cutoff.isoformat(),
                        "lte": today.isoformat(),
                    }
                }
            },
            "aggs": {
                "by_day": {
                    "date_histogram": {
                        "field": timestamp_field,
                        "calendar_interval": "day",
                        "min_doc_count": 1,
                    },
                    "aggs": {
                        "source_bytes": {
                            "sum": {"field": "_size"}
                        }
                    },
                }
            },
        })

        buckets = (resp.get("aggregations") or {}).get("by_day", {}).get("buckets", [])

        if buckets:
            for bucket in buckets:
                d             = date.fromtimestamp(bucket["key"] / 1000)
                docs_that_day = bucket["doc_count"]
                source_sum    = (bucket.get("source_bytes") or {}).get("value") or 0

                # Store estimate — always
                daily_primary[d] += int(avg_primary_per_doc * docs_that_day)
                daily_total[d]   += int(avg_total_per_doc   * docs_that_day)
                daily_docs[d]    += docs_that_day

                # _size sum — only when mapper is enabled on this index
                if source_sum > 0:
                    daily_source[d] += int(source_sum)
        else:
            # No timestamp data in window — classify as static, exclude from averages
            created = creation_dates.get(name, today)
            static_indices.append((name, primary_bytes, total_bytes, doc_count, created))

    return daily_primary, daily_total, daily_docs, daily_source, static_indices, skipped_count


def fetch_ilm_info(client: ESClient, pattern: str) -> tuple[dict, int]:
    """
    Fetch ILM policy assignments and phase definitions for indices matching pattern.

    Returns:
        policies  – {policy_name: {"index_count": int, "phases": {phase: {min_age, actions}}}}
        unmanaged – count of indices not under any ILM policy
    """
    explain = client.safe_get(f"/{pattern}/_ilm/explain")
    if explain is None:
        return {}, 0

    policy_counts: dict[str, int] = defaultdict(int)
    unmanaged = 0

    for idx_info in explain.get("indices", {}).values():
        if not idx_info.get("managed"):
            unmanaged += 1
            continue
        policy_counts[idx_info.get("policy", "unknown")] += 1

    if not policy_counts:
        return {}, unmanaged

    policies_raw = client.safe_get(f"/_ilm/policy/{','.join(policy_counts)}") or {}

    result = {}
    for name, count in policy_counts.items():
        phases = policies_raw.get(name, {}).get("policy", {}).get("phases", {})
        result[name] = {"index_count": count, "phases": phases}

    return result, unmanaged


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(
    cluster_name: str,
    es_version: str,
    pattern: str,
    window_days: int,
    daily_primary: dict,
    daily_total: dict,
    daily_docs: dict,
    daily_source: dict,
    static_indices: list,
    skipped_count: int,
    timestamp_field: str = "@timestamp",
) -> None:
    dates = sorted(daily_primary.keys())
    n = len(dates)
    size_available = bool(daily_source)

    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print(f"║  Elasticsearch Ingest Meter                                  ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  Cluster : {cluster_name:<51}║")
    print(f"║  Version : {es_version:<51}║")
    print(f"║  Pattern : {pattern:<51}║")
    print(f"║  Window  : {f'{window_days} days requested, {n} days with data':<51}║")
    print("╚══════════════════════════════════════════════════════════════╝")

    if not dates:
        print("\n  No dated indices found in the specified window.\n")
        print("  Tip: Use --pattern to target a specific index pattern, e.g. 'logs-*'")
        print("       or reduce --days if your indices are newer.\n")
        return

    max_primary = max(daily_primary.values())

    print()
    print(f"  {'Date':<12}  {'Store Est.':>12}  {'_size (src)':>12}  {'% src':>6}  {'With Replicas':>14}  {'Documents':>14}  Relative")
    print("  " + "─" * 97)

    for d in dates:
        pb  = daily_primary[d]
        tb  = daily_total[d]
        dc  = daily_docs[d]
        src = daily_source.get(d)
        b   = bar(pb, max_primary, width=16)
        src_col = human_bytes(src) if src else "—"
        pct_col = f"{src / pb * 100:.1f}%" if src and pb else "—"
        print(f"  {str(d):<12}  {human_bytes(pb):>12}  {src_col:>12}  {pct_col:>6}  {human_bytes(tb):>14}  {dc:>14,}  {b}")

    # Totals
    total_primary = sum(daily_primary.values())
    total_total   = sum(daily_total.values())
    total_docs    = sum(daily_docs.values())
    total_source  = sum(daily_source.values()) if size_available else None

    total_pct_col = f"{total_source / total_primary * 100:.1f}%" if total_source and total_primary else "—"
    print("  " + "─" * 97)
    src_total_col = human_bytes(total_source) if total_source else ("_size mapper not available" if not size_available else "—")
    print(f"  {'TOTAL':<12}  {human_bytes(total_primary):>12}  {src_total_col:>12}  {total_pct_col:>6}  {human_bytes(total_total):>14}  {total_docs:>14,}")

    # Averages and projections
    avg_primary = total_primary / n
    avg_total   = total_total   / n
    avg_docs    = total_docs    / n
    avg_source  = (total_source / n) if total_source else None

    # Replica factor (useful to surface for sizing)
    replica_factor = (total_total / total_primary) if total_primary else 1.0

    print()
    print("  ┌─ Rolling Averages (" + f"{n} days" + ") " + "─" * 38 + "┐")
    print(f"  │  Avg store est. / day : {human_bytes(avg_primary):<37}│")
    if avg_source:
        print(f"  │  Avg _size / day      : {human_bytes(avg_source):<37}│")
    else:
        print(f"  │  Avg _size / day      : {'_size mapper not available':<37}│")
    print(f"  │  Avg total / day      : {human_bytes(avg_total):<37}│")
    print(f"  │  Avg documents / day  : {avg_docs:>,.0f}{'':<37}│"[:70] + "│")
    print(f"  │  Observed replica ×   : {replica_factor:.2f}x{'':<36}│"[:70] + "│")
    print("  ├─ Projections (store estimate) ─" + "─" * 35 + "┤")
    print(f"  │  30-day primary       : {human_bytes(avg_primary * 30):<37}│")
    print(f"  │  30-day total         : {human_bytes(avg_total * 30):<37}│")
    print(f"  │  90-day primary       : {human_bytes(avg_primary * 90):<37}│")
    print(f"  │  365-day primary      : {human_bytes(avg_primary * 365):<37}│")
    print("  └" + "─" * 67 + "┘")

    # Static indices (no timestamp data in window)
    if static_indices:
        today = datetime.now(timezone.utc).date()
        static_primary = sum(r[1] for r in static_indices)
        static_total   = sum(r[2] for r in static_indices)
        static_docs    = sum(r[3] for r in static_indices)
        print()
        print(f"  ┌─ Static Indices ({len(static_indices)}) ─── excluded from daily averages ──────────────┐")
        print(f"  │  {'Index':<40} {'Primary':>10}  {'Age':>6}  │")
        print(f"  │  {'─'*40} {'─'*10}  {'─'*6}  │")
        for name, pb, tb, dc, created in sorted(static_indices, key=lambda r: r[1], reverse=True):
            age_days = (today - created).days
            age      = f"{age_days}d" if age_days < 365 else f"{age_days//365}y{age_days%365//30}m"
            print(f"  │  {name:<40} {human_bytes(pb):>10}  {age:>6}  │")
        print(f"  │  {'─'*40} {'─'*10}  {'─'*6}  │")
        print(f"  │  {'TOTAL':<40} {human_bytes(static_primary):>10}  {'':>6}  │")
        print(f"  └{'─'*57}┘")
        print(f"  (Not included in averages — no '{timestamp_field}' data in the {window_days}-day window.)")

    if skipped_count:
        print(f"\n  {skipped_count} indices skipped (system or outside the {window_days}-day window).")

    print()


def print_ilm_section(ilm_policies: dict, unmanaged: int) -> None:
    if not ilm_policies and not unmanaged:
        return

    W_NAME, W_IDX, W_ROLLOVER, W_AGE = 24, 4, 18, 6
    # inner = leading_pad + name + sep + idx + pad + rollover + pad + warm + pad + cold + pad + delete + trailing
    inner = 2 + W_NAME + 1 + W_IDX + 2 + W_ROLLOVER + 2 + W_AGE + 2 + W_AGE + 2 + W_AGE + 1  # 76

    total_managed = sum(p["index_count"] for p in ilm_policies.values())
    note = f"{total_managed} managed, {unmanaged} unmanaged" if unmanaged else f"{total_managed} managed"
    title = f"─ ILM Policies ({note}) "
    fill  = "─" * max(0, inner - len(title))

    print()
    print(f"  ┌{title}{fill}┐")
    print(f"  │  {'Policy':<{W_NAME}} {'Idx':>{W_IDX}}  {'Rollover':<{W_ROLLOVER}}  {'Warm':>{W_AGE}}  {'Cold':>{W_AGE}}  {'Delete':>{W_AGE}} │")
    print(f"  │  {'─'*W_NAME} {'─'*W_IDX}  {'─'*W_ROLLOVER}  {'─'*W_AGE}  {'─'*W_AGE}  {'─'*W_AGE} │")

    for policy_name, info in sorted(ilm_policies.items()):
        phases      = info["phases"]
        count       = info["index_count"]
        rollover    = _format_rollover(phases.get("hot", {}).get("actions", {}))
        warm        = _format_phase_age(phases, "warm")
        cold        = _format_phase_age(phases, "cold")
        delete      = _format_phase_age(phases, "delete")
        name_col    = policy_name if len(policy_name) <= W_NAME else policy_name[:W_NAME - 1] + "…"
        print(f"  │  {name_col:<{W_NAME}} {count:>{W_IDX}}  {rollover:<{W_ROLLOVER}}  {warm:>{W_AGE}}  {cold:>{W_AGE}}  {delete:>{W_AGE}} │")

    if unmanaged:
        print(f"  │  {'(unmanaged)':<{W_NAME}} {unmanaged:>{W_IDX}}  {'—':<{W_ROLLOVER}}  {'—':>{W_AGE}}  {'—':>{W_AGE}}  {'—':>{W_AGE}} │")

    print(f"  └{'─' * inner}┘")


def write_csv(path: str, daily_primary: dict, daily_total: dict, daily_docs: dict,
              daily_source: dict) -> None:
    dates = sorted(daily_primary.keys())
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "store_est_bytes", "size_source_bytes", "total_bytes", "documents"])
        for d in dates:
            writer.writerow([d, daily_primary[d], daily_source.get(d, ""), daily_total[d], daily_docs[d]])
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

    log = es_logger.setup(
        "es-ingest-meter",
        host=args.host,
        username=args.username,
        password=args.password,
        api_key=args.api_key,
        verify=not args.insecure,
    )

    # Cluster info
    info = client.get("/")
    cluster_name = info.get("cluster_name", "unknown")
    es_version   = info.get("version", {}).get("number", "unknown")

    log.info("Meter run started", extra={
        "cluster": cluster_name,
        "es_version": es_version,
        "pattern": args.pattern,
        "window_days": args.days,
    })

    # Collect stats
    daily_primary, daily_total, daily_docs, daily_source, static_indices, skipped = collect_stats(
        client, args.pattern, args.include_system, args.days, args.timestamp_field
    )

    # ILM policies
    ilm_policies, ilm_unmanaged = fetch_ilm_info(client, args.pattern)

    # Print report
    print_report(
        cluster_name=cluster_name,
        es_version=es_version,
        pattern=args.pattern,
        window_days=args.days,
        daily_primary=daily_primary,
        daily_total=daily_total,
        daily_docs=daily_docs,
        daily_source=daily_source,
        static_indices=static_indices,
        skipped_count=skipped,
        timestamp_field=args.timestamp_field,
    )

    print_ilm_section(ilm_policies, ilm_unmanaged)

    dates = sorted(daily_primary.keys())
    n     = len(dates)
    if n > 0:
        total_primary = sum(daily_primary.values())
        total_total   = sum(daily_total.values())
        total_docs    = sum(daily_docs.values())
        log.info("Meter run completed", extra={
            "cluster": cluster_name,
            "pattern": args.pattern,
            "days_with_data": n,
            "avg_primary_bytes_per_day": total_primary // n,
            "avg_total_bytes_per_day":   total_total   // n,
            "avg_docs_per_day":          total_docs    // n,
            "projected_30d_primary_bytes": (total_primary // n) * 30,
            "static_indices": len(static_indices),
            "skipped_indices": skipped,
            "size_field_available": bool(daily_source),
        })
    else:
        log.warning("Meter run completed with no dated indices found", extra={
            "cluster": cluster_name,
            "pattern": args.pattern,
            "window_days": args.days,
        })

    # Optional CSV export
    if args.csv:
        write_csv(args.csv, daily_primary, daily_total, daily_docs, daily_source)


if __name__ == "__main__":
    main()
