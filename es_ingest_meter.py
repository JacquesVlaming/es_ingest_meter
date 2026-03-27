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


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def collect_stats(client: ESClient, pattern: str, include_system: bool, window_days: int):
    """
    Fetch index stats and bucket them by date.

    Returns:
        daily_primary  – {date: bytes} primary shard bytes per day
        daily_total    – {date: bytes} total (primary + replica) bytes per day
        daily_docs     – {date: int}   document count per day
        undated        – list of (name, primary_bytes, total_bytes, docs) tuples
        skipped_count  – number of indices excluded (system or outside window)
    """
    raw = client.get(f"/{pattern}/_stats", level="indices")

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=window_days)

    daily_primary: dict[date, int] = defaultdict(int)
    daily_total:   dict[date, int] = defaultdict(int)
    daily_docs:    dict[date, int] = defaultdict(int)
    undated:       list            = []
    skipped_count: int             = 0

    for name, stats in raw.get("indices", {}).items():
        # Optionally skip system indices (but allow .ds- data stream backing indices)
        if name.startswith(".") and not name.startswith(".ds-") and not include_system:
            skipped_count += 1
            continue

        primaries     = stats["primaries"]
        primary_bytes = primaries["store"]["size_in_bytes"]
        total_bytes   = stats["total"]["store"]["size_in_bytes"]
        doc_count     = primaries["docs"]["count"]

        idx_date = extract_date(name)

        if idx_date is None:
            undated.append((name, primary_bytes, total_bytes, doc_count))
        elif idx_date < cutoff:
            skipped_count += 1          # outside the window
        else:
            daily_primary[idx_date] += primary_bytes
            daily_total[idx_date]   += total_bytes
            daily_docs[idx_date]    += doc_count

    return daily_primary, daily_total, daily_docs, undated, skipped_count


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
    undated: list,
    skipped_count: int,
) -> None:
    dates = sorted(daily_primary.keys())
    n = len(dates)

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
    print(f"  {'Date':<12}  {'Primary':>12}  {'With Replicas':>14}  {'Documents':>14}  Relative")
    print("  " + "─" * 72)

    for d in dates:
        pb = daily_primary[d]
        tb = daily_total[d]
        dc = daily_docs[d]
        b  = bar(pb, max_primary, width=18)
        print(f"  {str(d):<12}  {human_bytes(pb):>12}  {human_bytes(tb):>14}  {dc:>14,}  {b}")

    # Totals
    total_primary = sum(daily_primary.values())
    total_total   = sum(daily_total.values())
    total_docs    = sum(daily_docs.values())

    print("  " + "─" * 72)
    print(f"  {'TOTAL':<12}  {human_bytes(total_primary):>12}  {human_bytes(total_total):>14}  {total_docs:>14,}")

    # Averages and projections
    avg_primary = total_primary / n
    avg_total   = total_total   / n
    avg_docs    = total_docs    / n

    # Replica factor (useful to surface for sizing)
    replica_factor = (total_total / total_primary) if total_primary else 1.0

    print()
    print("  ┌─ Rolling Averages (" + f"{n} days" + ") " + "─" * 38 + "┐")
    print(f"  │  Avg primary / day   : {human_bytes(avg_primary):<38}│")
    print(f"  │  Avg total / day     : {human_bytes(avg_total):<38}│")
    print(f"  │  Avg documents / day : {avg_docs:>,.0f}{'':<38}│"[:69] + "│")
    print(f"  │  Observed replica ×  : {replica_factor:.2f}x{'':<36}│"[:69] + "│")
    print("  ├─ Projections ─" + "─" * 52 + "┤")
    print(f"  │  30-day primary      : {human_bytes(avg_primary * 30):<38}│")
    print(f"  │  30-day total        : {human_bytes(avg_total * 30):<38}│")
    print(f"  │  90-day primary      : {human_bytes(avg_primary * 90):<38}│")
    print(f"  │  365-day primary     : {human_bytes(avg_primary * 365):<38}│")
    print("  └" + "─" * 67 + "┘")

    # Undated indices summary
    if undated:
        undated_primary = sum(r[1] for r in undated)
        undated_total   = sum(r[2] for r in undated)
        undated_docs    = sum(r[3] for r in undated)
        print()
        print(f"  ⚠  {len(undated)} undated indices excluded from averages")
        print(f"     Primary: {human_bytes(undated_primary)}  |  "
              f"Total: {human_bytes(undated_total)}  |  Docs: {undated_docs:,}")
        print("     (These indices have no recognisable date in their name.)")

    if skipped_count:
        print(f"\n  {skipped_count} indices skipped (system or outside the {window_days}-day window).")

    print()


def write_csv(path: str, daily_primary: dict, daily_total: dict, daily_docs: dict) -> None:
    dates = sorted(daily_primary.keys())
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "primary_bytes", "total_bytes", "documents"])
        for d in dates:
            writer.writerow([d, daily_primary[d], daily_total[d], daily_docs[d]])
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
    daily_primary, daily_total, daily_docs, undated, skipped = collect_stats(
        client, args.pattern, args.include_system, args.days
    )

    # Print report
    print_report(
        cluster_name=cluster_name,
        es_version=es_version,
        pattern=args.pattern,
        window_days=args.days,
        daily_primary=daily_primary,
        daily_total=daily_total,
        daily_docs=daily_docs,
        undated=undated,
        skipped_count=skipped,
    )

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
            "undated_indices": len(undated),
            "skipped_indices": skipped,
        })
    else:
        log.warning("Meter run completed with no dated indices found", extra={
            "cluster": cluster_name,
            "pattern": args.pattern,
            "window_days": args.days,
        })

    # Optional CSV export
    if args.csv:
        write_csv(args.csv, daily_primary, daily_total, daily_docs)


if __name__ == "__main__":
    main()
