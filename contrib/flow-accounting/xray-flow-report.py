#!/usr/bin/env python3

import argparse
import csv
import glob
import gzip
import ipaddress
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone


def parse_time(value):
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def open_log(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", errors="replace")
    return open(path, "rt", errors="replace")


def human_bytes(value):
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    number = float(value)
    for unit in units:
        if abs(number) < 1024 or unit == units[-1]:
            return f"{number:.2f} {unit}"
        number /= 1024


def group_key(record, group_by):
    if group_by == "source-site":
        return record.get("source_ip", ""), record.get("site", "")
    if group_by == "source":
        return (record.get("source_ip", ""),)
    if group_by == "site":
        return (record.get("site", ""),)
    started = parse_time(record.get("started_at"))
    return ((started.date().isoformat() if started else "unknown"),)


def key_names(group_by):
    if group_by == "source-site":
        return ("source_ip", "site")
    return (group_by,)


def matches_source(candidate, requested):
    if not requested:
        return True
    try:
        return ipaddress.ip_address(candidate) in ipaddress.ip_network(requested, strict=False)
    except ValueError:
        return candidate == requested


def load_records(patterns, since, until, source, site):
    for pattern in patterns:
        for path in sorted(glob.glob(pattern)):
            with open_log(path) as stream:
                for line in stream:
                    try:
                        record = json.loads(line)
                        started = parse_time(record.get("started_at"))
                    except (ValueError, TypeError, json.JSONDecodeError):
                        continue
                    if started is None or started < since or (until and started >= until):
                        continue
                    if not matches_source(record.get("source_ip", ""), source):
                        continue
                    if site and site.lower() not in record.get("site", "").lower():
                        continue
                    yield record


def aggregate(records, group_by):
    grouped = defaultdict(lambda: {
        "requests": 0,
        "uplink_bytes": 0,
        "downlink_bytes": 0,
        "first_seen": None,
        "last_seen": None,
    })
    for record in records:
        key = group_key(record, group_by)
        item = grouped[key]
        item["requests"] += 1
        item["uplink_bytes"] += int(record.get("uplink_bytes", 0))
        item["downlink_bytes"] += int(record.get("downlink_bytes", 0))
        started = record.get("started_at")
        ended = record.get("ended_at")
        if started and (item["first_seen"] is None or started < item["first_seen"]):
            item["first_seen"] = started
        if ended and (item["last_seen"] is None or ended > item["last_seen"]):
            item["last_seen"] = ended
    names = key_names(group_by)
    rows = []
    for key, item in grouped.items():
        row = dict(zip(names, key))
        row.update(item)
        row["total_bytes"] = item["uplink_bytes"] + item["downlink_bytes"]
        rows.append(row)
    rows.sort(key=lambda row: row["total_bytes"], reverse=True)
    return rows


def write_json(rows):
    json.dump(rows, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


def write_csv(rows):
    if not rows:
        return
    writer = csv.DictWriter(sys.stdout, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)


def write_table(rows, names):
    headers = list(names) + ["requests", "uplink", "downlink", "total"]
    formatted = []
    for row in rows:
        formatted.append([*(str(row[name]) for name in names), str(row["requests"]),
                          human_bytes(row["uplink_bytes"]), human_bytes(row["downlink_bytes"]),
                          human_bytes(row["total_bytes"])])
    widths = [len(header) for header in headers]
    for row in formatted:
        widths = [max(width, len(value)) for width, value in zip(widths, row)]
    print("  ".join(header.ljust(width) for header, width in zip(headers, widths)))
    print("  ".join("-" * width for width in widths))
    for row in formatted:
        print("  ".join(value.ljust(width) for value, width in zip(row, widths)))


def main():
    parser = argparse.ArgumentParser(description="Summarize Xray logical flows by source IP and destination site.")
    parser.add_argument("--log-glob", action="append", default=[])
    parser.add_argument("--days", type=float, default=30)
    parser.add_argument("--since")
    parser.add_argument("--until")
    parser.add_argument("--source", help="Exact IP address or CIDR filter.")
    parser.add_argument("--site", help="Case-insensitive destination substring filter.")
    parser.add_argument("--group-by", choices=("source-site", "source", "site", "day"), default="source-site")
    parser.add_argument("--format", choices=("table", "json", "csv"), default="table")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    since = parse_time(args.since) if args.since else now - timedelta(days=args.days)
    until = parse_time(args.until)
    patterns = args.log_glob or ["/var/log/xray/flow.jsonl*"]
    rows = aggregate(load_records(patterns, since, until, args.source, args.site), args.group_by)
    if args.limit > 0:
        rows = rows[:args.limit]
    if args.format == "json":
        write_json(rows)
    elif args.format == "csv":
        write_csv(rows)
    else:
        write_table(rows, key_names(args.group_by))


if __name__ == "__main__":
    main()
