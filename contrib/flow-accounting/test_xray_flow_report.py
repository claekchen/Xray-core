#!/usr/bin/env python3

import importlib.util
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone


SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "xray-flow-report.py")
SPEC = importlib.util.spec_from_file_location("xray_flow_report", SCRIPT_PATH)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


class FlowReportTest(unittest.TestCase):
    def test_segments_are_summed_and_requests_are_deduplicated(self):
        records = [
            {
                "flow_id": "flow-1",
                "segment": 0,
                "interval_started_at": "2026-08-24T10:00:00Z",
                "started_at": "2026-08-24T10:00:00Z",
                "ended_at": "2026-08-24T10:05:00Z",
                "source_ip": "192.0.2.10",
                "site": "example.com",
                "uplink_bytes": 100,
                "downlink_bytes": 200,
            },
            {
                "flow_id": "flow-1",
                "segment": 1,
                "interval_started_at": "2026-08-24T10:05:00Z",
                "started_at": "2026-08-24T10:00:00Z",
                "ended_at": "2026-08-24T10:10:00Z",
                "source_ip": "192.0.2.10",
                "site": "example.com",
                "uplink_bytes": 50,
                "downlink_bytes": 75,
            },
            {
                "started_at": "2026-08-24T10:06:00Z",
                "ended_at": "2026-08-24T10:07:00Z",
                "source_ip": "192.0.2.10",
                "site": "example.com",
                "uplink_bytes": 10,
                "downlink_bytes": 20,
            },
        ]
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as stream:
            for record in records:
                stream.write(json.dumps(record) + "\n")
            path = stream.name
        try:
            since = datetime(2026, 8, 24, tzinfo=timezone.utc)
            loaded = REPORT.load_records([path], since, None, None, None)
            rows = REPORT.aggregate(loaded, "source-site")
        finally:
            os.unlink(path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["requests"], 2)
        self.assertEqual(rows[0]["uplink_bytes"], 160)
        self.assertEqual(rows[0]["downlink_bytes"], 295)
        self.assertEqual(rows[0]["total_bytes"], 455)


if __name__ == "__main__":
    unittest.main()
