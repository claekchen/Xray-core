# Per-flow traffic accounting

This build can emit one JSON Lines record for every logical flow handled by the default dispatcher. The record contains the source IP and port, the destination site and port, the selected outbound tag, and logical uplink/downlink byte counts.

Set `XRAY_FLOW_LOG` to enable the feature:

```ini
[Service]
Environment=XRAY_FLOW_LOG=/var/log/xray/flow.jsonl
Environment=XRAY_FLOW_SNAPSHOT_INTERVAL=5m
```

Example deployment files for the systemd drop-in and daily log rotation are included in this directory. Pre-create the output file with ownership matching the Xray service user before restarting the service.

The feature is disabled when the log environment variable is unset or empty. The destination is captured after protocol sniffing and routing but before DNS resolution, so domain names remain available. Accounting is attached after inbound multiplexing has been split into logical flows; multiple destination sites sharing one transport connection are recorded separately.

Active flows emit incremental checkpoint records at the configured interval, which defaults to five minutes. A final incremental record is emitted when the logical flow closes. All segments share a `flow_id`, and the report tool de-duplicates request counts by that ID. This prevents long-lived connections from delaying accounting or losing all in-progress usage during a process restart.

The output file is opened in append mode for each completed flow. This allows normal daily log rotation without signaling or restarting Xray. The service user must be able to create or append the configured file.

Example record:

```json
{"started_at":"2026-08-24T10:00:00Z","ended_at":"2026-08-24T10:00:01Z","duration_ms":1000,"source_ip":"192.0.2.10","source_port":54321,"inbound_tag":"vless-in","user":"primary","network":"tcp","site":"example.com","target_port":443,"original_site":"203.0.113.20","original_port":443,"outbound_tag":"direct","protocol":"tls","uplink_bytes":1024,"downlink_bytes":4096}
```

Use the bundled report tool to aggregate rotated plain or gzip-compressed logs:

```bash
python3 contrib/flow-accounting/xray-flow-report.py --days 30 --group-by source-site
python3 contrib/flow-accounting/xray-flow-report.py --days 7 --group-by site --limit 100
python3 contrib/flow-accounting/xray-flow-report.py --days 1 --source 192.0.2.0/24 --format json
```
