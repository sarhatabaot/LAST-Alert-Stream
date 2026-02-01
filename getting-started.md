# Alert Stream Starter (Kafka + Filesystem Archive)

This starter kit turns a **read-only NFS folder of JSON alerts** into a **Kafka alert stream** with a **filesystem archive**, without moving or modifying the source files.

It includes:
- Docker Compose stack (Kafka + publisher + example consumer)
- Publisher service:
  - polls NFS directory (NFS-safe)
  - sanitizes JSON (NaN/Inf -> null)
  - wraps payload in a stable envelope
  - deduplicates via SQLite (no file moves)
  - publishes to Kafka
  - appends to an archive (JSONL.GZ) after Kafka ACK
- Consumer example:
  - subscribes and prints basic fields

---

## Prerequisites
- Docker + Docker Compose
- Access to the NFS folder on the host machine running Docker
  - You will mount it read-only into the publisher container.

---

## Quick start

### 1) Put your NFS path into `.env`
Copy the example env file:
```bash
cp .env.example .env
```

Edit `.env` and set:
- `INPUT_DIR_HOST` to the **host path** of your NFS alerts folder
- (Optional) `CUTOUT_BASE_URL` if you want to convert cutout filenames to URLs

Example:
```env
INPUT_DIR_HOST=/net/nfs/alerts/json
```

### 2) Start the stack
```bash
docker compose up --build
```

You should see:
- `kafka` come up
- `publisher` start scanning and then polling
- `consumer` start and wait for messages

### 3) Backfill (publish everything once)
The publisher supports a backfill mode that scans the directory and publishes all unseen files.

Run:
```bash
PUBLISH_MODE=backfill docker compose up --build publisher
```

Then switch back to live mode:
```bash
PUBLISH_MODE=live docker compose up --build publisher
```

> The publisher **never** moves or modifies the source files. It uses `./state/state.db` to remember what was published.

---

## What gets published?

Topic: `last.alerts.raw`

Each message is a JSON envelope:
```json
{
  "alert_id": "sha1(...)",
  "event_time_ms": 1768863398000,
  "ingest_time_ms": 1769000000000,
  "source": "LAST",
  "cutouts": {"ref": "...", "new": "...", "diff": "..."},
  "payload": { "at_report": {...}, "last_report": {...} }
}
```

### Important note about NaN
If your JSON contains values like `NaN` (not valid strict JSON), the publisher converts them to `null` so downstream consumers in other languages won’t crash.

---

## Replay options

### Replay within Kafka retention
Kafka is configured for a finite retention window (default: 7 days). To reprocess within the window:
- start a consumer with a new consumer group, or
- reset offsets to earliest (example script below).

### Replay from filesystem archive (infinite history)
The publisher writes an append-only archive:
```
./archive/alerts/YYYY/MM/DD/alerts_YYYYMMDD_HH.jsonl.gz
```

You can replay archive → Kafka using the included script:
```bash
docker compose run --rm tools python tools/replay_archive_to_kafka.py
```

---

## Configuration

All configuration is via environment variables in `.env`:

- Kafka:
  - `BOOTSTRAP_SERVERS` (default: `kafka:9092`)
  - `TOPIC` (default: `last.alerts.raw`)
- Publisher:
  - `INPUT_DIR` (container path, default: `/input`)
  - `ARCHIVE_DIR` (default: `/archive`)
  - `STATE_DB` (default: `/state/state.db`)
  - `POLL_SECONDS` (default: 2)
  - `PUBLISH_MODE` (`live` or `backfill`)
  - `CUTOFF_DAYS` (live mode only; set empty to disable)
  - `CUTOUT_BASE_URL` (optional)
- Archive:
  - `ARCHIVE_ROTATE_HOURS` (default: 1; rotates by hour)

---

## Operational notes

- **No file moves**: safe for read-only or shared NFS directories.
- **NFS polling**: uses a polling observer instead of inotify for reliability.
- **Exactly-once-ish**: uses Kafka idempotent producer + SQLite state + commit-after-ACK pattern.
- **Scaling**: increase topic partitions and run more consumers; keep a single publisher unless you coordinate dedupe keys.

---

## Offset reset helper (optional)

If you have the Kafka CLI available:
```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group my-consumer \
  --topic last.alerts.raw \
  --reset-offsets --to-earliest --execute
```

---

## Files
- `docker-compose.yml` — full stack
- `publisher/` — publisher service
- `consumer/` — example consumer
- `tools/` — replay script
- `getting-started.md` — this document
