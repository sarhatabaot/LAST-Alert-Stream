import os
import json
import math
import time
import gzip
import shutil
import sqlite3
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from confluent_kafka import Producer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
from dateutil import parser as dtparser

# ---------------------------
# Config
# ---------------------------

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "last.alerts.raw")

INPUT_DIR = os.environ.get("INPUT_DIR", "/input")
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "/archive")
STATE_DB = os.environ.get("STATE_DB", "/state/state.db")

POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "2"))
PUBLISH_MODE = os.environ.get("PUBLISH_MODE", "live").strip().lower()  # live|backfill

# In live mode only: skip events older than cutoff (event_time-based).
# Leave empty to disable.
CUTOFF_DAYS_RAW = os.environ.get("CUTOFF_DAYS", "").strip()
CUTOFF_DAYS = int(CUTOFF_DAYS_RAW) if CUTOFF_DAYS_RAW else None

CUTOUT_BASE_URL = os.environ.get("CUTOUT_BASE_URL", "").strip().rstrip("/")

ARCHIVE_ROTATE_HOURS = int(os.environ.get("ARCHIVE_ROTATE_HOURS", "1"))


# ---------------------------
# Helpers
# ---------------------------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


def sanitize(obj: Any) -> Any:
    """Recursively replace NaN/Inf with None so JSON is strict."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    return obj


def file_is_stable(path: str, stable_for_s: float = 0.5) -> bool:
    """Avoid reading a file that is still being written."""
    try:
        s1 = os.path.getsize(path)
        time.sleep(stable_for_s)
        s2 = os.path.getsize(path)
        return s1 == s2
    except FileNotFoundError:
        return False


def parse_event_time_ms(payload: Dict[str, Any], fallback_epoch_ms: int) -> int:
    """Prefer at_report.discovery_datetime[0] else fallback to file mtime."""
    try:
        dt_str = payload["at_report"]["discovery_datetime"][0]
        dt = dtparser.parse(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return fallback_epoch_ms


def compute_alert_id(payload: Dict[str, Any], file_path: str, event_time_ms: int) -> str:
    """
    Patch-friendly stable-ish ID. Prefer a true upstream alert ID if you have one.
    Here: sha1(basename|event_time|object_id)
    """
    obj = str(payload.get("last_report", {}).get("object", ""))
    base = f"{os.path.basename(file_path)}|{event_time_ms}|{obj}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def compute_file_sig(file_path: str, st: os.stat_result) -> str:
    """Signature used for dedupe when we cannot move files: path|mtime|size."""
    base = f"{file_path}|{int(st.st_mtime)}|{int(st.st_size)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def cutout_links(payload: Dict[str, Any]) -> Dict[str, Optional[str]]:
    lr = payload.get("last_report", {}) or {}
    cutouts = {
        "ref": lr.get("ref_cutout"),
        "new": lr.get("new_cutout"),
        "diff": lr.get("diff_cutout"),
    }
    if CUTOUT_BASE_URL:
        for k, v in list(cutouts.items()):
            if v and not str(v).startswith("http"):
                cutouts[k] = f"{CUTOUT_BASE_URL}/{str(v).lstrip('/')}"
    return cutouts


def ensure_dirs() -> None:
    os.makedirs(os.path.dirname(STATE_DB), exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(STATE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS published_files (
            file_sig TEXT PRIMARY KEY,
            file_path TEXT NOT NULL,
            mtime INTEGER NOT NULL,
            size INTEGER NOT NULL,
            alert_id TEXT NOT NULL,
            event_time_ms INTEGER NOT NULL,
            published_at_ms INTEGER NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_time ON published_files(event_time_ms)")
    conn.commit()
    return conn


def already_published(conn: sqlite3.Connection, file_sig: str) -> bool:
    cur = conn.execute("SELECT 1 FROM published_files WHERE file_sig = ? LIMIT 1", (file_sig,))
    return cur.fetchone() is not None


def mark_published(conn: sqlite3.Connection, *, file_sig: str, file_path: str, st: os.stat_result,
                   alert_id: str, event_time_ms: int) -> None:
    now_ms = int(time.time() * 1000)
    conn.execute("""
        INSERT OR REPLACE INTO published_files
        (file_sig, file_path, mtime, size, alert_id, event_time_ms, published_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (file_sig, file_path, int(st.st_mtime), int(st.st_size), alert_id, event_time_ms, now_ms))
    conn.commit()


def archive_write(envelope: Dict[str, Any]) -> None:
    """Append one JSON line to a rotated .jsonl.gz file, partitioned by event_time."""
    dt = datetime.fromtimestamp(envelope["event_time_ms"] / 1000, tz=timezone.utc)
    subdir = os.path.join(ARCHIVE_DIR, "alerts", dt.strftime("%Y/%m/%d"))
    os.makedirs(subdir, exist_ok=True)

    # Rotate by hour buckets (default 1 hour)
    hour_bucket = (dt.hour // ARCHIVE_ROTATE_HOURS) * ARCHIVE_ROTATE_HOURS
    bucket_dt = dt.replace(minute=0, second=0, microsecond=0, hour=hour_bucket)
    fname = f"alerts_{bucket_dt.strftime('%Y%m%d_%H')}.jsonl.gz"
    path = os.path.join(subdir, fname)

    line = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)
    with gzip.open(path, "at", encoding="utf-8") as f:
        f.write(line + "\n")


def make_producer() -> Producer:
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "alert-publisher",
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10,
        "linger.ms": 20,
        "compression.type": "lz4",
        "message.timeout.ms": 30000,
    }
    return Producer(conf)


def produce_one(producer: Producer, envelope: Dict[str, Any], key: bytes, timestamp_ms: int) -> None:
    delivered = {"ok": False, "err": None}

    def cb(err, msg):
        if err is not None:
            delivered["err"] = err
        else:
            delivered["ok"] = True

    producer.produce(
        TOPIC,
        value=json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        key=key,
        timestamp=timestamp_ms,
        on_delivery=cb,
    )
    producer.flush(10)
    if not delivered["ok"]:
        raise RuntimeError(f"Kafka delivery failed: {delivered['err']}")


def should_skip_by_cutoff(event_time_ms: int) -> bool:
    if PUBLISH_MODE != "live" or CUTOFF_DAYS is None:
        return False
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=CUTOFF_DAYS)).timestamp() * 1000)
    return event_time_ms < cutoff_ms


def process_file(conn: sqlite3.Connection, producer: Producer, file_path: str) -> None:
    if not file_path.endswith(".json"):
        return
    if not os.path.isfile(file_path):
        return
    if not file_is_stable(file_path, stable_for_s=0.5):
        return

    st = os.stat(file_path)
    file_sig = compute_file_sig(file_path, st)
    if already_published(conn, file_sig):
        return

    mtime_ms = int(st.st_mtime * 1000)
    with open(file_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    payload = sanitize(payload)
    event_time_ms = parse_event_time_ms(payload, fallback_epoch_ms=mtime_ms)

    if should_skip_by_cutoff(event_time_ms):
        # Still mark as "seen" or not? Usually NO (so it can be replayed later if cutoff changes).
        # We'll skip without marking.
        return

    alert_id = compute_alert_id(payload, file_path, event_time_ms)

    envelope = {
        "alert_id": alert_id,
        "event_time_ms": event_time_ms,
        "ingest_time_ms": int(time.time() * 1000),
        "source": "LAST",
        "cutouts": cutout_links(payload),
        "payload": payload,
    }

    key_str = str(payload.get("last_report", {}).get("object", alert_id))
    key = key_str.encode("utf-8")

    # 1) Publish to Kafka (timestamp set to event_time for correct retention/windowing)
    produce_one(producer, envelope, key=key, timestamp_ms=event_time_ms)

    # 2) Archive after Kafka ACK
    archive_write(envelope)

    # 3) Mark published in state DB
    mark_published(conn, file_sig=file_sig, file_path=file_path, st=st, alert_id=alert_id, event_time_ms=event_time_ms)

    log(f"Published {os.path.basename(file_path)} alert_id={alert_id} event_time_ms={event_time_ms}")


class Handler(FileSystemEventHandler):
    def __init__(self, conn: sqlite3.Connection, producer: Producer):
        self.conn = conn
        self.producer = producer

    def on_created(self, event):
        if not event.is_directory:
            try:
                process_file(self.conn, self.producer, event.src_path)
            except Exception as e:
                log(f"ERROR publishing {event.src_path}: {e}")

    def on_moved(self, event):
        if not event.is_directory:
            try:
                process_file(self.conn, self.producer, event.dest_path)
            except Exception as e:
                log(f"ERROR publishing {event.dest_path}: {e}")


def backlog_scan(conn: sqlite3.Connection, producer: Producer) -> None:
    # Scan directory; in backfill mode, publish everything unseen.
    # In live mode, also publish unseen (optionally with cutoff).
    try:
        names = sorted(os.listdir(INPUT_DIR))
    except FileNotFoundError:
        log(f"INPUT_DIR not found: {INPUT_DIR}")
        return

    for name in names:
        path = os.path.join(INPUT_DIR, name)
        try:
            process_file(conn, producer, path)
        except Exception as e:
            log(f"ERROR backlog {path}: {e}")


def main() -> None:
    ensure_dirs()
    conn = init_db()
    producer = make_producer()

    log(f"Starting publisher mode={PUBLISH_MODE} input={INPUT_DIR} topic={TOPIC} bootstrap={BOOTSTRAP_SERVERS}")
    if CUTOFF_DAYS is not None:
        log(f"Live cutoff enabled: {CUTOFF_DAYS} days (event-time)")

    backlog_scan(conn, producer)

    observer = PollingObserver(timeout=POLL_SECONDS)
    observer.schedule(Handler(conn, producer), INPUT_DIR, recursive=False)
    observer.start()
    log(f"Polling {INPUT_DIR} every ~{POLL_SECONDS}s (NFS-safe)")

    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    main()
