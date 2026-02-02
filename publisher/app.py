import gzip
import hashlib
import json
import math
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from confluent_kafka import Producer, KafkaException
from dateutil import parser as dtparser
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "last.alerts.raw")

INPUT_DIR = os.environ.get("INPUT_DIR", "/input")
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "/archive")
STATE_DB = os.environ.get("STATE_DB", "/state/state.db")

POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "2"))
PUBLISH_MODE = os.environ.get("PUBLISH_MODE", "live").strip().lower()

CUTOFF_DAYS_RAW = os.environ.get("CUTOFF_DAYS", "").strip()
CUTOFF_DAYS = int(CUTOFF_DAYS_RAW) if CUTOFF_DAYS_RAW else None

CUTOUT_BASE_URL = os.environ.get("CUTOUT_BASE_URL", "").strip().rstrip("/")
ARCHIVE_ROTATE_HOURS = int(os.environ.get("ARCHIVE_ROTATE_HOURS", "1"))


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


def sanitize(obj: Any) -> Any:
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
    try:
        s1 = os.path.getsize(path)
        time.sleep(stable_for_s)
        s2 = os.path.getsize(path)
        return s1 == s2
    except FileNotFoundError:
        return False


def parse_event_time_ms(payload: Dict[str, Any], fallback_epoch_ms: int) -> int:
    try:
        dt_str = payload["at_report"]["discovery_datetime"][0]
        dt = dtparser.parse(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return fallback_epoch_ms


def compute_alert_id(payload: Dict[str, Any], file_path: str, event_time_ms: int) -> str:
    obj = str(payload.get("last_report", {}).get("object", ""))
    base = f"{os.path.basename(file_path)}|{event_time_ms}|{obj}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def compute_file_sig(file_path: str, st: os.stat_result) -> str:
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


def mark_published(
        conn: sqlite3.Connection,
        *,
        file_sig: str,
        file_path: str,
        st: os.stat_result,
        alert_id: str,
        event_time_ms: int,
) -> None:
    now_ms = int(time.time() * 1000)
    conn.execute("""
        INSERT OR REPLACE INTO published_files
        (file_sig, file_path, mtime, size, alert_id, event_time_ms, published_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (file_sig, file_path, int(st.st_mtime), int(st.st_size), alert_id, event_time_ms, now_ms))
    conn.commit()


def archive_write(envelope: Dict[str, Any]) -> None:
    dt = datetime.fromtimestamp(envelope["event_time_ms"] / 1000, tz=timezone.utc)
    subdir = os.path.join(ARCHIVE_DIR, "alerts", dt.strftime("%Y/%m/%d"))
    os.makedirs(subdir, exist_ok=True)

    hour_bucket = (dt.hour // ARCHIVE_ROTATE_HOURS) * ARCHIVE_ROTATE_HOURS
    bucket_dt = dt.replace(minute=0, second=0, microsecond=0, hour=hour_bucket)
    fname = f"alerts_{bucket_dt.strftime('%Y%m%d_%H')}.jsonl.gz"
    path = os.path.join(subdir, fname)

    line = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)
    with gzip.open(path, "at", encoding="utf-8") as f:
        f.write(line + "\n")


def wait_for_idempotent_producer() -> Producer:
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "alert-publisher",
        "enable.idempotence": True,
        "acks": "all",
        "retries": 100000,
        "retry.backoff.ms": 1000,
        "linger.ms": 20,
        "compression.type": "lz4",
        "message.timeout.ms": 60000,
    }

    log("Waiting for Kafka idempotence coordinator...")

    while True:
        try:
            p = Producer(conf)
            p.produce(TOPIC, value=b"__bootstrap__", key=b"bootstrap")
            p.flush(10)
            log("Kafka idempotent producer ready.")
            return p
        except KafkaException as e:
            log(f"Kafka not ready (PID): {e}")
        except Exception as e:
            log(f"Kafka not ready: {e}")
        time.sleep(2)


def produce_one(producer: Producer, envelope: Dict[str, Any], key: bytes, timestamp_ms: int) -> None:
    producer.produce(
        TOPIC,
        value=json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        key=key,
        timestamp=timestamp_ms,
    )
    producer.poll(0)


def should_skip_by_cutoff(event_time_ms: int) -> bool:
    if PUBLISH_MODE != "live" or CUTOFF_DAYS is None:
        return False
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=CUTOFF_DAYS)).timestamp() * 1000)
    return event_time_ms < cutoff_ms


def process_file(conn: sqlite3.Connection, producer: Producer, file_path: str) -> None:
    if not file_path.endswith(".json") or not os.path.isfile(file_path):
        return
    if not file_is_stable(file_path):
        return

    st = os.stat(file_path)
    file_sig = compute_file_sig(file_path, st)
    if already_published(conn, file_sig):
        return

    mtime_ms = int(st.st_mtime * 1000)
    with open(file_path, "r", encoding="utf-8") as f:
        payload = sanitize(json.load(f))

    event_time_ms = parse_event_time_ms(payload, mtime_ms)
    if should_skip_by_cutoff(event_time_ms):
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

    key = str(payload.get("last_report", {}).get("object", alert_id)).encode("utf-8")

    produce_one(producer, envelope, key, event_time_ms)
    archive_write(envelope)
    mark_published(
        conn,
        file_sig=file_sig,
        file_path=file_path,
        st=st,
        alert_id=alert_id,
        event_time_ms=event_time_ms,
    )

    log(f"Published {os.path.basename(file_path)} alert_id={alert_id}")


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
    try:
        for name in sorted(os.listdir(INPUT_DIR)):
            process_file(conn, producer, os.path.join(INPUT_DIR, name))
    except FileNotFoundError:
        log(f"INPUT_DIR not found: {INPUT_DIR}")


def main() -> None:
    ensure_dirs()
    conn = init_db()

    producer = wait_for_idempotent_producer()

    log(f"Starting publisher mode={PUBLISH_MODE} topic={TOPIC}")
    backlog_scan(conn, producer)

    observer = PollingObserver(timeout=POLL_SECONDS)
    observer.schedule(Handler(conn, producer), INPUT_DIR, recursive=False)
    observer.start()
    log(f"Polling {INPUT_DIR} every ~{POLL_SECONDS}s")

    try:
        last_flush = time.time()

        while True:
            producer.poll(0)
            if time.time() - last_flush > 5:
                producer.flush(0)
                last_flush = time.time()
            time.sleep(1)
    except KeyboardInterrupt:
        log("Shutting down...")
    finally:
        observer.stop()
        observer.join()
        producer.flush(10)


if __name__ == "__main__":
    main()
