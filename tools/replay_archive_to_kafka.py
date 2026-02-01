import os
import gzip
import json
import time
from pathlib import Path
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "last.alerts.raw")
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "/archive")

RATE_LIMIT_PER_SEC = float(os.environ.get("RATE_LIMIT_PER_SEC", "0"))  # 0 = unlimited

def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "archive-replayer",
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10,
        "linger.ms": 20,
        "compression.type": "lz4",
        "message.timeout.ms": 30000,
    })

def iter_archive_files(root: Path):
    # Looks for archive/alerts/**/alerts_*.jsonl.gz
    alerts_root = root / "alerts"
    if not alerts_root.exists():
        return
    for p in sorted(alerts_root.rglob("alerts_*.jsonl.gz")):
        yield p

def main():
    root = Path(ARCHIVE_DIR)
    producer = make_producer()

    print(f"Replaying from {root} to topic={TOPIC} bootstrap={BOOTSTRAP_SERVERS}", flush=True)

    count = 0
    last_tick = time.time()

    for fpath in iter_archive_files(root):
        print(f"Reading {fpath}", flush=True)
        with gzip.open(fpath, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                env = json.loads(line)
                key = str(env.get("payload", {}).get("last_report", {}).get("object", env.get("alert_id", ""))).encode("utf-8")
                ts = int(env.get("event_time_ms", int(time.time()*1000)))

                delivered = {"ok": False, "err": None}
                def cb(err, msg):
                    if err is not None:
                        delivered["err"] = err
                    else:
                        delivered["ok"] = True

                producer.produce(
                    TOPIC,
                    value=json.dumps(env, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
                    key=key,
                    timestamp=ts,
                    on_delivery=cb,
                )
                producer.flush(10)
                if not delivered["ok"]:
                    raise RuntimeError(f"Kafka delivery failed: {delivered['err']}")

                count += 1

                if RATE_LIMIT_PER_SEC and RATE_LIMIT_PER_SEC > 0:
                    # crude rate limit
                    now = time.time()
                    elapsed = now - last_tick
                    if elapsed < 1.0 / RATE_LIMIT_PER_SEC:
                        time.sleep((1.0 / RATE_LIMIT_PER_SEC) - elapsed)
                    last_tick = time.time()

    print(f"Done. Replayed {count} alerts.", flush=True)

if __name__ == "__main__":
    main()
