import os
import json
import time
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "last.alerts.raw")
GROUP_ID = os.environ.get("GROUP_ID", "example-consumer")
AUTO_OFFSET_RESET = os.environ.get("AUTO_OFFSET_RESET", "latest")

conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": AUTO_OFFSET_RESET,  # earliest|latest
    "enable.auto.commit": False,
}

def main():
    c = Consumer(conf)
    c.subscribe([TOPIC])
    print(f"Consumer started: topic={TOPIC} group={GROUP_ID} bootstrap={BOOTSTRAP_SERVERS}", flush=True)

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}", flush=True)
                continue

            env = json.loads(msg.value().decode("utf-8"))
            alert_id = env.get("alert_id")
            event_time_ms = env.get("event_time_ms")
            cutouts = env.get("cutouts", {})

            print(f"Got alert_id={alert_id} event_time_ms={event_time_ms} cutouts={list(cutouts.keys())}", flush=True)

            # Commit only after successful processing
            c.commit(message=msg)
    finally:
        c.close()

if __name__ == "__main__":
    main()
