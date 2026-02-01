# Agents (POC)

This initial proof of concept defines the core agents involved in the LAST alert
stream pipeline and their responsibilities. These responsibilities may evolve as
the system grows.

## Project Overview

The LAST alert stream ingests alert data, publishes it to Kafka, consumes alerts
for downstream processing, and supports tooling for replaying archived data.

## Agents & Responsibilities

### Publisher Agent

- Streams alert data into Kafka topics.
- Ensures data is serialized and formatted according to the alert schema.
- Handles retry/backoff for transient publish failures.

### Consumer Agent (Example Usage)

- Serves as an example consumer implementation for the POC.
- Subscribes to alert topics and processes inbound messages.
- Validates payloads and performs domain-specific enrichment or filtering.
- Emits processed outputs or metrics for downstream systems.

### Replay Tools Agent

- Replays archived alert data into Kafka for backfill or testing.
- Supports batch replay configuration (time ranges, topics, filters).
- Provides operational safeguards (rate limiting, progress logging).

## Future Considerations

- Add monitoring/observability agents (metrics, tracing, alerting).
- Introduce schema registry and validation agent for compatibility checks.
