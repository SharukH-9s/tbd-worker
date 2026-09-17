# Background Queue Implementation — Type B
## Postgres Outbox + CloudAMQP (RabbitMQ) + tbd-worker

This document is the finalized, production-ready specification and implementation guide for the background queue architecture connecting **TBD (Axum Backend & Outbox Relay)**, **CloudAMQP (RabbitMQ Broker)**, and **tbd-worker (Background Consumer)**.

---

## 1. Architecture Overview

```
tbd-backend (Axum API)
  ├── [Booking Handler] ── writes domain booking + outbox row atomically (in 1 DB transaction)
  │                                    ↓
  │                             Postgres (outbox table)
  │                                    ↑
  └── [Relay Task] ──────────── polls outbox (drain loop)
        ├── Claims row: status = 'publishing' (FOR UPDATE SKIP LOCKED)
        ├── Recovers stale locks (> 5 min) on restart
        ├── Wraps in standardized EventEnvelope
        ├── Publishes with Topic Routing Key (e.g. "booking.created")
        ├── Waits for Publisher Confirm ACK
        └── Marks row: status = 'done' (or schedules exponential retry)
                                       ↓
                             CloudAMQP (RabbitMQ)
                        Exchange: "tbd.events" (Topic)
                                       │
                      (Routing: "booking.#")
                                       v
                                  booking_jobs
                                       │
                                       ├─► (on permanent failure / requeue: false) ─► "tbd.dlx" ─► "tbd.dlq"
                                       │
                                       v
                                  tbd-worker
                         (Booking Consumer Service)
        ├── 1. Idempotency Pre-Check: SELECT FROM processed_jobs (skip if already done)
        ├── 2. Generate PDF Invoice: Gotenberg API (HTML-to-PDF, Basic Auth protected)
        ├── 3. Base64 Encode PDF Attachment
        ├── 4. Send Confirmation Email: Resend API
        ├── 5. In-process Transient Retry: exponential backoff (max 3 attempts)
        ├── 6. Record Completion: INSERT INTO processed_jobs
        └── 7. ACK message (or NACK to DLQ if permanent failure)
                                       ▲
  tbd-backend (Keep-Alive Task) ───────┘
  Pings GET /healthz every 10 min to keep Render's free-tier worker awake.
```

---

## 2. Database Schema

### 2.1 Outbox Table (`outbox`)
The `outbox` table lives in the primary Postgres database and decouples synchronous HTTP requests from asynchronous task execution.

```sql
CREATE TABLE public.outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued',
    retry_count     INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    process_after   TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Partial index for high-performance relay polling
CREATE INDEX idx_outbox_queued 
    ON public.outbox (created_at, process_after) 
    WHERE status = 'queued';

CREATE INDEX idx_outbox_event_type 
    ON public.outbox (event_type) 
    WHERE status IN ('queued', 'failed');
```

#### Outbox Lifecycle States

```
   [Handler writes] ──► queued ──► publishing ──► done (Terminal)
                                      │
                                      └──► failed ──► retry? ──► queued (with backoff delay)
                                                          │
                                                   (retry >= 5)
                                                          │
                                                   stays 'failed' permanently
```

| Status | Set By | Description |
| :--- | :--- | :--- |
| `queued` | API Handler | Job created and ready for relay dispatch. |
| `publishing` | Relay Task | Claimed by relay via `FOR UPDATE SKIP LOCKED`. |
| `done` | Relay Task | Confirmed published to CloudAMQP. |
| `failed` | Relay Task | Transient retries exceeded limit ($5$ attempts); requires manual review. |

### 2.2 Deduplication Table (`processed_jobs`)
Maintains an exact audit of completed consumer executions across worker restarts and redeliveries.

```sql
CREATE TABLE public.processed_jobs (
    outbox_id    UUID NOT NULL,
    consumer     TEXT NOT NULL, -- e.g. 'booking'
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    PRIMARY KEY (outbox_id, consumer)
);
```

---

## 3. RabbitMQ Topology & Contracts

### 3.1 Topology Configuration
Both `tbd-backend` (Relay) and `tbd-worker` (Consumer) declare identical topology idempotently on startup:

* **Exchange:** `tbd.events` (`ExchangeKind::Topic`, `durable: true`)
* **Dead Letter Exchange (DLX):** `tbd.dlx` (`ExchangeKind::Fanout`, `durable: true`)
* **Dead Letter Queue (DLQ):** `tbd.dlq` (`durable: true`, bound to `tbd.dlx`)
* **Queue:** `booking_jobs` (`durable: true`, argument: `x-dead-letter-exchange = "tbd.dlx"`)
* **Binding:** `booking_jobs` bound to `tbd.events` with routing pattern `booking.#`

### 3.2 Standardized Event Envelope
The relay encapsulates domain payloads into a structured metadata envelope:

```json
{
  "id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "event_type": "BookingCreated",
  "timestamp": "2026-08-23T12:00:00.000Z",
  "payload": {
    "booking_id": 42,
    "user_email": "user@example.com",
    "contact_name": "Alice Rahman",
    "slot_start": "2026-08-25T18:00:00Z",
    "amount": "1500.00"
  }
}
```

### 3.3 Routing Key Mapping
Event types (PascalCase) are automatically normalized to dot-separated lower-case routing keys:
* `BookingCreated` $\to$ `booking.created` (matches `booking.#`)
* `BookingCancelled` $\to$ `booking.cancelled` (matches `booking.#`)

---

## 4. `tbd-backend` — Outbox Relay Implementation

The Outbox Relay runs as a dedicated `tokio::spawn` task in `TBD`.

### 4.1 Key Relay Mechanics
1. **Active Drain Loop**: When triggered, drains all available ready outbox rows in a tight loop before sleeping, eliminating burst processing lag.
2. **Stale Lock Auto-Recovery**: On startup and reconnection, automatically resets rows stuck in `'publishing'` for $> 5$ minutes back to `'queued'`:
   ```sql
   UPDATE outbox
   SET    status = 'queued', updated_at = NOW()
   WHERE  status = 'publishing'
   AND    updated_at < NOW() - INTERVAL '5 minutes';
   ```
3. **Publisher Confirms**: Enables `confirm_select()` and awaits broker confirmation before committing `'done'`.
4. **Exponential Backoff**: If publish fails, schedules retries with exponential backoff ($5\text{s} \times 2^{\text{retry\_count}}$) up to 5 attempts:
   ```sql
   UPDATE outbox
   SET    status        = 'queued',
          retry_count   = retry_count + 1,
          last_error    = $2,
          process_after = NOW() + (INTERVAL '5 seconds' * POWER(2, retry_count)),
          updated_at    = NOW()
   WHERE  id = $1;
   ```

### 4.2 Code References
* Implementation: [`src/services/relay.rs`](file:///e:/projects/anti/TBD/src/services/relay.rs)
* Registration: [`src/services/mod.rs`](file:///e:/projects/anti/TBD/src/services/mod.rs)
* Spawn in main: [`src/main.rs`](file:///e:/projects/anti/TBD/src/main.rs#L198-L203)
* Keep-alive ping: [`src/api/utils/tester.rs`](file:///e:/projects/anti/TBD/src/api/utils/tester.rs#L86-L137)

---

## 5. `tbd-worker` — Background Consumer Implementation

`tbd-worker` is a standalone Rust service dedicated to executing background tasks.

### 5.1 Project Layout
```
tbd-worker/
├── Cargo.toml
└── src/
    ├── main.rs            # Entry point, health check server, runtime config
    ├── config.rs          # WorkerConfig struct
    ├── error.rs           # ConsumerError (Permanent vs Transient)
    └── consumers/
        ├── mod.rs         # AMQP connection loop & consumer supervision
        └── booking.rs     # Topology setup, message ingestion, PDF & Email workflow
```

### 5.2 Idempotency & Processing Sequence
To eliminate data loss and prevent duplicate side-effects:

```
[Delivery Received from booking_jobs]
       │
       ▼
[1. Deserialize EventEnvelope<BookingCreatedData>]
       ├─► (Invalid JSON) ──► NACK (requeue: false) ──► tbd.dlx ──► tbd.dlq
       ▼
[2. Idempotency Pre-Check]
       SELECT 1 FROM processed_jobs WHERE outbox_id = $1 AND consumer = 'booking'
       ├─► (Already Exists) ──► ACK & Skip
       ├─► (DB Error) ────────► NACK (requeue: true) & Skip
       ▼
[3. In-Process Execution with Retry Loop]
       Attempt 1..3:
         a. Generate PDF invoice via Gotenberg
         b. Base64-encode PDF
         c. Send email with attachment via Resend API
       ├─► (Transient Error) ─► Sleep backoff (2^attempt s) & Retry
       ├─► (Permanent Error or Max Attempts Reached) ─► NACK (requeue: false) ──► tbd.dlx ──► tbd.dlq
       ▼
[4. Commit on Success]
       INSERT INTO processed_jobs (outbox_id, consumer) VALUES ($1, 'booking') ON CONFLICT DO NOTHING
       ▼
[5. Manual ACK]
       delivery.ack() ──► RabbitMQ permanently removes message
```

### 5.3 Error Classification & DLQ Routing

| Error Scenario | Classification | Consumer Action | Reason |
| :--- | :--- | :--- | :--- |
| Email Sent + PDF Created | **Success** | `ack()` + write `processed_jobs` | Clean completion |
| Malformed JSON Body | **Permanent** | `nack(requeue: false)` | Corrupt payload; routes to DLQ |
| Resend 4xx (401, 403, 422) | **Permanent** | `nack(requeue: false)` | Invalid API key or domain; routes to DLQ |
| Gotenberg 4xx Client Error | **Permanent** | `nack(requeue: false)` | Invalid HTML payload; routes to DLQ |
| Resend 429 (Rate Limited) | **Transient** | Retry with backoff $\to$ `nack(requeue: false)` if maxed | Will succeed after rate limit reset |
| Resend 5xx Server Error | **Transient** | Retry with backoff $\to$ `nack(requeue: false)` if maxed | Upstream outage recovery |
| Network Timeout / Drop | **Transient** | Retry with backoff $\to$ `nack(requeue: false)` if maxed | Temporary network blip |
| Postgres Idempotency Check Error | **Transient** | `nack(requeue: true)` | DB connection blip; safe to retry |

---

## 6. Production Deployment & Render Free-Tier Setup

To host the entire background infrastructure with **$0/month hosting cost** on Render while maintaining high reliability:

### 6.1 Gotenberg Deployment (HTML $\to$ PDF)
* **Deployment Type**: Render **Free Web Service**
* **Image**: `docker.io/gotenberg/gotenberg:8`
* **Port**: `3000`
* **Security (Basic Auth)**:
  * `API_ENABLE_BASIC_AUTH` = `true`
  * `GOTENBERG_API_BASIC_AUTH_USERNAME` = `admin`
  * `GOTENBERG_API_BASIC_AUTH_PASSWORD` = `<strong_password>`
* `tbd-worker` automatically includes HTTP Basic Auth headers using `GOTENBERG_USER` and `GOTENBERG_PASSWORD`.

### 6.2 `tbd-worker` Deployment
* **Deployment Type**: Render **Free Web Service** (bypasses the paid Background Worker requirement)
* **Build Command**: `cargo build --release`
* **Start Command**: `cargo run --release`
* **Health Check Path**: `/healthz` (served via lightweight internal Axum router on `$PORT`)

### 6.3 Keep-Alive Ping Architecture
Render's free web services spin down after 15 minutes of HTTP inactivity.
* `TBD` runs a background task (`api::utils::tester::keep_alive_worker`) that issues `GET {WORKER_URL}/healthz` every 10 minutes.
* This keeps `tbd-worker` active and its RabbitMQ consumer connection continuously listening.

---

## 7. Environment Variables Reference

### For `tbd-backend` (`TBD`)
| Variable Key | Required | Description | Example |
| :--- | :---: | :--- | :--- |
| `AMQP_URL` | **Yes** | CloudAMQP connection URL | `amqps://user:pass@beaver.rmq.cloudamqp.com/vhost` |
| `DATABASE_URL` | **Yes** | Postgres database URL | `postgres://user:pass@ep-xxx.neon.tech/Turf_BD` |
| `WORKER_URL` | **Yes** | Deployed URL of `tbd-worker` | `https://tbd-worker.onrender.com` |
| `RELAY_POLL_INTERVAL_SECS` | No | Relay poll interval (default: 5s) | `5` |
| `KEEP_ALIVE_INTERVAL_SECS` | No | Keep-alive ping interval (default: 600s) | `600` |

### For `tbd-worker`
| Variable Key | Required | Description | Example |
| :--- | :---: | :--- | :--- |
| `AMQP_URL` | **Yes** | Same CloudAMQP instance | `amqps://user:pass@beaver.rmq.cloudamqp.com/vhost` |
| `DATABASE_URL` | **Yes** | Same Postgres database (for `processed_jobs`) | `postgres://user:pass@ep-xxx.neon.tech/Turf_BD` |
| `RESEND_API_KEY` | **Yes** | Resend API key | `re_1234567890` |
| `GOTENBERG_URL` | **Yes** | Gotenberg base URL | `https://gotenberg-pdf.onrender.com` |
| `GOTENBERG_USER` | No | Basic auth username for Gotenberg | `admin` |
| `GOTENBERG_PASSWORD` | No | Basic auth password for Gotenberg | `secret123` |
| `RESEND_FROM_EMAIL` | No | Sender address (default: `TBD <onboarding@resend.dev>`) | `TBD <no-reply@yourdomain.com>` |
| `PORT` | No | Health check server port (default: `8080`) | `8080` |
| `AMQP_PREFETCH_COUNT` | No | AMQP QoS prefetch count (default: `1`) | `1` |
| `APP_ENV` | No | Environment (`production` for JSON logging) | `production` |
