# tbd-worker — Background Job Consumer

A standalone Rust binary that consumes events from CloudAMQP (RabbitMQ) and handles:
- **Email delivery** via [Resend](https://resend.com) (`email_jobs` queue)
- **PDF generation** via [Gotenberg](https://gotenberg.dev) (`pdf_jobs` queue)

Part of the **TBD Turf Booking** backend architecture.  
See `tbd-backend` for the outbox relay that publishes events to the queues.

## Project Structure

```
src/
├── main.rs               — Boot tracing, load env vars, start consumers
├── config.rs             — WorkerConfig struct (shared across tasks)
└── consumers/
    ├── mod.rs            — Connect to AMQP, spawn email + pdf consumer tasks
    ├── email.rs          — Consumes 'email_jobs' → calls Resend API
    └── pdf.rs            — Consumes 'pdf_jobs'   → calls Gotenberg API
```

## Environment Variables

| Variable | Description |
|---|---|
| `AMQP_URL` | CloudAMQP connection URL (`amqps://...`) |
| `RESEND_API_KEY` | Resend API key (`re_xxxx`) |
| `GOTENBERG_URL` | Gotenberg base URL (`http://gotenberg:3000`) |
| `APP_ENV` | `production` for JSON logs, anything else for human-readable logs |
| `RUST_LOG` | Log filter (default: `info`) |

## Running Locally

```bash
cp .env.example .env
# Fill in your values
cargo run
```

## Deployment (Render)

Deploy as a **Background Worker** service pointing to this repository.
Set all environment variables listed above in the Render dashboard.
