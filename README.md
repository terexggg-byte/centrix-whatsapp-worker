# Centrix WhatsApp Worker

Public GitHub Actions fallback worker for Centrix WhatsApp notifications.

Required repository secrets:

- `DATABASE_URL`
- `WHATSAPP_AUTH_ENCRYPTION_KEY`

The workflow runs a long-lived worker on GitHub Actions and uses the PostgreSQL `NotificationWorkerLease` lock so only one worker is active at a time.

This is a free fallback, not a guaranteed VPS replacement. GitHub scheduled workflows can be delayed, and GitHub-hosted runners are not intended to provide a strict uptime SLA.
