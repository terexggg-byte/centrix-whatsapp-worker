import test from "node:test";
import assert from "node:assert/strict";
import {
  acquireLease,
  buildHealthSnapshot,
  createMemoryLeaseStore,
  deliveryOwnershipWhere,
  deterministicWhatsAppMessageId,
  executeIdempotentSend,
  isValidWhatsAppVersion,
  redactLogValue,
  resolveWhatsAppVersion,
  sameWhatsAppVersion,
  shouldReleaseDeliveryOnLeadershipChange,
  validateWorkerConfiguration
} from "../scripts/notifications-worker-runtime.mjs";
import { buildNotificationWorkerHealth } from "../lib/server/notification-worker-health.mjs";

const validProductionEnv = {
  NODE_ENV: "production",
  DATA_SOURCE: "prisma",
  DATABASE_URL: "postgresql://worker:password@db.example.test:5432/centrix?sslmode=require",
  WHATSAPP_AUTH_ENCRYPTION_KEY: "existing-production-key-placeholder",
  NOTIFICATIONS_WORKER_HA_ENABLED: "true",
  NOTIFICATIONS_WORKER_STANDBY_ONLY: "false",
  NOTIFICATIONS_WORKER_ROLE: "fallback",
  NOTIFICATIONS_WORKER_INSTANCE_LABEL: "back4app-fallback",
  NOTIFICATIONS_WORKER_PREFERRED_LABEL: "back4app-primary",
  NOTIFICATIONS_WORKER_LEASE_TTL_MS: "45000",
  NOTIFICATIONS_WORKER_LEASE_RENEW_MS: "10000",
  NOTIFICATIONS_WORKER_PREEMPT_GRACE_MS: "12500"
};

test("production configuration is fail-closed", () => {
  assert.throws(
    () => validateWorkerConfiguration({ ...validProductionEnv, WHATSAPP_AUTH_ENCRYPTION_KEY: "" }),
    /WHATSAPP_AUTH_ENCRYPTION_KEY/
  );
  assert.throws(
    () => validateWorkerConfiguration({ ...validProductionEnv, NOTIFICATIONS_WORKER_HA_ENABLED: "false" }),
    /must be true/
  );
  assert.throws(
    () => validateWorkerConfiguration({ ...validProductionEnv, NOTIFICATIONS_WORKER_INSTANCE_LABEL: "" }, "local"),
    /INSTANCE_LABEL/
  );
  assert.throws(
    () => validateWorkerConfiguration({ ...validProductionEnv, NOTIFICATIONS_WORKER_ROLE: "" }),
    /ROLE is required/
  );
  assert.throws(
    () => validateWorkerConfiguration({ ...validProductionEnv, NOTIFICATIONS_WORKER_STANDBY_ONLY: "treu" }),
    /explicit boolean/
  );
  assert.throws(
    () => validateWorkerConfiguration({ ...validProductionEnv, NOTIFICATIONS_WORKER_LEASE_RENEW_MS: "0" }),
    /positive number/
  );
  assert.throws(
    () => validateWorkerConfiguration({
      ...validProductionEnv,
      NOTIFICATIONS_WORKER_INSTANCE_LABEL: "back4app-primary"
    }),
    /must not prefer its own label/
  );
  assert.throws(
    () => validateWorkerConfiguration({
      ...validProductionEnv,
      NOTIFICATIONS_WORKER_LEASE_TTL_MS: "20000"
    }),
    /at least three times/
  );

  const config = validateWorkerConfiguration({
    ...validProductionEnv,
    NOTIFICATIONS_WORKER_STANDBY_ONLY: "true"
  });
  assert.equal(config.standbyOnly, true);
  assert.equal(config.role, "fallback");

  assert.throws(
    () => validateWorkerConfiguration({
      ...validProductionEnv,
      NOTIFICATIONS_WORKER_ROLE: "primary",
      NOTIFICATIONS_WORKER_INSTANCE_LABEL: "back4app-primary",
      NOTIFICATIONS_WORKER_STANDBY_ONLY: "true"
    }),
    /primary worker cannot run/
  );
});

test("WhatsApp protocol version resolution prefers the maintained Baileys version", async () => {
  const resolved = await resolveWhatsAppVersion({
    fetchBaileysVersion: async () => ({ version: [2, 3000, 1043857760], isLatest: true }),
    fetchWaWebVersion: async () => ({ version: [2, 3000, 1044300879], isLatest: true }),
    fallbackVersion: [2, 3000, 1035194821]
  });

  assert.deepEqual(resolved.version, [2, 3000, 1043857760]);
  assert.equal(resolved.source, "baileys-master");
  assert.equal(resolved.verified, true);
  assert.equal(isValidWhatsAppVersion(resolved.version), true);
  assert.equal(sameWhatsAppVersion(resolved.version, [2, 3000, 1043857760]), true);
});

test("WhatsApp protocol version resolution falls back without inventing a version", async () => {
  const webFallback = await resolveWhatsAppVersion({
    fetchBaileysVersion: async () => ({
      version: [2, 3000, 1035194821],
      isLatest: false,
      error: new Error("upstream unavailable")
    }),
    fetchWaWebVersion: async () => ({ version: [2, 3000, 1044300879], isLatest: true })
  });
  assert.deepEqual(webFallback.version, [2, 3000, 1044300879]);
  assert.equal(webFallback.source, "whatsapp-web");
  assert.equal(webFallback.verified, true);

  const cachedFallback = await resolveWhatsAppVersion({
    fetchBaileysVersion: async () => ({ version: null, isLatest: false }),
    fetchWaWebVersion: async () => {
      throw new Error("network unavailable");
    },
    fallbackVersion: [2, 3000, 1043857760]
  });
  assert.deepEqual(cachedFallback.version, [2, 3000, 1043857760]);
  assert.equal(cachedFallback.verified, false);
  assert.equal(cachedFallback.source, "cached-or-package-default");
});

test("WhatsApp message ids are deterministic, distinct, and Baileys-compatible", () => {
  const first = deterministicWhatsAppMessageId("delivery-1");
  assert.equal(first, deterministicWhatsAppMessageId("delivery-1"));
  assert.notEqual(first, deterministicWhatsAppMessageId("delivery-2"));
  assert.match(first, /^3EB0[A-F0-9]{18}$/);
});

test("a database acknowledgement failure never invokes the send retry handler", async () => {
  let retryCalls = 0;
  const sentIds = [];
  const outcome = await executeIdempotentSend({
    deliveryId: "delivery-ack-gap",
    send: async (messageId) => {
      sentIds.push(messageId);
      return { key: { id: messageId } };
    },
    persistAcknowledgement: async () => {
      throw new Error("database unavailable after provider acknowledgement");
    },
    handleSendFailure: async () => {
      retryCalls += 1;
    }
  });

  assert.equal(outcome.status, "ack-persist-failed");
  assert.equal(retryCalls, 0);
  assert.deepEqual(sentIds, [deterministicWhatsAppMessageId("delivery-ack-gap")]);
});

test("a WhatsApp send failure follows the existing retry handler", async () => {
  let retryError = null;
  const outcome = await executeIdempotentSend({
    deliveryId: "delivery-send-failure",
    send: async () => {
      throw new Error("provider rejected send");
    },
    persistAcknowledgement: async () => true,
    handleSendFailure: async (error) => {
      retryError = error;
    }
  });

  assert.equal(outcome.status, "send-failed");
  assert.match(retryError.message, /provider rejected/);
});

test("a stale worker cannot acknowledge a delivery after its lock token changes", async () => {
  const original = { id: "delivery-lock", lockToken: "old-lock" };
  assert.deepEqual(deliveryOwnershipWhere(original), {
    id: "delivery-lock",
    status: "PROCESSING",
    lockToken: "old-lock"
  });

  const currentRow = { id: "delivery-lock", status: "PROCESSING", lockToken: "new-lock" };
  const where = deliveryOwnershipWhere(original);
  const stillOwned = Object.entries(where).every(([key, value]) => currentRow[key] === value);
  assert.equal(stillOwned, false);

  const outcome = await executeIdempotentSend({
    deliveryId: original.id,
    send: async (messageId) => ({ key: { id: messageId } }),
    persistAcknowledgement: async () => stillOwned,
    handleSendFailure: async () => undefined
  });
  assert.equal(outcome.status, "ack-conflict");
});

test("leadership loss never requeues a delivery with an unresolved provider outcome", () => {
  assert.equal(shouldReleaseDeliveryOnLeadershipChange({ processingPhase: "claimed" }), true);
  assert.equal(shouldReleaseDeliveryOnLeadershipChange({ processingPhase: "retrying" }), true);
  assert.equal(shouldReleaseDeliveryOnLeadershipChange({ processingPhase: "sending" }), false);
  assert.equal(shouldReleaseDeliveryOnLeadershipChange({ processingPhase: "acknowledging" }), false);
});

test("the database lease allows one leader and controlled preferred restoration", async () => {
  const now = new Date("2026-08-01T00:00:00.000Z");
  const store = createMemoryLeaseStore();
  const fallback = await acquireLease({
    store,
    leaseId: "notifications-worker",
    ownerId: "fallback-instance",
    ownerLabel: "back4app-fallback",
    preferredLabel: "back4app-primary",
    ttlMs: 45_000,
    now
  });
  assert.deepEqual(fallback, { acquired: true, reason: "created" });

  const competingFallback = await acquireLease({
    store,
    leaseId: "notifications-worker",
    ownerId: "other-fallback",
    ownerLabel: "back4app-fallback",
    preferredLabel: "back4app-primary",
    ttlMs: 45_000,
    now: new Date(now.getTime() + 1_000)
  });
  assert.equal(competingFallback.acquired, false);
  assert.equal(store.snapshot().ownerId, "fallback-instance");

  const primary = await acquireLease({
    store,
    leaseId: "notifications-worker",
    ownerId: "primary-instance",
    ownerLabel: "back4app-primary",
    preferredLabel: "back4app-primary",
    ttlMs: 45_000,
    now: new Date(now.getTime() + 2_000)
  });
  assert.deepEqual(primary, { acquired: true, reason: "preferred-preempted" });
  assert.equal(store.snapshot().ownerId, "primary-instance");

  const fallbackCannotPreempt = await acquireLease({
    store,
    leaseId: "notifications-worker",
    ownerId: "fallback-instance",
    ownerLabel: "back4app-fallback",
    preferredLabel: "back4app-primary",
    ttlMs: 45_000,
    now: new Date(now.getTime() + 3_000)
  });
  assert.equal(fallbackCannotPreempt.acquired, false);
  assert.equal(store.snapshot().ownerId, "primary-instance");
});

test("an expired lease can be acquired by the fallback", async () => {
  const expiredAt = new Date("2026-08-01T00:00:00.000Z");
  const store = createMemoryLeaseStore({
    id: "notifications-worker",
    ownerId: "dead-primary",
    ownerLabel: "back4app-primary",
    heartbeatAt: new Date(expiredAt.getTime() - 45_000),
    expiresAt: expiredAt,
    startedAt: new Date(expiredAt.getTime() - 60_000)
  });
  const takeover = await acquireLease({
    store,
    leaseId: "notifications-worker",
    ownerId: "fallback-instance",
    ownerLabel: "back4app-fallback",
    preferredLabel: "back4app-primary",
    ttlMs: 45_000,
    now: new Date(expiredAt.getTime() + 1)
  });
  assert.deepEqual(takeover, { acquired: true, reason: "expired" });
});

test("health snapshots distinguish liveness, readiness, staleness, and standby lock", () => {
  const startedAt = new Date("2026-08-01T00:00:00.000Z");
  const statusAt = new Date("2026-08-01T00:00:10.000Z");
  const base = {
    startedAt,
    now: new Date("2026-08-01T00:00:20.000Z"),
    label: "back4app-fallback",
    role: "fallback",
    release: "test-release",
    workerExit: null,
    workerStatus: { state: "standby-locked", at: statusAt.toISOString() },
    staleAfterMs: 30_000
  };

  const ready = buildHealthSnapshot({ ...base, path: "/ready" });
  assert.equal(ready.statusCode, 200);
  assert.equal(ready.body.state, "standby-locked");

  const stale = buildHealthSnapshot({
    ...base,
    path: "/ready",
    now: new Date("2026-08-01T00:01:00.000Z")
  });
  assert.equal(stale.statusCode, 503);

  const live = buildHealthSnapshot({
    ...base,
    path: "/live",
    now: new Date("2026-08-01T00:01:00.000Z")
  });
  assert.equal(live.statusCode, 200);
});

test("the protected application health check reports leader and stale-lock state without identifiers", () => {
  const now = new Date("2026-08-02T12:00:00.000Z");
  const healthy = buildNotificationWorkerHealth({
    lease: {
      heartbeatAt: new Date(now.getTime() - 5_000),
      expiresAt: new Date(now.getTime() + 40_000)
    },
    staleProcessingCount: 0,
    now,
    leaseTtlMs: 45_000
  });
  assert.deepEqual(healthy, {
    ok: true,
    state: "leader-active",
    leaderActive: true,
    heartbeatAgeMs: 5_000,
    leaseExpiresInMs: 40_000,
    staleProcessingDetected: false,
    staleProcessingCount: 0
  });
  assert.equal("ownerId" in healthy, false);
  assert.equal("ownerLabel" in healthy, false);

  const stale = buildNotificationWorkerHealth({
    lease: {
      heartbeatAt: new Date(now.getTime() - 5_000),
      expiresAt: new Date(now.getTime() + 40_000)
    },
    staleProcessingCount: 2,
    now,
    leaseTtlMs: 45_000
  });
  assert.equal(stale.ok, false);
  assert.equal(stale.state, "stale-processing");

  const missing = buildNotificationWorkerHealth({
    lease: null,
    staleProcessingCount: 0,
    now,
    leaseTtlMs: 45_000
  });
  assert.equal(missing.ok, false);
  assert.equal(missing.state, "leader-missing");

  const staleHeartbeat = buildNotificationWorkerHealth({
    lease: {
      heartbeatAt: new Date(now.getTime() - 50_000),
      expiresAt: new Date(now.getTime() + 5_000)
    },
    staleProcessingCount: 0,
    now,
    leaseTtlMs: 45_000
  });
  assert.equal(staleHeartbeat.ok, false);
  assert.equal(staleHeartbeat.state, "leader-missing");
});

test("log redaction removes credentials, connection URLs, phones, JIDs, and protocol nodes", () => {
  const secretUrl = "postgresql://user:super-secret@private-db.example.test:5432/centrix";
  const safe = redactLogValue({
    databaseUrl: secretUrl,
    phoneNumber: "201234567890",
    remoteJid: "201234567890@s.whatsapp.net",
    tenantId: "tenant-private-id",
    ownerId: "worker-private-id",
    node: { attrs: { id: "device-secret" } },
    error: new Error(`Cannot reach database server at private-db.example.test:5432 via ${secretUrl}`),
    event: "db.degraded"
  });
  const serialized = JSON.stringify(safe);

  assert.doesNotMatch(serialized, /super-secret/);
  assert.doesNotMatch(serialized, /private-db\.example\.test/);
  assert.doesNotMatch(serialized, /201234567890/);
  assert.doesNotMatch(serialized, /tenant-private-id/);
  assert.doesNotMatch(serialized, /worker-private-id/);
  assert.doesNotMatch(serialized, /device-secret/);
  assert.match(serialized, /db\.degraded/);
});
