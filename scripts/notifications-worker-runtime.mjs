import crypto from "crypto";

export const WORKER_STATUS_MESSAGE_TYPE = "centrix.notifications-worker.status.v1";
export const READY_WORKER_STATES = new Set(["standby-locked", "standby", "leader"]);

const REDACTED = "[REDACTED]";
const SENSITIVE_KEYS = new Set([
  "auth",
  "creds",
  "credentials",
  "databaseurl",
  "database_url",
  "devicepairingdata",
  "helloMsg",
  "host",
  "hostname",
  "jid",
  "key",
  "node",
  "ownerid",
  "password",
  "phone",
  "phonenumber",
  "qr",
  "recipientaddress",
  "recipientphone",
  "remotejid",
  "secret",
  "subscriptionid",
  "tenantid",
  "token"
].map((key) => key.toLowerCase()));

export function parseBoolean(value, defaultValue = false) {
  if (value === undefined || value === null || value === "") {
    return defaultValue;
  }
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function isBooleanLiteral(value) {
  return ["0", "1", "false", "true", "no", "yes", "off", "on"].includes(
    String(value).toLowerCase()
  );
}

function isPositiveNumberLiteral(value) {
  if (value === undefined || value === null || value === "") return true;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0;
}

export function positiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function isValidWhatsAppVersion(value) {
  return Array.isArray(value)
    && value.length === 3
    && value.every((part) => Number.isSafeInteger(part) && part >= 0);
}

export function sameWhatsAppVersion(left, right) {
  return isValidWhatsAppVersion(left)
    && isValidWhatsAppVersion(right)
    && left.every((part, index) => part === right[index]);
}

export async function resolveWhatsAppVersion({
  fetchBaileysVersion,
  fetchWaWebVersion,
  fallbackVersion
}) {
  const attempts = [
    ["baileys-master", fetchBaileysVersion],
    ["whatsapp-web", fetchWaWebVersion]
  ];
  let usableFallback = isValidWhatsAppVersion(fallbackVersion)
    ? [...fallbackVersion]
    : null;
  const errors = [];

  for (const [source, fetchVersion] of attempts) {
    if (typeof fetchVersion !== "function") continue;
    try {
      const result = await fetchVersion();
      if (isValidWhatsAppVersion(result?.version) && result?.isLatest) {
        return {
          version: [...result.version],
          source,
          verified: true,
          errors
        };
      }
      if (!usableFallback && isValidWhatsAppVersion(result?.version)) {
        usableFallback = [...result.version];
      }
      if (result?.error) errors.push(result.error);
    } catch (error) {
      errors.push(error);
    }
  }

  if (usableFallback) {
    return {
      version: usableFallback,
      source: "cached-or-package-default",
      verified: false,
      errors
    };
  }

  const error = new Error("Unable to resolve a usable WhatsApp Web protocol version.");
  error.code = "WHATSAPP_VERSION_UNAVAILABLE";
  error.causes = errors;
  throw error;
}

export function deterministicWhatsAppMessageId(deliveryId) {
  const normalizedId = String(deliveryId || "").trim();
  if (!normalizedId) {
    throw new Error("A delivery id is required to derive a WhatsApp message id.");
  }

  const digest = crypto
    .createHash("sha256")
    .update(`centrix-notification-delivery:v1:${normalizedId}`)
    .digest("hex")
    .toUpperCase();
  return `3EB0${digest.slice(0, 18)}`;
}

export function deliveryOwnershipWhere(delivery) {
  return {
    id: delivery.id,
    status: "PROCESSING",
    lockToken: delivery.lockToken
  };
}

export function shouldReleaseDeliveryOnLeadershipChange(delivery) {
  return !["sending", "acknowledging"].includes(delivery?.processingPhase);
}

export async function executeIdempotentSend({
  deliveryId,
  send,
  persistAcknowledgement,
  handleSendFailure
}) {
  const messageId = deterministicWhatsAppMessageId(deliveryId);
  let result;
  try {
    result = await send(messageId);
  } catch (error) {
    await handleSendFailure(error);
    return { status: "send-failed", messageId, error };
  }

  const providerMessageId = result?.key?.id || messageId;
  try {
    const persisted = await persistAcknowledgement(providerMessageId);
    return {
      status: persisted ? "sent" : "ack-conflict",
      messageId,
      providerMessageId
    };
  } catch (error) {
    return {
      status: "ack-persist-failed",
      messageId,
      providerMessageId,
      error
    };
  }
}

export function validateWorkerConfiguration(env, inferredLabel) {
  const production = env.NODE_ENV === "production";
  const databaseUrl = String(env.DATABASE_URL || "").trim();
  const encryptionKey = String(env.WHATSAPP_AUTH_ENCRYPTION_KEY || "").trim();
  const explicitOwnerLabel = String(env.NOTIFICATIONS_WORKER_INSTANCE_LABEL || "").trim();
  const ownerLabel = explicitOwnerLabel || (production ? "" : String(inferredLabel || "").trim());
  const preferredLabel = String(env.NOTIFICATIONS_WORKER_PREFERRED_LABEL || "").trim();
  const haEnabled = parseBoolean(env.NOTIFICATIONS_WORKER_HA_ENABLED, true);
  const standbyOnly = parseBoolean(env.NOTIFICATIONS_WORKER_STANDBY_ONLY, false);
  const leaseTtlMs = positiveNumber(env.NOTIFICATIONS_WORKER_LEASE_TTL_MS, 45_000);
  const leaseRenewMs = positiveNumber(env.NOTIFICATIONS_WORKER_LEASE_RENEW_MS, 10_000);
  const preemptGraceMs = positiveNumber(
    env.NOTIFICATIONS_WORKER_PREEMPT_GRACE_MS,
    leaseRenewMs + 2_500
  );
  const explicitRole = String(env.NOTIFICATIONS_WORKER_ROLE || "").trim().toLowerCase();
  const role = explicitRole || (ownerLabel && ownerLabel === preferredLabel ? "primary" : "fallback");

  const errors = [];
  if (production && env.DATA_SOURCE !== "prisma") {
    errors.push("DATA_SOURCE must be prisma in production.");
  }
  if (production && !databaseUrl) errors.push("DATABASE_URL is required in production.");
  if (production && !encryptionKey) {
    errors.push("WHATSAPP_AUTH_ENCRYPTION_KEY is required in production.");
  }
  if (production && !haEnabled) {
    errors.push("NOTIFICATIONS_WORKER_HA_ENABLED must be true in production.");
  }
  if (production && !isBooleanLiteral(env.NOTIFICATIONS_WORKER_HA_ENABLED)) {
    errors.push("NOTIFICATIONS_WORKER_HA_ENABLED must be an explicit boolean in production.");
  }
  if (production && !isBooleanLiteral(env.NOTIFICATIONS_WORKER_STANDBY_ONLY)) {
    errors.push("NOTIFICATIONS_WORKER_STANDBY_ONLY must be an explicit boolean in production.");
  }
  if (!ownerLabel) errors.push("NOTIFICATIONS_WORKER_INSTANCE_LABEL is required.");
  if (!preferredLabel) errors.push("NOTIFICATIONS_WORKER_PREFERRED_LABEL is required.");
  if (production && !explicitRole) errors.push("NOTIFICATIONS_WORKER_ROLE is required in production.");
  if (!["primary", "fallback", "manual"].includes(role)) {
    errors.push("NOTIFICATIONS_WORKER_ROLE must be primary, fallback, or manual.");
  }
  if (explicitRole === "primary" && ownerLabel !== preferredLabel) {
    errors.push("The primary worker label must match NOTIFICATIONS_WORKER_PREFERRED_LABEL.");
  }
  if (explicitRole === "fallback" && ownerLabel === preferredLabel) {
    errors.push("The fallback worker must not prefer its own label.");
  }
  if (explicitRole === "primary" && standbyOnly) {
    errors.push("The primary worker cannot run with NOTIFICATIONS_WORKER_STANDBY_ONLY=true.");
  }
  for (const [name, value] of [
    ["NOTIFICATIONS_WORKER_LEASE_TTL_MS", env.NOTIFICATIONS_WORKER_LEASE_TTL_MS],
    ["NOTIFICATIONS_WORKER_LEASE_RENEW_MS", env.NOTIFICATIONS_WORKER_LEASE_RENEW_MS],
    ["NOTIFICATIONS_WORKER_PREEMPT_GRACE_MS", env.NOTIFICATIONS_WORKER_PREEMPT_GRACE_MS]
  ]) {
    if (!isPositiveNumberLiteral(value)) errors.push(`${name} must be a positive number.`);
  }
  if (leaseTtlMs < leaseRenewMs * 3) {
    errors.push("The worker lease TTL must be at least three times the renewal interval.");
  }
  if (preemptGraceMs < leaseRenewMs || preemptGraceMs >= leaseTtlMs) {
    errors.push("The preemption grace must be at least one renewal interval and less than the lease TTL.");
  }

  if (databaseUrl) {
    try {
      const parsed = new URL(databaseUrl);
      if (!["postgres:", "postgresql:"].includes(parsed.protocol)) {
        errors.push("DATABASE_URL must use the postgres or postgresql protocol.");
      }
    } catch {
      errors.push("DATABASE_URL is not a valid URL.");
    }
  }

  if (errors.length) {
    const error = new Error(`Invalid WhatsApp worker configuration: ${errors.join(" ")}`);
    error.code = "WORKER_CONFIG_INVALID";
    throw error;
  }

  return {
    databaseUrl,
    encryptionKey,
    ownerLabel,
    preferredLabel,
    role,
    haEnabled,
    standbyOnly,
    leaseTtlMs,
    leaseRenewMs,
    preemptGraceMs
  };
}

export function sanitizeErrorMessage(value) {
  return String(value || "")
    .replace(/postgres(?:ql)?:\/\/[^\s"']+/gi, "[REDACTED_DATABASE_URL]")
    .replace(/database server at [`'"]?[^\s`'"]+[`'"]?/gi, "database server at [REDACTED_HOST]")
    .replace(/\b\d{8,15}@(?:s\.whatsapp\.net|c\.us)\b/gi, "[REDACTED_JID]");
}

export function serializeSafeError(error) {
  if (!error) return error;
  return {
    type: error.name || "Error",
    message: sanitizeErrorMessage(error.message || error),
    code: error.code || undefined
  };
}

export function redactLogValue(value, seen = new WeakSet()) {
  if (value === null || value === undefined) return value;
  if (value instanceof Error) return serializeSafeError(value);
  if (typeof value === "string") return sanitizeErrorMessage(value);
  if (typeof value !== "object") return value;
  if (seen.has(value)) return "[Circular]";

  seen.add(value);
  if (Array.isArray(value)) {
    return value.map((item) => redactLogValue(item, seen));
  }

  const result = {};
  for (const [key, nestedValue] of Object.entries(value)) {
    if (SENSITIVE_KEYS.has(key.toLowerCase())) {
      result[key] = REDACTED;
    } else {
      result[key] = redactLogValue(nestedValue, seen);
    }
  }
  return result;
}

export function buildHealthSnapshot({
  path,
  startedAt,
  now = new Date(),
  label,
  role,
  release,
  workerExit,
  workerStatus,
  staleAfterMs
}) {
  const live = !workerExit;
  const statusAtMs = workerStatus?.at ? Date.parse(workerStatus.at) : Number.NaN;
  const statusAgeMs = Number.isFinite(statusAtMs) ? Math.max(0, now.getTime() - statusAtMs) : null;
  const statusFresh = statusAgeMs !== null && statusAgeMs <= staleAfterMs;
  const ready = live && statusFresh && READY_WORKER_STATES.has(workerStatus?.state);
  const livenessRequest = path === "/" || path === "/live";
  const ok = livenessRequest ? live : ready;

  return {
    statusCode: ok ? 200 : 503,
    body: {
      ok,
      service: "centrix-whatsapp-worker",
      label,
      role,
      state: workerExit ? "crashed" : workerStatus?.state || "starting",
      startedAt: startedAt.toISOString(),
      statusUpdatedAt: workerStatus?.at || null,
      statusAgeMs,
      release: release || null
    }
  };
}

export function createMemoryLeaseStore(initialLease = null) {
  let lease = initialLease ? { ...initialLease } : null;
  return {
    async find() {
      return lease ? { ...lease } : null;
    },
    async create(value) {
      if (lease) {
        const error = new Error("Unique constraint failed");
        error.code = "P2002";
        throw error;
      }
      lease = { ...value };
    },
    async compareAndSet(predicate, value) {
      if (!lease || !predicate(lease)) return false;
      lease = { ...lease, ...value };
      return true;
    },
    async deleteIf(predicate) {
      if (!lease || !predicate(lease)) return false;
      lease = null;
      return true;
    },
    snapshot() {
      return lease ? { ...lease } : null;
    }
  };
}

export async function acquireLease({ store, leaseId, ownerId, ownerLabel, preferredLabel, ttlMs, now, capabilities }) {
  const existing = await store.find(leaseId);
  const ownershipData = {
    id: leaseId,
    ownerId,
    ownerLabel,
    heartbeatAt: now,
    expiresAt: new Date(now.getTime() + ttlMs),
    startedAt: now,
    capabilities
  };

  if (!existing) {
    try {
      await store.create(ownershipData);
      return { acquired: true, reason: "created" };
    } catch (error) {
      if (error?.code !== "P2002") throw error;
      return { acquired: false, reason: "create-raced" };
    }
  }

  const expired = new Date(existing.expiresAt).getTime() <= now.getTime();
  const ownedByThisWorker = existing.ownerId === ownerId;
  const preferredPreemption = (
    ownerLabel === preferredLabel
    && existing.ownerLabel !== preferredLabel
    && !expired
  );

  if (!expired && !ownedByThisWorker && !preferredPreemption) {
    return { acquired: false, reason: "held", ownerLabel: existing.ownerLabel };
  }

  const updated = await store.compareAndSet(
    (current) => (
      current.id === leaseId
      && (
        current.ownerId === ownerId
        || new Date(current.expiresAt).getTime() <= now.getTime()
        || (preferredPreemption && current.ownerLabel !== preferredLabel)
      )
    ),
    ownedByThisWorker && !expired
      ? { heartbeatAt: now, expiresAt: ownershipData.expiresAt, capabilities }
      : ownershipData
  );

  if (!updated) return { acquired: false, reason: "update-raced" };
  return {
    acquired: true,
    reason: preferredPreemption ? "preferred-preempted" : expired ? "expired" : ownedByThisWorker ? "renewed" : "expired"
  };
}

export async function renewLease({ store, leaseId, ownerId, ttlMs, now, capabilities }) {
  const expiresAt = new Date(now.getTime() + ttlMs);
  return store.compareAndSet(
    (current) => (
      current.id === leaseId
      && current.ownerId === ownerId
      && new Date(current.expiresAt).getTime() > now.getTime()
    ),
    { heartbeatAt: now, expiresAt, capabilities }
  );
}

export function createLeaseHeartbeatScheduler({ intervalMs, heartbeat, onError = () => undefined }) {
  let running = false;
  let stopped = false;
  const tick = async () => {
    if (running || stopped) return false;
    running = true;
    try {
      await heartbeat();
      return true;
    } catch (error) {
      await onError(error);
      return false;
    } finally {
      running = false;
    }
  };
  const timer = setInterval(() => { void tick(); }, intervalMs);
  return {
    tick,
    stop() {
      stopped = true;
      clearInterval(timer);
    }
  };
}
