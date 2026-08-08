import crypto from "crypto";
import nextEnv from "@next/env";
import { PrismaClient } from "@prisma/client";
import makeWASocket, {
  BufferJSON,
  DisconnectReason,
  fetchLatestBaileysVersion,
  fetchLatestWaWebVersion,
  initAuthCreds,
  proto
} from "baileys";
import pino from "pino";
import QRCode from "qrcode";
import { normalizeWorkerEnv } from "./notifications-worker-env.mjs";
import {
  WORKER_STATUS_MESSAGE_TYPE,
  createLeaseHeartbeatScheduler,
  deliveryOwnershipWhere,
  executeIdempotentSend,
  parseBoolean,
  positiveNumber,
  redactLogValue,
  resolveWhatsAppVersion,
  sameWhatsAppVersion,
  sanitizeErrorMessage,
  serializeSafeError,
  shouldReleaseDeliveryOnLeadershipChange,
  validateWorkerConfiguration
} from "./notifications-worker-runtime.mjs";

nextEnv.loadEnvConfig(process.cwd(), process.env.NODE_ENV !== "production");
normalizeWorkerEnv();

const prisma = new PrismaClient();
const inferredWorkerLabel = inferWorkerLabel();
const workerConfig = validateWorkerConfiguration(process.env, inferredWorkerLabel);
const logger = pino({
  level: process.env.NOTIFICATIONS_WORKER_LOG_LEVEL || "info",
  serializers: {
    err: serializeSafeError,
    error: serializeSafeError
  },
  hooks: {
    logMethod(args, method) {
      const sanitizedArgs = args.map((arg) => (
        arg && typeof arg === "object" ? redactLogValue(arg) : arg
      ));
      return method.apply(this, sanitizedArgs);
    }
  },
  redact: {
    paths: [
      "auth", "creds", "credentials", "databaseUrl", "DATABASE_URL", "devicePairingData",
      "helloMsg", "host", "hostname", "jid", "key", "node", "password", "phone",
      "phoneNumber", "qr", "recipientAddress", "recipientPhone", "remoteJid", "secret", "token",
      "*.auth", "*.creds", "*.credentials", "*.databaseUrl", "*.DATABASE_URL", "*.devicePairingData",
      "*.helloMsg", "*.host", "*.hostname", "*.jid", "*.key", "*.node", "*.password", "*.phone",
      "*.phoneNumber", "*.qr", "*.recipientAddress", "*.recipientPhone", "*.remoteJid", "*.secret", "*.token"
    ],
    censor: "[REDACTED]"
  }
});
const baileysLogger = logger.child({ component: "baileys" }, {
  level: process.env.NOTIFICATIONS_WORKER_BAILEYS_LOG_LEVEL || "warn"
});
const sockets = new Map();
const connecting = new Set();
const pendingCredentialSaves = new Map();
const reconnectTimers = new Map();
const reconnectAttempts = new Map();
const intentionallyEndingSockets = new Set();
const leaderTimers = new Set();
const activeDeliveries = new Map();
const activeCardExportJobs = new Map();
let cardExportQueueInFlight = false;
const healthcheckLastPingAt = new Map();
let shuttingDown = false;
let leaderActive = false;
let leaseTimer = null;
let leaseCycleRunning = false;
let leaderAcquiredAt = null;
let staleLockScanAt = 0;
let whatsAppVersionState = null;
let whatsAppVersionRefreshPromise = null;
let whatsAppVersionForcedAt = 0;

const POLL_INTERVAL_MS = Number(process.env.NOTIFICATIONS_WORKER_POLL_MS || 5000);
const CARD_EXPORT_WORKER_ENABLED = parseBoolean(process.env.CARD_EXPORT_WORKER_ENABLED, false);
const CARD_EXPORT_WORKER_ONLY = parseBoolean(process.env.NOTIFICATIONS_WORKER_CARD_EXPORT_ONLY, false);
const CARD_EXPORT_WORKER_POLL_MS = positiveNumber(process.env.CARD_EXPORT_WORKER_POLL_MS, 5_000);
const CARD_EXPORT_WORKER_STALE_LOCK_MS = positiveNumber(process.env.CARD_EXPORT_WORKER_STALE_LOCK_MS, 10 * 60 * 1000);
const CARD_EXPORT_WORKER_REQUEST_TIMEOUT_MS = positiveNumber(
  process.env.CARD_EXPORT_WORKER_REQUEST_TIMEOUT_MS,
  4 * 60 * 1000
);
const CARD_EXPORT_APP_BASE_URL = String(process.env.CARD_EXPORT_APP_BASE_URL || "").trim().replace(/\/$/, "");
const CARD_EXPORT_WORKER_SECRET = String(process.env.CARD_EXPORT_WORKER_SECRET || "").trim();
const SESSION_POLL_INTERVAL_MS = Number(process.env.NOTIFICATIONS_WORKER_SESSION_POLL_MS || 4000);
const SUBSCRIPTION_SCAN_INTERVAL_MS = Number(process.env.NOTIFICATIONS_SUBSCRIPTION_SCAN_MS || 60 * 60 * 1000);
const SUBSCRIPTION_EXPIRING_DAYS = Number(process.env.NOTIFICATIONS_SUBSCRIPTION_EXPIRING_DAYS || 7);
const WHATSAPP_RECONNECT_BASE_MS = Number(process.env.NOTIFICATIONS_WHATSAPP_RECONNECT_BASE_MS || 1500);
const WHATSAPP_RECONNECT_MAX_MS = Number(process.env.NOTIFICATIONS_WHATSAPP_RECONNECT_MAX_MS || 30000);
const WHATSAPP_VERSION_REFRESH_MS = positiveNumber(
  process.env.NOTIFICATIONS_WHATSAPP_VERSION_REFRESH_MS,
  6 * 60 * 60 * 1000
);
const WHATSAPP_VERSION_FORCE_COOLDOWN_MS = positiveNumber(
  process.env.NOTIFICATIONS_WHATSAPP_VERSION_FORCE_COOLDOWN_MS,
  5 * 60 * 1000
);
const WHATSAPP_VERSION_FETCH_TIMEOUT_MS = positiveNumber(
  process.env.NOTIFICATIONS_WHATSAPP_VERSION_FETCH_TIMEOUT_MS,
  5 * 1000
);
const WORKER_LEASE_ID = process.env.NOTIFICATIONS_WORKER_LEASE_ID || "notifications-worker";
const WORKER_OWNER_ID = process.env.NOTIFICATIONS_WORKER_INSTANCE_ID || crypto.randomUUID();
const WORKER_OWNER_LABEL = workerConfig.ownerLabel;
const PREFERRED_WORKER_LABEL = workerConfig.preferredLabel;
const WORKER_ROLE = workerConfig.role;
const HA_ENABLED = workerConfig.haEnabled;
const STANDBY_ONLY = workerConfig.standbyOnly;
const WORKER_LEASE_TTL_MS = workerConfig.leaseTtlMs;
const WORKER_LEASE_RENEW_MS = workerConfig.leaseRenewMs;
const WORKER_PREEMPT_GRACE_MS = workerConfig.preemptGraceMs;
const STALE_PROCESSING_THRESHOLD_MS = positiveNumber(
  process.env.NOTIFICATIONS_WORKER_STALE_PROCESSING_ALERT_MS,
  10 * 60 * 1000
);
const STALE_PROCESSING_SCAN_MS = positiveNumber(
  process.env.NOTIFICATIONS_WORKER_STALE_PROCESSING_SCAN_MS,
  5 * 60 * 1000
);
const HEALTHCHECK_PING_INTERVAL_MS = positiveNumber(
  process.env.NOTIFICATIONS_WORKER_HEALTHCHECK_PING_MS,
  60 * 1000
);
const HEALTHCHECK_TIMEOUT_MS = positiveNumber(
  process.env.NOTIFICATIONS_WORKER_HEALTHCHECK_TIMEOUT_MS,
  5 * 1000
);

async function withTimeout(operation, timeoutMs, label) {
  let timer;
  try {
    return await Promise.race([
      operation(),
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          const error = new Error(`${label} timed out.`);
          error.code = "WHATSAPP_VERSION_FETCH_TIMEOUT";
          reject(error);
        }, timeoutMs);
      })
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function getWhatsAppProtocolVersion({ force = false } = {}) {
  const now = Date.now();
  const cachedIsFresh = whatsAppVersionState
    && now - whatsAppVersionState.resolvedAt < WHATSAPP_VERSION_REFRESH_MS;
  const forceIsCoolingDown = force
    && whatsAppVersionState
    && now - whatsAppVersionForcedAt < WHATSAPP_VERSION_FORCE_COOLDOWN_MS;

  if ((!force && cachedIsFresh) || forceIsCoolingDown) {
    return whatsAppVersionState;
  }
  if (whatsAppVersionRefreshPromise) {
    return whatsAppVersionRefreshPromise;
  }
  if (force) whatsAppVersionForcedAt = now;

  whatsAppVersionRefreshPromise = (async () => {
    const resolved = await resolveWhatsAppVersion({
      fetchBaileysVersion: () => withTimeout(
        () => fetchLatestBaileysVersion(),
        WHATSAPP_VERSION_FETCH_TIMEOUT_MS,
        "Baileys protocol version lookup"
      ),
      fetchWaWebVersion: () => withTimeout(
        () => fetchLatestWaWebVersion({ signal: AbortSignal.timeout(WHATSAPP_VERSION_FETCH_TIMEOUT_MS) }),
        WHATSAPP_VERSION_FETCH_TIMEOUT_MS,
        "WhatsApp Web protocol version lookup"
      ),
      fallbackVersion: whatsAppVersionState?.version
    });
    const nextState = {
      version: resolved.version,
      source: resolved.source,
      verified: resolved.verified,
      resolvedAt: Date.now()
    };
    whatsAppVersionState = nextState;

    const log = resolved.verified ? logger.info.bind(logger) : logger.warn.bind(logger);
    log({
      event: resolved.verified ? "whatsapp.version_resolved" : "whatsapp.version_degraded",
      source: resolved.source,
      verified: resolved.verified,
      revision: resolved.version[2],
      resolutionErrors: resolved.errors.length
    }, resolved.verified
      ? "resolved current WhatsApp Web protocol version"
      : "using cached or package WhatsApp Web protocol version");
    return nextState;
  })();

  try {
    return await whatsAppVersionRefreshPromise;
  } finally {
    whatsAppVersionRefreshPromise = null;
  }
}

const DEFAULT_EVENT_TEMPLATES = {
  SUBSCRIPTION_EXPIRING:
    "اشتراك الطالب {{studentName}} يقترب من الانتهاء بتاريخ {{date}}. المبلغ المستحق: {{amount}}."
};

function inferWorkerLabel() {
  if (process.env.NOTIFICATIONS_WORKER_INSTANCE_LABEL) {
    return process.env.NOTIFICATIONS_WORKER_INSTANCE_LABEL;
  }
  if (process.env.RAILWAY_SERVICE_ID || process.env.RAILWAY_ENVIRONMENT_ID || process.env.RAILWAY_PROJECT_ID) {
    return "railway";
  }
  if (process.env.NF_SERVICE_ID || process.env.NORTHFLANK_SERVICE_ID || process.env.NORTHFLANK_PROJECT_ID) {
    return "northflank";
  }
  if (process.env.GITHUB_ACTIONS) {
    return "github-actions";
  }
  if (process.env.SPACE_ID || process.env.SPACE_HOST) {
    return "huggingface";
  }
  return "local";
}

function getEncryptionKey() {
  const raw = workerConfig.encryptionKey;

  if (!raw && process.env.NODE_ENV === "production") {
    throw new Error("Set WHATSAPP_AUTH_ENCRYPTION_KEY before running the notification worker in production.");
  }

  return crypto.createHash("sha256").update(raw || "centrix-local-notification-worker-key").digest();
}

const encryptionKey = getEncryptionKey();

function encryptAuthValue(value) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", encryptionKey, iv);
  const encrypted = Buffer.concat([cipher.update(value, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `v1:${iv.toString("base64")}:${tag.toString("base64")}:${encrypted.toString("base64")}`;
}

function decryptAuthValue(value) {
  if (!value.startsWith("v1:")) {
    return value;
  }

  const [, ivRaw, tagRaw, encryptedRaw] = value.split(":");
  const decipher = crypto.createDecipheriv("aes-256-gcm", encryptionKey, Buffer.from(ivRaw, "base64"));
  decipher.setAuthTag(Buffer.from(tagRaw, "base64"));
  return Buffer.concat([
    decipher.update(Buffer.from(encryptedRaw, "base64")),
    decipher.final()
  ]).toString("utf8");
}

function serializeAuthValue(value) {
  return JSON.stringify(value, BufferJSON.replacer);
}

function deserializeAuthValue(value) {
  return JSON.parse(value, BufferJSON.reviver);
}

function normalizeAuthStateKey(key) {
  return key?.replace(/\//g, "__")?.replace(/:/g, "-");
}

function authStateKey(type, id) {
  return normalizeAuthStateKey(`${type}-${id}.json`);
}

async function readAuthStateEntry(tenantId, key, fallbackKey) {
  const row = await prisma.whatsAppAuthState.findUnique({
    where: {
      tenantId_key: {
        tenantId,
        key
      }
    }
  });
  const fallbackRow = !row && fallbackKey
    ? await prisma.whatsAppAuthState.findUnique({
        where: {
          tenantId_key: {
            tenantId,
            key: fallbackKey
          }
        }
      })
    : null;
  const selectedRow = row || fallbackRow;

  if (!selectedRow) {
    return { status: "missing", value: null };
  }

  try {
    return {
      status: "found",
      value: deserializeAuthValue(decryptAuthValue(selectedRow.encryptedValue)),
      key: selectedRow.key
    };
  } catch (error) {
    logger.error({ error, tenantId, key: selectedRow.key }, "stored WhatsApp auth state is corrupt");
    return { status: "corrupt", value: null, key: selectedRow.key, error };
  }
}

async function readAuthStateValue(tenantId, key, fallbackKey) {
  const entry = await readAuthStateEntry(tenantId, key, fallbackKey);
  if (entry.status === "corrupt") {
    throw entry.error || new Error(`Corrupt WhatsApp auth state for ${key}`);
  }
  return entry.value;
}

async function writeAuthStateValue(tenantId, key, value) {
  await prisma.whatsAppAuthState.upsert({
    where: {
      tenantId_key: {
        tenantId,
        key
      }
    },
    update: {
      encryptedValue: encryptAuthValue(serializeAuthValue(value))
    },
    create: {
      tenantId,
      key,
      encryptedValue: encryptAuthValue(serializeAuthValue(value))
    }
  });
}

async function deleteAuthStateValue(tenantId, key) {
  await prisma.whatsAppAuthState.deleteMany({
    where: { tenantId, key }
  });
}

async function deleteAuthStateKeys(tenantId, keys) {
  await prisma.whatsAppAuthState.deleteMany({
    where: {
      tenantId,
      key: { in: [...new Set(keys.filter(Boolean))] }
    }
  });
}

function queueCredentialSave(tenantId, save) {
  const previous = pendingCredentialSaves.get(tenantId) || Promise.resolve();
  const next = previous.catch(() => undefined).then(save);
  pendingCredentialSaves.set(tenantId, next);
  next.finally(() => {
    if (pendingCredentialSaves.get(tenantId) === next) {
      pendingCredentialSaves.delete(tenantId);
    }
  }).catch(() => undefined);
  return next;
}

async function waitForCredentialSave(tenantId) {
  const pending = pendingCredentialSaves.get(tenantId);
  if (!pending) {
    return;
  }

  await pending.catch((error) => {
    logger.error({ error, tenantId }, "pending WhatsApp credential save failed");
  });
}

async function countAuthRows(tenantId) {
  return prisma.whatsAppAuthState.count({ where: { tenantId } }).catch(() => 0);
}

async function validateRuntimeReadiness() {
  await prisma.notificationWorkerLease.findUnique({
    where: { id: WORKER_LEASE_ID },
    select: { id: true }
  });
  const encryptedAuthState = CARD_EXPORT_WORKER_ONLY ? null : await prisma.whatsAppAuthState.findFirst({
    select: { encryptedValue: true }
  });
  if (!CARD_EXPORT_WORKER_ONLY && encryptedAuthState?.encryptedValue) {
    deserializeAuthValue(decryptAuthValue(encryptedAuthState.encryptedValue));
  }
  if (CARD_EXPORT_WORKER_ENABLED) {
    if (!CARD_EXPORT_APP_BASE_URL || !CARD_EXPORT_WORKER_SECRET) {
      throw new Error("CARD_EXPORT_APP_BASE_URL and CARD_EXPORT_WORKER_SECRET are required when card export worker is enabled.");
    }
    const appUrl = new URL(CARD_EXPORT_APP_BASE_URL);
    if (process.env.NODE_ENV === "production" && appUrl.protocol !== "https:") {
      throw new Error("CARD_EXPORT_APP_BASE_URL must use HTTPS in production.");
    }
    if (process.env.NODE_ENV === "production" && CARD_EXPORT_WORKER_SECRET.length < 32) {
      throw new Error("CARD_EXPORT_WORKER_SECRET must contain at least 32 characters in production.");
    }
    await prisma.studentCardExportJob.findFirst({ select: { id: true } });
  }
  logger.info({
    event: "worker.preflight_passed",
    authStateVerified: Boolean(encryptedAuthState),
    standbyOnly: STANDBY_ONLY,
    cardExportWorkerEnabled: CARD_EXPORT_WORKER_ENABLED,
    cardExportOnly: CARD_EXPORT_WORKER_ONLY
  }, "WhatsApp worker production preflight passed");
}

async function usePrismaAuthState(tenantId) {
  const credsEntry = await readAuthStateEntry(tenantId, "creds.json");
  if (credsEntry.status === "corrupt") {
    throw new Error("Stored WhatsApp credentials are corrupt. Disconnect this WhatsApp session and connect again.");
  }

  const creds = credsEntry.status === "found" ? credsEntry.value : initAuthCreds();
  logger.info({
    tenantId,
    credsStatus: credsEntry.status,
    hasMe: Boolean(creds?.me?.id),
    registered: Boolean(creds?.registered),
    authRows: await countAuthRows(tenantId)
  }, "loaded WhatsApp auth state");

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const result = {};
          await Promise.all(ids.map(async (id) => {
            const key = authStateKey(type, id);
            const legacyKey = `${type}-${id}.json`;
            let value = await readAuthStateValue(tenantId, key, key === legacyKey ? undefined : legacyKey);
            if (type === "app-state-sync-key" && value) {
              value = proto.Message.AppStateSyncKeyData.fromObject(value);
            }
            result[id] = value;
          }));
          logger.debug({
            tenantId,
            type,
            requested: ids.length,
            found: Object.values(result).filter(Boolean).length
          }, "loaded WhatsApp auth keys");
          return result;
        },
        set: async (data) => {
          const tasks = [];
          let writes = 0;
          let deletes = 0;
          for (const category of Object.keys(data)) {
            for (const id of Object.keys(data[category])) {
              const key = authStateKey(category, id);
              const legacyKey = `${category}-${id}.json`;
              const value = data[category][id];
              if (value) {
                writes += 1;
                tasks.push(writeAuthStateValue(tenantId, key, value));
              } else {
                deletes += 1;
                tasks.push(deleteAuthStateKeys(tenantId, [key, legacyKey]));
              }
            }
          }
          await Promise.all(tasks);
          logger.debug({ tenantId, writes, deletes }, "saved WhatsApp auth keys");
        }
      }
    },
    saveCreds: () => writeAuthStateValue(tenantId, "creds.json", creds)
  };
}

function phoneFromSocket(socket, state) {
  const raw = socket.user?.id || state.creds.me?.id || "";
  const first = raw.split(":")[0] || raw.split("@")[0] || "";
  const digits = first.replace(/\D/g, "");
  return digits || null;
}

async function setSessionStatus(tenantId, data) {
  await prisma.whatsAppSession.upsert({
    where: { tenantId },
    update: data,
    create: {
      tenantId,
      status: data.status || "DISCONNECTED",
      ...data
    }
  });
}

function canRunLeaderWork() {
  return leaderActive && !shuttingDown;
}

function emitWorkerStatus(state, details = {}) {
  const payload = {
    type: WORKER_STATUS_MESSAGE_TYPE,
    version: 1,
    state,
    at: new Date().toISOString(),
    label: WORKER_OWNER_LABEL,
    role: WORKER_ROLE,
    standbyOnly: STANDBY_ONLY,
    ...details
  };

  if (typeof process.send === "function" && process.connected) {
    process.send(payload, (error) => {
      if (error && error.code !== "ERR_IPC_CHANNEL_CLOSED") {
        logger.warn({ event: "worker.status_emit_failed", error }, "failed to report worker status to service wrapper");
      }
    });
  }
}

function healthcheckFailureUrl(rawUrl) {
  const parsed = new URL(rawUrl);
  parsed.pathname = `${parsed.pathname.replace(/\/+$/, "")}/fail`;
  return parsed.toString();
}

async function pingHealthcheck(name, rawUrl, { failure = false, force = false } = {}) {
  if (!rawUrl) return;
  const now = Date.now();
  const lastPingAt = healthcheckLastPingAt.get(name) || 0;
  if (!force && now - lastPingAt < HEALTHCHECK_PING_INTERVAL_MS) return;

  healthcheckLastPingAt.set(name, now);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), HEALTHCHECK_TIMEOUT_MS);
  try {
    const response = await fetch(failure ? healthcheckFailureUrl(rawUrl) : rawUrl, {
      method: "POST",
      signal: controller.signal,
      headers: { "User-Agent": "centrix-whatsapp-worker/1" }
    });
    if (!response.ok) {
      throw new Error(`Healthcheck returned HTTP ${response.status}.`);
    }
  } catch (error) {
    logger.warn({ event: "monitor.ping_failed", monitor: name, error }, "worker healthcheck ping failed");
  } finally {
    clearTimeout(timeout);
  }
}

async function reportHealthyInstance(state) {
  await pingHealthcheck("instance", process.env.HEALTHCHECKS_INSTANCE_PING_URL);
  if (state === "leader") {
    await Promise.all([
      pingHealthcheck("leader", process.env.HEALTHCHECKS_LEADER_PING_URL),
      pingHealthcheck("lease", process.env.HEALTHCHECKS_LEASE_PING_URL)
    ]);
  }
}

async function scanForStaleProcessingLocks() {
  const now = Date.now();
  if (now - staleLockScanAt < STALE_PROCESSING_SCAN_MS) return;
  staleLockScanAt = now;

  const staleBefore = new Date(now - STALE_PROCESSING_THRESHOLD_MS);
  const [count, oldest] = await Promise.all([
    prisma.notificationDelivery.count({
      where: {
        channel: "WHATSAPP",
        status: "PROCESSING",
        lockedAt: { lt: staleBefore }
      }
    }),
    prisma.notificationDelivery.findFirst({
      where: {
        channel: "WHATSAPP",
        status: "PROCESSING",
        lockedAt: { lt: staleBefore }
      },
      orderBy: { lockedAt: "asc" },
      select: { lockedAt: true }
    })
  ]);

  if (count > 0) {
    const oldestAgeMs = oldest?.lockedAt ? Math.max(0, now - oldest.lockedAt.getTime()) : null;
    logger.warn({
      event: "queue.stale_processing_detected",
      count,
      oldestAgeMs
    }, "stale WhatsApp delivery locks require operator review");
    await pingHealthcheck("queue", process.env.HEALTHCHECKS_QUEUE_PING_URL, {
      failure: true,
      force: true
    });
    return;
  }

  await pingHealthcheck("queue", process.env.HEALTHCHECKS_QUEUE_PING_URL);
}

async function verifyLeaseOwnership() {
  if (!HA_ENABLED || STANDBY_ONLY || !leaderActive) return false;
  const lease = await prisma.notificationWorkerLease.findFirst({
    where: {
      id: WORKER_LEASE_ID,
      ownerId: WORKER_OWNER_ID,
      expiresAt: { gt: new Date() }
    },
    select: { id: true }
  });
  return Boolean(lease);
}

function leaseHeartbeatData(now = new Date()) {
  return {
    ownerId: WORKER_OWNER_ID,
    ownerLabel: WORKER_OWNER_LABEL,
    heartbeatAt: now,
    expiresAt: new Date(now.getTime() + WORKER_LEASE_TTL_MS),
    capabilities: { cardExport: CARD_EXPORT_WORKER_ENABLED }
  };
}

function logDevelopmentLeaseSnapshot(event, now, expiresAt, extra = {}) {
  if (process.env.NODE_ENV === "production") return;
  logger.info({
    event,
    pid: process.pid,
    instanceLabel: WORKER_OWNER_LABEL,
    role: WORKER_ROLE,
    leaseId: WORKER_LEASE_ID,
    acquiredAt: leaderAcquiredAt?.toISOString() || null,
    heartbeatAt: now.toISOString(),
    leaseExpiresAt: expiresAt.toISOString(),
    remainingLeaseMs: Math.max(0, expiresAt.getTime() - now.getTime()),
    capabilities: { cardExport: CARD_EXPORT_WORKER_ENABLED },
    currentExportJobId: activeCardExportJobs.keys().next().value || null,
    ...extra
  }, "local notification worker lease state");
}

function leaseOwnershipData(now = new Date()) {
  return {
    ...leaseHeartbeatData(now),
    startedAt: now
  };
}

async function tryAcquireWorkerLease() {
  const now = new Date();
  const existing = await prisma.notificationWorkerLease.findUnique({
    where: { id: WORKER_LEASE_ID }
  });
  const ownershipData = leaseOwnershipData(now);
  const heartbeatData = leaseHeartbeatData(now);

  if (!existing) {
    try {
      await prisma.notificationWorkerLease.create({
        data: {
          id: WORKER_LEASE_ID,
          ...ownershipData
        }
      });
      return { acquired: true, reason: "created" };
    } catch (error) {
      if (error?.code !== "P2002") {
        throw error;
      }
      logger.debug({ event: "lease.create_raced", error, ownerLabel: WORKER_OWNER_LABEL }, "worker lease create raced with another worker");
      return { acquired: false, reason: "create-raced" };
    }
  }

  const expired = existing.expiresAt.getTime() <= now.getTime();
  const ownedByThisWorker = existing.ownerId === WORKER_OWNER_ID;
  const shouldPreemptForPreferredWorker = (
    WORKER_OWNER_LABEL === PREFERRED_WORKER_LABEL
    && existing.ownerLabel !== PREFERRED_WORKER_LABEL
    && !expired
  );

  if (!expired && !ownedByThisWorker && !shouldPreemptForPreferredWorker) {
    return { acquired: false, reason: "held", ownerLabel: existing.ownerLabel };
  }

  const updated = await prisma.notificationWorkerLease.updateMany({
    where: {
      id: WORKER_LEASE_ID,
      OR: [
        { ownerId: WORKER_OWNER_ID },
        { expiresAt: { lte: now } },
        ...(shouldPreemptForPreferredWorker ? [{ ownerLabel: null }, { ownerLabel: { not: PREFERRED_WORKER_LABEL } }] : [])
      ]
    },
    data: ownedByThisWorker && !expired ? heartbeatData : ownershipData
  });

  if (updated.count !== 1) {
    return { acquired: false, reason: "update-raced" };
  }

  return {
    acquired: true,
    reason: shouldPreemptForPreferredWorker ? "preferred-preempted" : expired ? "expired" : ownedByThisWorker ? "renewed" : "expired"
  };
}

async function renewWorkerLease() {
  const now = new Date();
  const expiresAt = new Date(now.getTime() + WORKER_LEASE_TTL_MS);
  const updated = await prisma.notificationWorkerLease.updateMany({
    where: {
      id: WORKER_LEASE_ID,
      ownerId: WORKER_OWNER_ID,
      expiresAt: { gt: now }
    },
    data: {
      heartbeatAt: now,
      expiresAt,
      capabilities: { cardExport: CARD_EXPORT_WORKER_ENABLED }
    }
  });
  if (updated.count === 1) logDevelopmentLeaseSnapshot("lease.heartbeat", now, expiresAt);
  return updated.count === 1;
}

async function releaseWorkerLease() {
  if (!HA_ENABLED) {
    return;
  }

  await prisma.notificationWorkerLease.deleteMany({
    where: {
      id: WORKER_LEASE_ID,
      ownerId: WORKER_OWNER_ID
    }
  });
}

function addLeaderInterval(callback, intervalMs) {
  const timer = setInterval(callback, intervalMs);
  leaderTimers.add(timer);
  return timer;
}

function clearLeaderTimers() {
  for (const timer of leaderTimers) {
    clearInterval(timer);
  }
  leaderTimers.clear();
}

function startLeaderWork(reason) {
  if (leaderActive || shuttingDown) {
    return;
  }

  leaderActive = true;
  leaderAcquiredAt = new Date();
  logger.info({
    event: WORKER_ROLE === "fallback" && (reason === "expired" || reason === "created")
      ? "fallback.activated"
      : "lease.acquired",
    ownerLabel: WORKER_OWNER_LABEL,
    role: WORKER_ROLE,
    reason,
    haEnabled: HA_ENABLED
  }, "notification worker became leader");
  emitWorkerStatus("leader", { reason });
  logDevelopmentLeaseSnapshot(
    "lease.ready",
    leaderAcquiredAt,
    new Date(leaderAcquiredAt.getTime() + WORKER_LEASE_TTL_MS),
    { reason }
  );
  void reportHealthyInstance("leader");

  if (!CARD_EXPORT_WORKER_ONLY) {
    void pollConnectionRequests().catch((error) => logger.error({ error }, "connection poll failed"));
    void processQueue().catch((error) => logger.error({ error }, "queue processing failed"));
    void scanSubscriptionExpiring().catch((error) => logger.error({ error }, "subscription scan failed"));
  }
  if (CARD_EXPORT_WORKER_ENABLED) {
    void processCardExportQueue().catch((error) => logger.error({ error }, "card export queue processing failed"));
  }

  if (!CARD_EXPORT_WORKER_ONLY) {
    addLeaderInterval(() => {
      void pollConnectionRequests().catch((error) => logger.error({ error }, "connection poll failed"));
    }, SESSION_POLL_INTERVAL_MS);

    addLeaderInterval(() => {
      void processQueue().catch((error) => logger.error({ error }, "queue processing failed"));
    }, POLL_INTERVAL_MS);

    addLeaderInterval(() => {
      void scanSubscriptionExpiring().catch((error) => logger.error({ error }, "subscription scan failed"));
    }, SUBSCRIPTION_SCAN_INTERVAL_MS);
  }

  if (CARD_EXPORT_WORKER_ENABLED) {
    addLeaderInterval(() => {
      void processCardExportQueue().catch((error) => logger.error({ error }, "card export queue processing failed"));
    }, CARD_EXPORT_WORKER_POLL_MS);
  }

}

async function stopLeaderWork(reason) {
  if (!leaderActive) {
    return;
  }

  leaderActive = false;
  leaderAcquiredAt = null;
  clearLeaderTimers();

  for (const timer of reconnectTimers.values()) {
    clearTimeout(timer);
  }
  reconnectTimers.clear();

  const socketsToClose = [...sockets.entries()];
  sockets.clear();
  for (const [tenantId, socket] of socketsToClose) {
    intentionallyEndingSockets.add(tenantId);
    await socket.end(undefined).catch(() => undefined);
  }

  await releaseActiveDeliveries(reason);
  await releaseActiveCardExportJobs(reason);

  if (!shuttingDown) {
    await pingHealthcheck("lease", process.env.HEALTHCHECKS_LEASE_PING_URL, {
      failure: true,
      force: true
    });
  }

  logger.warn({
    event: "lease.lost",
    ownerLabel: WORKER_OWNER_LABEL,
    role: WORKER_ROLE,
    reason
  }, "notification worker left leader mode");
  if (!shuttingDown) emitWorkerStatus("degraded", { reason });
}

async function becomeLeader(reason) {
  const delayMs = reason === "preferred-preempted" ? WORKER_PREEMPT_GRACE_MS : 0;
  if (delayMs > 0) {
    logger.info({
      event: "primary.restoring",
      ownerLabel: WORKER_OWNER_LABEL,
      delayMs
    }, "preferred worker acquired lease; waiting for previous worker to yield");
    await sleep(delayMs);
    if (!(await renewWorkerLease())) {
      logger.warn({ event: "lease.handoff_lost", ownerLabel: WORKER_OWNER_LABEL }, "preferred worker lost lease during handoff");
      emitWorkerStatus("degraded", { reason: "handoff-lost" });
      return;
    }
  }

  startLeaderWork(reason);
  if (reason === "preferred-preempted") {
    logger.info({ event: "primary.restored", ownerLabel: WORKER_OWNER_LABEL }, "preferred worker restored leadership");
  }
}

async function maintainWorkerLease() {
  if (leaseCycleRunning || shuttingDown) {
    return;
  }

  leaseCycleRunning = true;
  try {
    if (STANDBY_ONLY) {
      const lease = await prisma.notificationWorkerLease.findUnique({
        where: { id: WORKER_LEASE_ID },
        select: { expiresAt: true }
      });
      const leaderPresent = Boolean(lease && lease.expiresAt.getTime() > Date.now());
      emitWorkerStatus("standby-locked", { leaderPresent });
      await reportHealthyInstance("standby-locked");
      return;
    }

    if (!HA_ENABLED) {
      startLeaderWork("ha-disabled");
      return;
    }

    if (leaderActive) {
      const renewed = await renewWorkerLease();
      if (!renewed) {
        await stopLeaderWork("lease-lost");
      } else {
        emitWorkerStatus("leader", { reason: "renewed" });
        await Promise.all([
          reportHealthyInstance("leader"),
          scanForStaleProcessingLocks()
        ]);
      }
      return;
    }

    const result = await tryAcquireWorkerLease();
    if (result.acquired) {
      await becomeLeader(result.reason);
      return;
    }

    logger.debug({
      ownerLabel: WORKER_OWNER_LABEL,
      leaseOwnerLabel: result.ownerLabel,
      reason: result.reason
    }, "notification worker standby; lease held by another worker");
    emitWorkerStatus("standby", { leaderPresent: true });
    await reportHealthyInstance("standby");
  } catch (error) {
    logger.error({ event: "db.degraded", error, ownerLabel: WORKER_OWNER_LABEL }, "worker lease cycle failed");
    await stopLeaderWork("lease-error");
    emitWorkerStatus("degraded", { reason: "lease-error" });
  } finally {
    leaseCycleRunning = false;
  }
}

async function pauseActiveDeliveries(tenantId, errorMessage = "WhatsApp disconnected") {
  await prisma.notificationDelivery.updateMany({
    where: {
      tenantId,
      channel: "WHATSAPP",
      status: { in: ["QUEUED", "RETRY"] },
      lockToken: null
    },
    data: {
      status: "PAUSED",
      errorMessage
    }
  });

  const ownedDeliveries = [...activeDeliveries.values()].filter((delivery) => (
    delivery.tenantId === tenantId
    && shouldReleaseDeliveryOnLeadershipChange(delivery)
  ));
  await Promise.all(ownedDeliveries.map((delivery) => (
    prisma.notificationDelivery.updateMany({
      where: deliveryOwnershipWhere(delivery),
      data: {
        status: "PAUSED",
        errorMessage,
        lockToken: null,
        lockedAt: null,
        processingStartedAt: null
      }
    })
  )));
}

function clearReconnectTimer(tenantId) {
  const timer = reconnectTimers.get(tenantId);
  if (timer) {
    clearTimeout(timer);
    reconnectTimers.delete(tenantId);
  }
}

function scheduleReconnect(tenantId, reason, immediate = false) {
  if (shuttingDown || !canRunLeaderWork() || reconnectTimers.has(tenantId)) {
    return;
  }

  const attempt = (reconnectAttempts.get(tenantId) || 0) + 1;
  reconnectAttempts.set(tenantId, attempt);
  const exponentialDelay = WHATSAPP_RECONNECT_BASE_MS * Math.pow(2, Math.max(0, attempt - 1));
  const jitter = Math.floor(Math.random() * WHATSAPP_RECONNECT_BASE_MS);
  const delayMs = immediate ? 0 : Math.min(WHATSAPP_RECONNECT_MAX_MS, exponentialDelay + jitter);

  logger.info({ tenantId, attempt, delayMs, reason }, "scheduled WhatsApp reconnect");
  const timer = setTimeout(() => {
    reconnectTimers.delete(tenantId);
    if (!canRunLeaderWork()) {
      return;
    }
    void connectTenant(tenantId).catch((error) => logger.error({ error, tenantId }, "scheduled WhatsApp reconnect failed"));
  }, delayMs);
  reconnectTimers.set(tenantId, timer);
}

async function connectTenant(tenantId) {
  if (!canRunLeaderWork()) {
    return;
  }

  if (sockets.has(tenantId) || connecting.has(tenantId)) {
    return;
  }

  connecting.add(tenantId);
  try {
    const session = await prisma.whatsAppSession.findUnique({
      where: { tenantId },
      select: { status: true }
    });
    if (session?.status === "DISCONNECTED") {
      logger.info({ tenantId }, "skipping WhatsApp connect for disconnected session");
      return;
    }

    const { state, saveCreds } = await usePrismaAuthState(tenantId);
    const protocolVersion = await getWhatsAppProtocolVersion();
    const socket = makeWASocket({
      auth: state,
      logger: baileysLogger,
      markOnlineOnConnect: false,
      printQRInTerminal: false,
      version: protocolVersion.version
    });

    sockets.set(tenantId, socket);
    let knownHasMe = Boolean(state.creds?.me?.id);

    socket.ev.on("creds.update", (update) => {
      const hadMe = knownHasMe;
      Object.assign(state.creds, update);
      const hasMe = Boolean(state.creds?.me?.id);
      knownHasMe = hasMe;
      const save = queueCredentialSave(tenantId, () => saveCreds());
      void save.then(() => {
        const log = !hadMe && hasMe ? logger.info.bind(logger) : logger.debug.bind(logger);
        log({ tenantId, hadMe, hasMe, registered: Boolean(state.creds?.registered) }, "saved WhatsApp credential update");
      }).catch((error) => logger.error({ error, tenantId, hadMe, hasMe }, "failed to save WhatsApp creds"));
    });

    socket.ev.on("connection.update", (update) => {
      void (async () => {
        if (update.qr) {
          if (state.creds?.me?.id) {
            logger.warn({ tenantId, hasMe: true }, "ignored WhatsApp QR for restored authenticated session");
            return;
          }

          const qrCodeDataUrl = await QRCode.toDataURL(update.qr, { margin: 1, width: 320 });
          await setSessionStatus(tenantId, {
            status: "CONNECTING",
            qrCode: update.qr,
            qrCodeDataUrl,
            errorMessage: null
          });
        }

        if (update.connection === "open") {
          reconnectAttempts.delete(tenantId);
          clearReconnectTimer(tenantId);
          const phoneNumber = phoneFromSocket(socket, state);
          await setSessionStatus(tenantId, {
            status: "CONNECTED",
            phoneNumber,
            qrCode: null,
            qrCodeDataUrl: null,
            lastConnectedAt: new Date(),
            errorMessage: null
          });
          await prisma.notificationDelivery.updateMany({
            where: {
              tenantId,
              channel: "WHATSAPP",
              status: "PAUSED"
            },
            data: {
              status: "QUEUED",
              errorMessage: null,
              availableAt: new Date(),
              lockToken: null,
              lockedAt: null,
              processingStartedAt: null
            }
          });
          logger.info({
            event: "whatsapp.connected",
            tenantId,
            hasPhoneNumber: Boolean(phoneNumber),
            hasMe: Boolean(state.creds?.me?.id)
          }, "WhatsApp connected");
        }

        if (update.connection === "close") {
          sockets.delete(tenantId);
          if (shuttingDown) {
            logger.info({ tenantId }, "WhatsApp socket closed during worker shutdown");
            return;
          }
          if (intentionallyEndingSockets.delete(tenantId)) {
            logger.info({ tenantId }, "WhatsApp socket closed after explicit disconnect");
            return;
          }

          const statusCode = update.lastDisconnect?.error?.output?.statusCode;
          const loggedOut = statusCode === DisconnectReason.loggedOut;
          const restartRequired = statusCode === DisconnectReason.restartRequired;
          const unregisteredSession = !state.creds?.registered && !state.creds?.me?.id;
          const protocolRejected = statusCode === 405 && unregisteredSession;
          const errorMessage = protocolRejected
            ? "تعذر بدء ربط واتساب بسبب رفض إصدار بروتوكول الاتصال. يقوم النظام بتحديث الإصدار وإعادة المحاولة تلقائيًا."
            : sanitizeErrorMessage(update.lastDisconnect?.error?.message || "") || null;
          await waitForCredentialSave(tenantId);

          if (loggedOut) {
            await prisma.whatsAppAuthState.deleteMany({ where: { tenantId } });
            reconnectAttempts.delete(tenantId);
            clearReconnectTimer(tenantId);
            await setSessionStatus(tenantId, {
              status: "DISCONNECTED",
              qrCode: null,
              qrCodeDataUrl: null,
              disconnectedAt: new Date(),
              errorMessage
            });
            await pauseActiveDeliveries(tenantId);
            logger.warn({ tenantId, statusCode, hasMe: Boolean(state.creds?.me?.id) }, "WhatsApp terminal logout; auth state cleared");
            return;
          }

          if (protocolRejected) {
            const refreshedVersion = await getWhatsAppProtocolVersion({ force: true });
            const versionChanged = !sameWhatsAppVersion(protocolVersion.version, refreshedVersion.version);
            await setSessionStatus(tenantId, {
              status: "CONNECTING",
              qrCode: null,
              qrCodeDataUrl: null,
              disconnectedAt: new Date(),
              errorMessage
            });
            await pauseActiveDeliveries(tenantId);
            logger.warn({
              event: "whatsapp.registration_protocol_rejected",
              tenantId,
              statusCode,
              versionChanged,
              versionSource: refreshedVersion.source,
              versionVerified: refreshedVersion.verified
            }, "WhatsApp rejected new-device registration; refreshed protocol version");
            scheduleReconnect(
              tenantId,
              versionChanged ? "protocol-version-refreshed" : "protocol-version-rejected",
              versionChanged
            );
            return;
          }

          await setSessionStatus(tenantId, {
            status: "CONNECTING",
            qrCode: null,
            qrCodeDataUrl: null,
            disconnectedAt: new Date(),
            errorMessage
          });
          await pauseActiveDeliveries(tenantId);
          logger.warn({
            tenantId,
            statusCode,
            restartRequired,
            hasMe: Boolean(state.creds?.me?.id)
          }, "WhatsApp disconnected; preserving auth state for reconnect");
          scheduleReconnect(tenantId, restartRequired ? "restart-required" : "transient-close", restartRequired);
        }
      })().catch((error) => logger.error({ error, tenantId }, "connection update failed"));
    });
  } catch (error) {
    await setSessionStatus(tenantId, {
      status: "DISCONNECTED",
      errorMessage: error instanceof Error ? error.message : String(error),
      disconnectedAt: new Date()
    });
    logger.error({ error, tenantId }, "failed to connect WhatsApp");
  } finally {
    connecting.delete(tenantId);
  }
}

async function pollConnectionRequests() {
  if (!canRunLeaderWork()) {
    return;
  }

  const sessions = await prisma.whatsAppSession.findMany({
    where: { status: { in: ["CONNECTING", "CONNECTED"] } },
    select: { tenantId: true, status: true }
  });

  await Promise.all(sessions.map((session) => (
    reconnectTimers.has(session.tenantId) ? undefined : connectTenant(session.tenantId)
  )));

  const disconnected = await prisma.whatsAppSession.findMany({
    where: { status: "DISCONNECTED" },
    select: { tenantId: true }
  });
  for (const session of disconnected) {
    clearReconnectTimer(session.tenantId);
    const socket = sockets.get(session.tenantId);
    if (!socket) {
      continue;
    }
    intentionallyEndingSockets.add(session.tenantId);
    sockets.delete(session.tenantId);
    await socket.end(undefined).catch(() => undefined);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randomDelayMs(setting) {
  const min = Math.max(0, Number(setting?.minDelaySeconds ?? 20));
  const max = Math.max(min, Number(setting?.maxDelaySeconds ?? 90));
  const value = min + Math.floor(Math.random() * (max - min + 1));
  return value * 1000;
}

async function sentCountSince(tenantId, since) {
  return prisma.notificationDelivery.count({
    where: {
      tenantId,
      channel: "WHATSAPP",
      status: "SENT",
      sentAt: { gte: since }
    }
  });
}

async function refreshJobStatus(jobId) {
  const deliveries = await prisma.notificationDelivery.findMany({
    where: { jobId },
    select: { status: true }
  });

  if (!deliveries.length) {
    await prisma.notificationJob.update({ where: { id: jobId }, data: { status: "SKIPPED" } });
    return;
  }

  const statuses = deliveries.map((delivery) => delivery.status);
  const pending = statuses.some((status) => ["QUEUED", "PROCESSING", "RETRY", "PAUSED"].includes(status));
  const sent = statuses.includes("SENT");
  const failed = statuses.includes("FAILED");
  const status = pending ? "QUEUED" : sent && failed ? "PARTIAL" : sent ? "SENT" : "FAILED";
  await prisma.notificationJob.update({ where: { id: jobId }, data: { status } });
}

async function rescheduleForLimit(delivery, nextAvailableAt, message) {
  return prisma.notificationDelivery.updateMany({
    where: deliveryOwnershipWhere(delivery),
    data: {
      status: "QUEUED",
      lockToken: null,
      lockedAt: null,
      processingStartedAt: null,
      availableAt: nextAvailableAt,
      errorMessage: message
    }
  });
}

async function failOrRetry(delivery, error, setting) {
  const retryCount = delivery.retryCount + 1;
  const maxAttempts = Math.max(1, delivery.maxAttempts || setting?.retryMaxAttempts || 3);
  const message = sanitizeErrorMessage(error instanceof Error ? error.message : String(error));

  if (retryCount >= maxAttempts) {
    const updated = await prisma.notificationDelivery.updateMany({
      where: deliveryOwnershipWhere(delivery),
      data: {
        status: "FAILED",
        retryCount,
        errorMessage: message,
        lockToken: null,
        lockedAt: null,
        processingStartedAt: null
      }
    });
    if (updated.count === 1) await refreshJobStatus(delivery.jobId);
    return;
  }

  const baseDelay = Math.max(30, Number(setting?.retryBaseDelaySeconds ?? 300));
  const jitter = Math.floor(Math.random() * baseDelay);
  const delaySeconds = baseDelay * Math.pow(2, Math.max(0, retryCount - 1)) + jitter;
  await prisma.notificationDelivery.updateMany({
    where: deliveryOwnershipWhere(delivery),
    data: {
      status: "RETRY",
      retryCount,
      errorMessage: message,
      availableAt: new Date(Date.now() + delaySeconds * 1000),
      lockToken: null,
      lockedAt: null,
      processingStartedAt: null
    }
  });
}

async function releaseDeliveryForLeadershipChange(delivery) {
  if (!delivery?.id || !delivery?.lockToken) {
    return;
  }

  await prisma.notificationDelivery.updateMany({
    where: deliveryOwnershipWhere(delivery),
    data: {
      status: "QUEUED",
      lockToken: null,
      lockedAt: null,
      processingStartedAt: null,
      availableAt: new Date(),
      errorMessage: "WhatsApp worker leadership changed"
    }
  });
}

async function releaseActiveDeliveries(reason) {
  const deliveries = [...activeDeliveries.values()];
  if (!deliveries.length) return;

  await Promise.all(deliveries.map((delivery) => {
    if (!shouldReleaseDeliveryOnLeadershipChange(delivery)) {
      logger.error({
        event: "queue.inflight_delivery_preserved",
        deliveryId: delivery.id,
        phase: delivery.processingPhase,
        reason
      }, "an in-flight WhatsApp delivery remains PROCESSING for operator review");
      return Promise.resolve();
    }

    return releaseDeliveryForLeadershipChange(delivery).catch((error) => {
      logger.error({
        event: "queue.delivery_release_failed",
        deliveryId: delivery.id,
        reason,
        error
      }, "failed to release an active delivery after leadership changed");
    });
  }));
  activeDeliveries.clear();
}

async function deliveryStillOwned(delivery) {
  const row = await prisma.notificationDelivery.findFirst({
    where: deliveryOwnershipWhere(delivery),
    select: { id: true }
  });
  return Boolean(row);
}

async function processDelivery(delivery) {
  if (!canRunLeaderWork()) {
    await releaseDeliveryForLeadershipChange(delivery);
    return;
  }

  const session = await prisma.whatsAppSession.findUnique({
    where: { tenantId: delivery.tenantId }
  });
  if (session?.status !== "CONNECTED") {
    const updated = await prisma.notificationDelivery.updateMany({
      where: deliveryOwnershipWhere(delivery),
      data: {
        status: "PAUSED",
        lockToken: null,
        lockedAt: null,
        processingStartedAt: null,
        errorMessage: "WhatsApp disconnected"
      }
    });
    if (updated.count === 1) await refreshJobStatus(delivery.jobId);
    return;
  }

  const socket = sockets.get(delivery.tenantId);
  if (!socket) {
    await connectTenant(delivery.tenantId);
    await prisma.notificationDelivery.updateMany({
      where: deliveryOwnershipWhere(delivery),
      data: {
        status: "PAUSED",
        lockToken: null,
        lockedAt: null,
        processingStartedAt: null,
        errorMessage: "WhatsApp socket is not ready"
      }
    });
    return;
  }

  const setting = await prisma.notificationChannelSetting.findUnique({
    where: {
      tenantId_channel: {
        tenantId: delivery.tenantId,
        channel: "WHATSAPP"
      }
    }
  });
  if (!setting?.enabled) {
    const updated = await prisma.notificationDelivery.updateMany({
      where: deliveryOwnershipWhere(delivery),
      data: {
        status: "PAUSED",
        lockToken: null,
        lockedAt: null,
        processingStartedAt: null,
        errorMessage: "WhatsApp notifications disabled"
      }
    });
    if (updated.count === 1) await refreshJobStatus(delivery.jobId);
    return;
  }

  const now = Date.now();
  const hourStart = new Date(now - 60 * 60 * 1000);
  const dayStart = new Date(now - 24 * 60 * 60 * 1000);
  const [hourCount, dayCount] = await Promise.all([
    sentCountSince(delivery.tenantId, hourStart),
    sentCountSince(delivery.tenantId, dayStart)
  ]);
  if (hourCount >= setting.hourlyLimit) {
    await rescheduleForLimit(delivery, new Date(Date.now() + 60 * 60 * 1000), "Hourly sending limit reached");
    return;
  }
  if (dayCount >= setting.dailyLimit) {
    await rescheduleForLimit(delivery, new Date(Date.now() + 24 * 60 * 60 * 1000), "Daily sending limit reached");
    return;
  }

  await sleep(randomDelayMs(setting));
  if (!canRunLeaderWork()) {
    await releaseDeliveryForLeadershipChange(delivery);
    return;
  }

  const [leaseOwned, lockOwned] = await Promise.all([
    verifyLeaseOwnership(),
    deliveryStillOwned(delivery)
  ]);
  if (!leaseOwned || !lockOwned) {
    logger.warn({
      event: "queue.delivery_fenced",
      deliveryId: delivery.id,
      leaseOwned,
      lockOwned
    }, "delivery send was fenced before contacting WhatsApp");
    if (!leaseOwned) await stopLeaderWork("lease-verification-failed");
    else await releaseDeliveryForLeadershipChange(delivery);
    return;
  }

  const jid = `${delivery.recipientAddress}@s.whatsapp.net`;
  const renderedText = delivery.eventType === "PRODUCT_SALE_PAYMENT_RECEIVED"
    ? delivery.renderedMessage.replace(/\n?مرفق نسخة PDF محدثة من الفاتورة\.\s*$/u, "").trim()
    : delivery.renderedMessage;
  delivery.processingPhase = "sending";
  const outcome = await executeIdempotentSend({
    deliveryId: delivery.id,
    send: (messageId) => socket.sendMessage(jid, { text: renderedText }, { messageId }),
    persistAcknowledgement: async (providerMessageId) => {
      delivery.processingPhase = "acknowledging";
      const updated = await prisma.notificationDelivery.updateMany({
        where: deliveryOwnershipWhere(delivery),
        data: {
          status: "SENT",
          providerMessageId,
          sentAt: new Date(),
          errorMessage: null,
          lockToken: null,
          lockedAt: null,
          processingStartedAt: null
        }
      });
      return updated.count === 1;
    },
    handleSendFailure: async (error) => {
      delivery.processingPhase = "retrying";
      logger.error({
        event: "whatsapp.send_failed",
        error,
        deliveryId: delivery.id
      }, "failed to send WhatsApp notification");
      await failOrRetry(delivery, error, setting);
    }
  });

  if (outcome.status === "sent") {
    await refreshJobStatus(delivery.jobId);
  } else if (outcome.status === "ack-conflict") {
    logger.error({
      event: "queue.sent_ack_conflict",
      deliveryId: delivery.id
    }, "WhatsApp accepted a message but the delivery lock was no longer owned");
  } else if (outcome.status === "ack-persist-failed") {
    logger.error({
      event: "queue.sent_ack_persist_failed",
      error: outcome.error,
      deliveryId: delivery.id
    }, "WhatsApp accepted a message but its database acknowledgement could not be stored");
  }
}

async function claimDeliveries() {
  if (!canRunLeaderWork()) {
    return [];
  }

  const rows = await prisma.notificationDelivery.findMany({
    where: {
      channel: "WHATSAPP",
      status: { in: ["QUEUED", "RETRY"] },
      availableAt: { lte: new Date() }
    },
    orderBy: [{ availableAt: "asc" }, { createdAt: "asc" }],
    take: 5
  });

  const claimed = [];
  for (const row of rows) {
    const lockToken = crypto.randomUUID();
    const updated = await prisma.notificationDelivery.updateMany({
      where: {
        id: row.id,
        status: row.status,
        lockToken: null
      },
      data: {
        status: "PROCESSING",
        lockToken,
        lockedAt: new Date(),
        processingStartedAt: new Date()
      }
    });
    if (updated.count === 1) {
      const delivery = { ...row, lockToken, processingPhase: "claimed" };
      activeDeliveries.set(delivery.id, delivery);
      claimed.push(delivery);
    }
  }

  return claimed;
}

async function processQueue() {
  if (!canRunLeaderWork()) {
    return;
  }

  const deliveries = await claimDeliveries();
  for (const delivery of deliveries) {
    try {
      if (!canRunLeaderWork()) {
        await releaseDeliveryForLeadershipChange(delivery);
        continue;
      }
      await processDelivery(delivery);
    } finally {
      activeDeliveries.delete(delivery.id);
    }
  }
}

async function releaseActiveCardExportJobs(reason) {
  const active = [...activeCardExportJobs.values()];
  activeCardExportJobs.clear();
  for (const job of active) {
    await prisma.$transaction(async (tx) => {
      await tx.studentCardExportPart.updateMany({
        where: { jobId: job.id, status: "PROCESSING" },
        data: {
          status: "RETRY",
          phase: "RETRY",
          progressUpdatedAt: new Date(),
          errorCode: "CARD_EXPORT_WORKER_LEADERSHIP_CHANGED",
          errorMessage: reason
        }
      });
      await tx.studentCardExportJob.updateMany({
        where: { id: job.id, lockToken: job.lockToken, status: "PROCESSING" },
        data: {
          status: "RETRY",
          phase: "RETRY",
          availableAt: new Date(),
          lockToken: null,
          lockedAt: null,
          lockedBy: null,
          currentPartNumber: null,
          progressUpdatedAt: new Date(),
          errorCode: "CARD_EXPORT_WORKER_LEADERSHIP_CHANGED",
          errorMessage: reason
        }
      });
    }).catch((error) => logger.warn({ event: "card_export.release_failed", error, jobId: job.id }, "failed to release card export job"));
  }
}

async function recoverStaleCardExportJobs() {
  if (!CARD_EXPORT_WORKER_ENABLED || !canRunLeaderWork()) return;
  const cutoff = new Date(Date.now() - CARD_EXPORT_WORKER_STALE_LOCK_MS);
  const stale = await prisma.studentCardExportJob.findMany({
    where: { status: "PROCESSING", lockedAt: { lt: cutoff } },
    select: { id: true, lockToken: true },
    take: 20
  });
  for (const job of stale) {
    await prisma.$transaction(async (tx) => {
      await tx.studentCardExportPart.updateMany({
        where: { jobId: job.id, status: "PROCESSING" },
        data: {
          status: "RETRY",
          phase: "RETRY",
          progressUpdatedAt: new Date(),
          errorCode: "CARD_EXPORT_STALE_LOCK_RECOVERED",
          errorMessage: "The previous worker stopped before completing this part."
        }
      });
      await tx.studentCardExportJob.updateMany({
        where: { id: job.id, status: "PROCESSING", lockToken: job.lockToken, lockedAt: { lt: cutoff } },
        data: {
          status: "RETRY",
          phase: "RETRY",
          availableAt: new Date(),
          lockToken: null,
          lockedAt: null,
          lockedBy: null,
          currentPartNumber: null,
          progressUpdatedAt: new Date(),
          errorCode: "CARD_EXPORT_STALE_LOCK_RECOVERED",
          errorMessage: "The previous worker stopped before completing this part."
        }
      });
    });
    logger.warn({ event: "card_export.stale_lock_recovered", jobId: job.id }, "recovered stale card export job");
  }
}

async function claimCardExportJob() {
  if (!CARD_EXPORT_WORKER_ENABLED || !canRunLeaderWork()) return null;
  const candidates = await prisma.studentCardExportJob.findMany({
    where: {
      status: { in: ["QUEUED", "RETRY"] },
      availableAt: { lte: new Date() },
      lockToken: null
    },
    orderBy: [{ availableAt: "asc" }, { createdAt: "asc" }],
    take: 5
  });
  for (const candidate of candidates) {
    const lockToken = crypto.randomUUID();
    const claimed = await prisma.studentCardExportJob.updateMany({
      where: { id: candidate.id, status: candidate.status, lockToken: null },
      data: {
        status: "PROCESSING",
        phase: "LOADING_ASSETS",
        lockToken,
        lockedAt: new Date(),
        lockedBy: WORKER_OWNER_LABEL,
        progressUpdatedAt: new Date(),
        startedAt: candidate.startedAt || new Date(),
        attempts: { increment: 1 },
        errorCode: null,
        errorMessage: null
      }
    });
    if (claimed.count === 1) {
      const job = { id: candidate.id, lockToken };
      activeCardExportJobs.set(candidate.id, job);
      return job;
    }
  }
  return null;
}

async function processClaimedCardExportJob(job) {
  const endpoint = `${CARD_EXPORT_APP_BASE_URL}/api/internal/student-card-exports/process-next`;
  try {
    if (!(await verifyLeaseOwnership())) {
      await stopLeaderWork("lease-invalid-before-card-export-request");
      return;
    }
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Card-Export-Worker-Secret": CARD_EXPORT_WORKER_SECRET,
        "X-Notification-Worker-Lease-Id": WORKER_LEASE_ID,
        "X-Notification-Worker-Owner-Id": WORKER_OWNER_ID,
        "User-Agent": "centrix-card-export-worker/1"
      },
      body: JSON.stringify({ jobId: job.id, lockToken: job.lockToken }),
      signal: AbortSignal.timeout(CARD_EXPORT_WORKER_REQUEST_TIMEOUT_MS)
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      logger.error({
        event: "card_export.process_rejected",
        jobId: job.id,
        statusCode: response.status,
        code: payload?.code || "CARD_EXPORT_PROCESS_REJECTED"
      }, "card export processing endpoint rejected the job");
      return;
    }
    logger.info({
      event: payload?.data?.done ? "card_export.completed" : "card_export.part_completed",
      jobId: job.id,
      partNumber: payload?.data?.partNumber,
      completedParts: payload?.data?.completedParts,
      totalParts: payload?.data?.totalParts
    }, "card export job progressed");
  } catch (error) {
    logger.error({ event: "card_export.request_failed", error, jobId: job.id }, "card export processing request failed");
    await prisma.$transaction(async (tx) => {
      await tx.studentCardExportPart.updateMany({
        where: { jobId: job.id, status: "PROCESSING" },
        data: {
          status: "RETRY",
          phase: "RETRY",
          progressUpdatedAt: new Date(),
          errorCode: "CARD_EXPORT_WORKER_REQUEST_FAILED",
          errorMessage: "Worker request failed"
        }
      });
      await tx.studentCardExportJob.updateMany({
        where: { id: job.id, lockToken: job.lockToken, status: "PROCESSING" },
        data: {
          status: "RETRY",
          phase: "RETRY",
          availableAt: new Date(Date.now() + 5_000),
          lockToken: null,
          lockedAt: null,
          lockedBy: null,
          currentPartNumber: null,
          progressUpdatedAt: new Date(),
          errorCode: "CARD_EXPORT_WORKER_REQUEST_FAILED",
          errorMessage: "Worker request failed"
        }
      });
    });
  } finally {
    activeCardExportJobs.delete(job.id);
  }
}

async function processCardExportQueue() {
  if (!CARD_EXPORT_WORKER_ENABLED || !canRunLeaderWork() || cardExportQueueInFlight) return;
  cardExportQueueInFlight = true;
  try {
    if (!(await verifyLeaseOwnership())) {
      await stopLeaderWork("lease-invalid-before-card-export-claim");
      return;
    }
    await recoverStaleCardExportJobs();
    const job = await claimCardExportJob();
    if (!job || !canRunLeaderWork()) {
      if (job) await releaseActiveCardExportJobs("leadership changed before card export processing");
      return;
    }
    await processClaimedCardExportJob(job);
  } finally {
    cardExportQueueInFlight = false;
  }
}

function renderTemplate(template, variables) {
  return template.replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_match, key) => {
    const value = variables[key];
    return value === null || value === undefined || value === "" ? "-" : String(value);
  });
}

function normalizePhoneForWhatsApp(value) {
  let digits = String(value || "").replace(/\D/g, "");
  if (!digits) return null;
  if (digits.startsWith("00")) digits = digits.slice(2);
  if (digits.startsWith("0")) digits = `20${digits.slice(1)}`;
  if (digits.length === 10 && digits.startsWith("1")) digits = `20${digits}`;
  return /^\d{8,15}$/.test(digits) ? digits : null;
}

async function ensureSubscriptionNotificationDefaults(tenantId) {
  await Promise.all([
    prisma.notificationEventSetting.upsert({
      where: {
        tenantId_eventType: {
          tenantId,
          eventType: "SUBSCRIPTION_EXPIRING"
        }
      },
      update: {},
      create: {
        tenantId,
        eventType: "SUBSCRIPTION_EXPIRING",
        enabled: true,
        recipientMode: "PARENT_FIRST"
      }
    }),
    prisma.notificationTemplate.upsert({
      where: {
        tenantId_eventType_channel: {
          tenantId,
          eventType: "SUBSCRIPTION_EXPIRING",
          channel: "WHATSAPP"
        }
      },
      update: {},
      create: {
        tenantId,
        eventType: "SUBSCRIPTION_EXPIRING",
        channel: "WHATSAPP",
        body: DEFAULT_EVENT_TEMPLATES.SUBSCRIPTION_EXPIRING
      }
    }),
    prisma.notificationChannelSetting.upsert({
      where: {
        tenantId_channel: {
          tenantId,
          channel: "WHATSAPP"
        }
      },
      update: {},
      create: {
        tenantId,
        channel: "WHATSAPP"
      }
    })
  ]);
}

function resolveRecipients(mode, student) {
  const parentAddress = normalizePhoneForWhatsApp(student.parentPhone);
  const studentAddress = normalizePhoneForWhatsApp(student.phone);
  const recipients = [];
  const addParent = () => parentAddress && recipients.push({
    role: "PARENT",
    name: "ولي الأمر",
    phone: student.parentPhone,
    address: parentAddress
  });
  const addStudent = () => studentAddress && recipients.push({
    role: "STUDENT",
    name: student.fullName,
    phone: student.phone,
    address: studentAddress
  });

  if (mode === "PARENT_ONLY") addParent();
  else if (mode === "STUDENT_ONLY") addStudent();
  else if (mode === "PARENT_AND_STUDENT") {
    addParent();
    addStudent();
  } else if (parentAddress) addParent();
  else addStudent();

  const seen = new Set();
  return recipients.filter((recipient) => {
    if (seen.has(recipient.address)) return false;
    seen.add(recipient.address);
    return true;
  });
}

async function enqueueSubscriptionExpiring(subscription) {
  const tenantId = subscription.tenantId;
  await ensureSubscriptionNotificationDefaults(tenantId);
  const idempotencyKey = `subscription-expiring:${subscription.id}:${subscription.renewalDate.toISOString().slice(0, 10)}`;
  const existing = await prisma.notificationJob.findUnique({
    where: {
      tenantId_idempotencyKey: {
        tenantId,
        idempotencyKey
      }
    }
  });
  if (existing) return;

  const [eventSetting, template, channelSetting] = await Promise.all([
    prisma.notificationEventSetting.findUnique({
      where: { tenantId_eventType: { tenantId, eventType: "SUBSCRIPTION_EXPIRING" } }
    }),
    prisma.notificationTemplate.findUnique({
      where: { tenantId_eventType_channel: { tenantId, eventType: "SUBSCRIPTION_EXPIRING", channel: "WHATSAPP" } }
    }),
    prisma.notificationChannelSetting.findUnique({
      where: { tenantId_channel: { tenantId, channel: "WHATSAPP" } }
    })
  ]);
  if (!eventSetting?.enabled || !channelSetting?.enabled || !template) return;

  const remainingAmount = Math.max(0, Number(subscription.totalAmount) - Number(subscription.paidAmount));
  const variables = {
    studentName: subscription.student.fullName,
    parentName: "ولي الأمر",
    group: subscription.student.group?.name || "",
    grade: subscription.student.grade,
    amount: remainingAmount.toFixed(2),
    date: subscription.renewalDate.toISOString().slice(0, 10),
    time: "",
    reason: ""
  };
  const recipients = resolveRecipients(eventSetting.recipientMode, subscription.student);
  const renderedMessage = renderTemplate(template.body, variables);

  await prisma.notificationJob.create({
    data: {
      tenantId,
      eventType: "SUBSCRIPTION_EXPIRING",
      entityType: "StudentSubscription",
      entityId: subscription.id,
      variables,
      recipientContext: {
        student: {
          id: subscription.student.id,
          fullName: subscription.student.fullName,
          phone: subscription.student.phone,
          parentPhone: subscription.student.parentPhone,
          grade: subscription.student.grade,
          group: subscription.student.group?.name || ""
        }
      },
      status: recipients.length ? "QUEUED" : "SKIPPED",
      idempotencyKey,
      errorMessage: recipients.length ? null : "No valid recipient phone number",
      deliveries: recipients.length
        ? {
            create: recipients.map((recipient) => ({
              tenantId,
              eventType: "SUBSCRIPTION_EXPIRING",
              channel: "WHATSAPP",
              recipientRole: recipient.role,
              recipientName: recipient.name,
              recipientPhone: recipient.phone,
              recipientAddress: recipient.address,
              renderedMessage,
              status: "QUEUED",
              maxAttempts: channelSetting.retryMaxAttempts
            }))
          }
        : undefined
    }
  });
}

async function scanSubscriptionExpiring() {
  if (!canRunLeaderWork()) {
    return;
  }

  const now = new Date();
  const upper = new Date(now.getTime() + Math.max(1, SUBSCRIPTION_EXPIRING_DAYS) * 24 * 60 * 60 * 1000);
  const subscriptions = await prisma.studentSubscription.findMany({
    where: {
      renewalDate: {
        gte: now,
        lte: upper
      },
      student: {
        status: "ACTIVE"
      }
    },
    include: {
      student: {
        select: {
          id: true,
          tenantId: true,
          fullName: true,
          phone: true,
          parentPhone: true,
          grade: true,
          group: { select: { name: true } }
        }
      }
    },
    take: 500
  });

  for (const subscription of subscriptions) {
    await enqueueSubscriptionExpiring(subscription).catch((error) => {
      logger.error({ error, subscriptionId: subscription.id }, "failed to enqueue subscription expiring notification");
    });
  }
}

async function main() {
  logger.info({
    event: "worker.started",
    ownerLabel: WORKER_OWNER_LABEL,
    preferredLabel: PREFERRED_WORKER_LABEL,
    role: WORKER_ROLE,
    standbyOnly: STANDBY_ONLY,
    haEnabled: HA_ENABLED,
    leaseTtlMs: WORKER_LEASE_TTL_MS,
    leaseRenewMs: WORKER_LEASE_RENEW_MS
  }, "notification worker started");
  emitWorkerStatus("starting");

  await validateRuntimeReadiness();
  await maintainWorkerLease();
  if (HA_ENABLED) {
    leaseTimer = createLeaseHeartbeatScheduler({
      intervalMs: WORKER_LEASE_RENEW_MS,
      heartbeat: maintainWorkerLease,
      onError: (error) => logger.error({ event: "lease.scheduler_failed", error }, "worker lease scheduler failed")
    });
  }
}

async function shutdownWorker(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  logger.info({ event: "worker.stopping", signal, ownerLabel: WORKER_OWNER_LABEL }, "notification worker stopping");
  emitWorkerStatus("degraded", { reason: "stopping" });

  if (leaseTimer) {
    leaseTimer.stop();
    leaseTimer = null;
  }

  await stopLeaderWork("shutdown");
  await releaseWorkerLease().catch((error) => logger.error({ error }, "failed to release worker lease"));
  await prisma.$disconnect();
  process.exit(0);
}

process.on("SIGINT", () => {
  void shutdownWorker("SIGINT");
});

process.on("SIGTERM", () => {
  void shutdownWorker("SIGTERM");
});

main().catch(async (error) => {
  logger.error({ error }, "notification worker crashed");
  await stopLeaderWork("crash").catch((stopError) => logger.error({ error: stopError }, "failed to stop leader work after crash"));
  await releaseWorkerLease().catch((releaseError) => logger.error({ error: releaseError }, "failed to release worker lease after crash"));
  await prisma.$disconnect();
  process.exit(1);
});
