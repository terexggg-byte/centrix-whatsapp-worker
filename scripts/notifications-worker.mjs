import crypto from "crypto";
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
import { normalizeWorkerEnv } from "./env.mjs";
import { resolveWhatsAppVersion, sameWhatsAppVersion } from "./whatsapp-version.mjs";

normalizeWorkerEnv();

const { PrismaClient } = await import("@prisma/client");
const prisma = new PrismaClient();
const logger = pino({ level: process.env.NOTIFICATIONS_WORKER_LOG_LEVEL || "info" });
const sockets = new Map();
const connecting = new Set();
const pendingCredentialSaves = new Map();
const reconnectTimers = new Map();
const reconnectAttempts = new Map();
const intentionallyEndingSockets = new Set();
const leaderTimers = new Set();
let shuttingDown = false;
let leaderActive = false;
let leaseTimer = null;
let leaseCycleRunning = false;
let whatsAppVersionState = null;
let whatsAppVersionRefreshPromise = null;
let whatsAppVersionForcedAt = 0;

const POLL_INTERVAL_MS = Number(process.env.NOTIFICATIONS_WORKER_POLL_MS || 5000);
const SESSION_POLL_INTERVAL_MS = Number(process.env.NOTIFICATIONS_WORKER_SESSION_POLL_MS || 4000);
const SUBSCRIPTION_SCAN_INTERVAL_MS = Number(process.env.NOTIFICATIONS_SUBSCRIPTION_SCAN_MS || 60 * 60 * 1000);
const SUBSCRIPTION_EXPIRING_DAYS = Number(process.env.NOTIFICATIONS_SUBSCRIPTION_EXPIRING_DAYS || 7);
const WHATSAPP_RECONNECT_BASE_MS = Number(process.env.NOTIFICATIONS_WHATSAPP_RECONNECT_BASE_MS || 1500);
const WHATSAPP_RECONNECT_MAX_MS = Number(process.env.NOTIFICATIONS_WHATSAPP_RECONNECT_MAX_MS || 30000);
const WHATSAPP_VERSION_REFRESH_MS = Number(process.env.NOTIFICATIONS_WHATSAPP_VERSION_REFRESH_MS || 6 * 60 * 60 * 1000);
const WHATSAPP_VERSION_FORCE_COOLDOWN_MS = Number(process.env.NOTIFICATIONS_WHATSAPP_VERSION_FORCE_COOLDOWN_MS || 5 * 60 * 1000);
const WHATSAPP_VERSION_FETCH_TIMEOUT_MS = Number(process.env.NOTIFICATIONS_WHATSAPP_VERSION_FETCH_TIMEOUT_MS || 5000);
const WORKER_LEASE_ID = process.env.NOTIFICATIONS_WORKER_LEASE_ID || "notifications-worker";
const WORKER_OWNER_ID = process.env.NOTIFICATIONS_WORKER_INSTANCE_ID || crypto.randomUUID();
const WORKER_OWNER_LABEL = inferWorkerLabel();
const PREFERRED_WORKER_LABEL = process.env.NOTIFICATIONS_WORKER_PREFERRED_LABEL || "northflank";
const HA_ENABLED = parseBoolean(process.env.NOTIFICATIONS_WORKER_HA_ENABLED, true);
const WORKER_LEASE_TTL_MS = positiveNumber(process.env.NOTIFICATIONS_WORKER_LEASE_TTL_MS, 45 * 1000);
const WORKER_LEASE_RENEW_MS = Math.min(
  positiveNumber(process.env.NOTIFICATIONS_WORKER_LEASE_RENEW_MS, 10 * 1000),
  Math.max(1000, Math.floor(WORKER_LEASE_TTL_MS / 2))
);
const WORKER_PREEMPT_GRACE_MS = positiveNumber(
  process.env.NOTIFICATIONS_WORKER_PREEMPT_GRACE_MS,
  WORKER_LEASE_RENEW_MS + 2500
);

const DEFAULT_EVENT_TEMPLATES = {
  SUBSCRIPTION_EXPIRING:
    "اشتراك الطالب {{studentName}} يقترب من الانتهاء بتاريخ {{date}}. المبلغ المستحق: {{amount}}."
};

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

  if ((!force && cachedIsFresh) || forceIsCoolingDown) return whatsAppVersionState;
  if (whatsAppVersionRefreshPromise) return whatsAppVersionRefreshPromise;
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
    whatsAppVersionState = {
      version: resolved.version,
      source: resolved.source,
      verified: resolved.verified,
      resolvedAt: Date.now()
    };
    logger[resolved.verified ? "info" : "warn"]({
      source: resolved.source,
      verified: resolved.verified,
      revision: resolved.version[2],
      resolutionErrors: resolved.errors.length
    }, resolved.verified
      ? "resolved current WhatsApp Web protocol version"
      : "using cached or package WhatsApp Web protocol version");
    return whatsAppVersionState;
  })();

  try {
    return await whatsAppVersionRefreshPromise;
  } finally {
    whatsAppVersionRefreshPromise = null;
  }
}

function parseBoolean(value, defaultValue = false) {
  if (value === undefined || value === null || value === "") {
    return defaultValue;
  }
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function positiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

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
  const raw = (
    process.env.WHATSAPP_AUTH_ENCRYPTION_KEY
    || process.env.NOTIFICATION_AUTH_ENCRYPTION_KEY
    || process.env.SESSION_SECRET
    || process.env.NEXTAUTH_SECRET
    || process.env.JWT_SECRET
    || ""
  ).trim();

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

function leaseHeartbeatData(now = new Date()) {
  return {
    ownerId: WORKER_OWNER_ID,
    ownerLabel: WORKER_OWNER_LABEL,
    heartbeatAt: now,
    expiresAt: new Date(now.getTime() + WORKER_LEASE_TTL_MS)
  };
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
      logger.debug({ error, ownerId: WORKER_OWNER_ID }, "worker lease create raced with another worker");
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
    data: ownedByThisWorker ? heartbeatData : ownershipData
  });

  if (updated.count !== 1) {
    return { acquired: false, reason: "update-raced" };
  }

  return {
    acquired: true,
    reason: shouldPreemptForPreferredWorker ? "preferred-preempted" : ownedByThisWorker ? "renewed" : "expired"
  };
}

async function renewWorkerLease() {
  const now = new Date();
  const updated = await prisma.notificationWorkerLease.updateMany({
    where: {
      id: WORKER_LEASE_ID,
      ownerId: WORKER_OWNER_ID
    },
    data: {
      heartbeatAt: now,
      expiresAt: new Date(now.getTime() + WORKER_LEASE_TTL_MS)
    }
  });
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
  logger.info({
    ownerId: WORKER_OWNER_ID,
    ownerLabel: WORKER_OWNER_LABEL,
    reason,
    haEnabled: HA_ENABLED
  }, "notification worker became leader");

  void pollConnectionRequests().catch((error) => logger.error({ error }, "connection poll failed"));
  void processQueue().catch((error) => logger.error({ error }, "queue processing failed"));
  void scanSubscriptionExpiring().catch((error) => logger.error({ error }, "subscription scan failed"));

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

async function stopLeaderWork(reason) {
  if (!leaderActive) {
    return;
  }

  leaderActive = false;
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

  logger.warn({
    ownerId: WORKER_OWNER_ID,
    ownerLabel: WORKER_OWNER_LABEL,
    reason
  }, "notification worker left leader mode");
}

async function becomeLeader(reason) {
  const delayMs = reason === "preferred-preempted" ? WORKER_PREEMPT_GRACE_MS : 0;
  if (delayMs > 0) {
    logger.info({
      ownerId: WORKER_OWNER_ID,
      ownerLabel: WORKER_OWNER_LABEL,
      delayMs
    }, "preferred worker acquired lease; waiting for previous worker to yield");
    await sleep(delayMs);
    if (!(await renewWorkerLease())) {
      logger.warn({ ownerId: WORKER_OWNER_ID, ownerLabel: WORKER_OWNER_LABEL }, "preferred worker lost lease during handoff");
      return;
    }
  }

  startLeaderWork(reason);
}

async function maintainWorkerLease() {
  if (leaseCycleRunning || shuttingDown) {
    return;
  }

  leaseCycleRunning = true;
  try {
    if (!HA_ENABLED) {
      startLeaderWork("ha-disabled");
      return;
    }

    if (leaderActive) {
      const renewed = await renewWorkerLease();
      if (!renewed) {
        await stopLeaderWork("lease-lost");
      }
      return;
    }

    const result = await tryAcquireWorkerLease();
    if (result.acquired) {
      await becomeLeader(result.reason);
      return;
    }

    logger.debug({
      ownerId: WORKER_OWNER_ID,
      ownerLabel: WORKER_OWNER_LABEL,
      leaseOwnerLabel: result.ownerLabel,
      reason: result.reason
    }, "notification worker standby; lease held by another worker");
  } catch (error) {
    logger.error({ error, ownerId: WORKER_OWNER_ID, ownerLabel: WORKER_OWNER_LABEL }, "worker lease cycle failed");
    await stopLeaderWork("lease-error");
  } finally {
    leaseCycleRunning = false;
  }
}

async function pauseActiveDeliveries(tenantId, errorMessage = "WhatsApp disconnected") {
  await prisma.notificationDelivery.updateMany({
    where: {
      tenantId,
      channel: "WHATSAPP",
      status: { in: ["QUEUED", "RETRY", "PROCESSING"] }
    },
    data: {
      status: "PAUSED",
      errorMessage
    }
  });
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
      logger,
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
              availableAt: new Date()
            }
          });
          logger.info({ tenantId, phoneNumber, hasMe: Boolean(state.creds?.me?.id) }, "WhatsApp connected");
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
            : update.lastDisconnect?.error?.message || null;
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
  await prisma.notificationDelivery.update({
    where: { id: delivery.id },
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
  const message = error instanceof Error ? error.message : String(error);

  if (retryCount >= maxAttempts) {
    await prisma.notificationDelivery.update({
      where: { id: delivery.id },
      data: {
        status: "FAILED",
        retryCount,
        errorMessage: message,
        lockToken: null,
        lockedAt: null,
        processingStartedAt: null
      }
    });
    await refreshJobStatus(delivery.jobId);
    return;
  }

  const baseDelay = Math.max(30, Number(setting?.retryBaseDelaySeconds ?? 300));
  const jitter = Math.floor(Math.random() * baseDelay);
  const delaySeconds = baseDelay * Math.pow(2, Math.max(0, retryCount - 1)) + jitter;
  await prisma.notificationDelivery.update({
    where: { id: delivery.id },
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
    where: {
      id: delivery.id,
      status: "PROCESSING",
      lockToken: delivery.lockToken
    },
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

async function processDelivery(delivery) {
  if (!canRunLeaderWork()) {
    await releaseDeliveryForLeadershipChange(delivery);
    return;
  }

  const session = await prisma.whatsAppSession.findUnique({
    where: { tenantId: delivery.tenantId }
  });
  if (session?.status !== "CONNECTED") {
    await prisma.notificationDelivery.update({
      where: { id: delivery.id },
      data: {
        status: "PAUSED",
        lockToken: null,
        lockedAt: null,
        processingStartedAt: null,
        errorMessage: "WhatsApp disconnected"
      }
    });
    await refreshJobStatus(delivery.jobId);
    return;
  }

  const socket = sockets.get(delivery.tenantId);
  if (!socket) {
    await connectTenant(delivery.tenantId);
    await prisma.notificationDelivery.update({
      where: { id: delivery.id },
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
    await prisma.notificationDelivery.update({
      where: { id: delivery.id },
      data: {
        status: "PAUSED",
        lockToken: null,
        lockedAt: null,
        processingStartedAt: null,
        errorMessage: "WhatsApp notifications disabled"
      }
    });
    await refreshJobStatus(delivery.jobId);
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

  try {
    const jid = `${delivery.recipientAddress}@s.whatsapp.net`;
    const result = await socket.sendMessage(jid, { text: delivery.renderedMessage });
    await prisma.notificationDelivery.update({
      where: { id: delivery.id },
      data: {
        status: "SENT",
        providerMessageId: result?.key?.id || null,
        sentAt: new Date(),
        errorMessage: null,
        lockToken: null,
        lockedAt: null,
        processingStartedAt: null
      }
    });
    await refreshJobStatus(delivery.jobId);
  } catch (error) {
    logger.error({ error, deliveryId: delivery.id }, "failed to send WhatsApp notification");
    await failOrRetry(delivery, error, setting);
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
      claimed.push({ ...row, lockToken });
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
    if (!canRunLeaderWork()) {
      await releaseDeliveryForLeadershipChange(delivery);
      continue;
    }
    await processDelivery(delivery);
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
    ownerId: WORKER_OWNER_ID,
    ownerLabel: WORKER_OWNER_LABEL,
    haEnabled: HA_ENABLED,
    leaseTtlMs: WORKER_LEASE_TTL_MS,
    leaseRenewMs: WORKER_LEASE_RENEW_MS
  }, "notification worker started");

  await maintainWorkerLease();
  if (HA_ENABLED) {
    leaseTimer = setInterval(() => {
      void maintainWorkerLease();
    }, WORKER_LEASE_RENEW_MS);
  }
}

async function shutdownWorker(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  logger.info({ signal, ownerId: WORKER_OWNER_ID, ownerLabel: WORKER_OWNER_LABEL }, "notification worker stopping");

  if (leaseTimer) {
    clearInterval(leaseTimer);
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
