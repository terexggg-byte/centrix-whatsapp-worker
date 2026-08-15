import test from "node:test";
import assert from "node:assert/strict";
import { fork, execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import {
  readPostgresWatchdogSnapshot,
  runLeaseGate,
  runWatchdog
} from "../scripts/notifications-self-handoff.mjs";

const execFileAsync = promisify(execFile);
const databaseUrl = String(process.env.TEST_DATABASE_URL || "").trim();
const rcDir = String(process.env.CENTRIX_RC_DIR || "").trim();
const psql = String(process.env.TEST_PSQL_BIN || "psql").trim();
const releaseSha = "2c920190719a4d6634ecfdc809817d5905d1a2d7";
const statusType = "centrix.notifications-worker.status.v1";

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function sql(command) {
  const { stdout } = await execFileAsync(psql, [
    `--dbname=${databaseUrl}`,
    "--no-psqlrc",
    "--set=ON_ERROR_STOP=1",
    "--tuples-only",
    "--no-align",
    "--command",
    command
  ], { maxBuffer: 1024 * 1024 });
  return stdout.trim();
}

async function leaseSnapshot(leaseId) {
  const raw = await sql(`
    WITH db AS (SELECT transaction_timestamp() AS now)
    SELECT json_build_object(
      'dbNow', db.now,
      'rows', COUNT(lease.*),
      'activeLeaders', COUNT(lease.*) FILTER (WHERE lease."expiresAt" > db.now),
      'ownerId', MAX(lease."ownerId"),
      'ownerLabel', MAX(lease."ownerLabel"),
      'heartbeatAt', MAX(lease."heartbeatAt"),
      'expiresAt', MAX(lease."expiresAt"),
      'release', MAX(lease."capabilities"->>'release')
    )::text
    FROM db
    LEFT JOIN "NotificationWorkerLease" lease ON lease."id" = '${leaseId.replaceAll("'", "''")}'
    GROUP BY db.now;
  `);
  return JSON.parse(raw.split("\n").filter(Boolean).at(-1));
}

function spawnRcWorker({ leaseId, ownerId }) {
  const messages = [];
  const waiters = [];
  const child = fork(path.join(rcDir, "scripts", "notifications-worker.mjs"), [], {
    cwd: rcDir,
    env: {
      ...process.env,
      NODE_ENV: "production",
      DATA_SOURCE: "prisma",
      DATABASE_URL: databaseUrl,
      RELEASE_SHA: releaseSha,
      WHATSAPP_AUTH_ENCRYPTION_KEY: "isolated-test-only-encryption-key",
      NOTIFICATIONS_WORKER_LOG_LEVEL: "silent",
      NOTIFICATIONS_WORKER_HA_ENABLED: "true",
      NOTIFICATIONS_WORKER_ROLE: "primary",
      NOTIFICATIONS_WORKER_INSTANCE_ID: ownerId,
      NOTIFICATIONS_WORKER_INSTANCE_LABEL: "github-actions",
      NOTIFICATIONS_WORKER_PREFERRED_LABEL: "github-actions",
      NOTIFICATIONS_WORKER_PREFERRED_PREEMPTION_ENABLED: "false",
      NOTIFICATIONS_WORKER_STANDBY_ONLY: "false",
      NOTIFICATIONS_WORKER_LEASE_ID: leaseId,
      NOTIFICATIONS_WORKER_LEASE_TTL_MS: "2400",
      NOTIFICATIONS_WORKER_LEASE_RENEW_MS: "500",
      NOTIFICATIONS_WORKER_PREEMPT_GRACE_MS: "750",
      NOTIFICATIONS_WORKER_POLL_MS: "60000",
      NOTIFICATIONS_WORKER_SESSION_POLL_MS: "60000",
      NOTIFICATIONS_SUBSCRIPTION_SCAN_MS: "60000",
      CARD_EXPORT_WORKER_ENABLED: "false"
    },
    stdio: ["ignore", "ignore", "ignore", "ipc"]
  });
  child.on("message", (message) => {
    if (message?.type !== statusType) return;
    messages.push(message);
    for (const waiter of [...waiters]) {
      if (waiter.state !== message.state) continue;
      clearTimeout(waiter.timeout);
      waiters.splice(waiters.indexOf(waiter), 1);
      waiter.resolve(message);
    }
  });
  return {
    child,
    messages,
    waitFor(state, timeoutMs = 15_000) {
      const existing = messages.find((message) => message.state === state);
      if (existing) return Promise.resolve(existing);
      return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          waiters.splice(waiters.indexOf(waiter), 1);
          reject(new Error(`Timed out waiting for ${ownerId} state ${state}.`));
        }, timeoutMs);
        timeout.unref();
        const waiter = {
          state,
          resolve,
          timeout
        };
        waiters.push(waiter);
      });
    }
  };
}

async function stopWorker(worker) {
  if (!worker || worker.child.exitCode !== null || worker.child.signalCode !== null) return;
  await new Promise((resolve) => {
    const timeout = setTimeout(() => {
      if (worker.child.exitCode === null && worker.child.signalCode === null) worker.child.kill("SIGKILL");
    }, 5_000);
    worker.child.once("exit", () => {
      clearTimeout(timeout);
      resolve();
    });
    worker.child.kill("SIGTERM");
  });
}

async function collectThreeRenewals(leaseId, ownerId) {
  const heartbeats = new Set();
  const deadline = Date.now() + 8_000;
  while (Date.now() < deadline && heartbeats.size < 4) {
    const snapshot = await leaseSnapshot(leaseId);
    assert.ok(snapshot.activeLeaders <= 1, "PostgreSQL must never report more than one active lease owner");
    if (snapshot.ownerId === ownerId && snapshot.heartbeatAt) heartbeats.add(snapshot.heartbeatAt);
    await delay(225);
  }
  assert.ok(heartbeats.size >= 4, `expected acquisition plus three renewals for ${ownerId}`);
  return heartbeats;
}

test("PostgreSQL-only gate hands off after expiration and an atomic race elects one successor", {
  skip: !databaseUrl || !rcDir,
  timeout: 45_000
}, async () => {
  const leaseId = `notifications-self-handoff-${Date.now()}`;
  let predecessor;
  let candidateA;
  let candidateB;
  try {
    predecessor = spawnRcWorker({ leaseId, ownerId: "isolated-predecessor" });
    await predecessor.waitFor("leader");
    await collectThreeRenewals(leaseId, "isolated-predecessor");

    let gateOpened = false;
    const gatePromise = runLeaseGate({
      env: {
        ...process.env,
        CENTRIX_RC_DIR: rcDir,
        DATABASE_URL: databaseUrl,
        NOTIFICATIONS_WORKER_LEASE_ID: leaseId,
        NOTIFICATIONS_WORKER_LEASE_RENEW_MS: "500",
        SELF_HANDOFF_GATE_TIMEOUT_MS: "12000",
        SELF_HANDOFF_GATE_RETRY_MS: "100",
        SELF_HANDOFF_GATE_PROBE_TIMEOUT_MS: "3000"
      }
    }).then((value) => {
      gateOpened = true;
      return value;
    });

    await delay(1_200);
    assert.equal(gateOpened, false, "Jobs readiness must not open the PostgreSQL lease gate");
    const whileLeading = await leaseSnapshot(leaseId);
    assert.equal(whileLeading.activeLeaders, 1);
    assert.equal(whileLeading.ownerId, "isolated-predecessor");

    await stopWorker(predecessor);
    predecessor = null;
    const gate = await gatePromise;
    assert.equal(gate.open, true);
    const expired = await leaseSnapshot(leaseId);
    assert.equal(expired.activeLeaders, 0);
    assert.equal(expired.ownerId, "isolated-predecessor");

    candidateA = spawnRcWorker({ leaseId, ownerId: "isolated-handoff" });
    candidateB = spawnRcWorker({ leaseId, ownerId: "isolated-watchdog" });
    const winner = await Promise.race([
      candidateA.waitFor("leader").then(() => candidateA),
      candidateB.waitFor("leader").then(() => candidateB)
    ]);
    const loser = winner === candidateA ? candidateB : candidateA;
    await loser.waitFor("standby");
    assert.equal(loser.messages.some((message) => message.state === "leader"), false);

    const winnerOwner = winner === candidateA ? "isolated-handoff" : "isolated-watchdog";
    await collectThreeRenewals(leaseId, winnerOwner);
    const final = await leaseSnapshot(leaseId);
    assert.equal(final.activeLeaders, 1);
    assert.equal(final.ownerId, winnerOwner);
    assert.equal(final.ownerLabel, "github-actions");
    assert.equal(final.release, releaseSha);
    assert.equal(final.rows, 1);
  } finally {
    await Promise.all([stopWorker(predecessor), stopWorker(candidateA), stopWorker(candidateB)]);
    if (databaseUrl) {
      await sql(`DELETE FROM "NotificationWorkerLease" WHERE "id" = '${leaseId.replaceAll("'", "''")}';`);
    }
  }
});

test("watchdog dispatches once only when PostgreSQL proves zero leaders and zero PROCESSING", {
  skip: !databaseUrl || !rcDir,
  timeout: 15_000
}, async () => {
  const requests = [];
  const fetchImpl = async (url, options = {}) => {
    const parsed = new URL(url);
    requests.push({ method: options.method || "GET", path: `${parsed.pathname}${parsed.search}` });
    if ((options.method || "GET") === "POST") {
      return new Response(JSON.stringify({ workflow_run_id: 501 }), {
        status: 200,
        headers: { "content-type": "application/json" }
      });
    }
    return new Response(JSON.stringify({ workflow_runs: [] }), {
      status: 200,
      headers: { "content-type": "application/json" }
    });
  };
  const result = await runWatchdog({
    env: {
      ...process.env,
      DATABASE_URL: databaseUrl,
      PSQL_BIN: psql,
      RELEASE_SHA: releaseSha,
      GH_TOKEN: "isolated-test-token",
      GITHUB_REPOSITORY: "example/worker",
      GITHUB_API_URL: "https://api.example.test",
      SELF_HANDOFF_WORKFLOW_FILE: "notifications-worker-self-handoff.yml",
      SELF_HANDOFF_WORKFLOW_REF: "notifications-self-handoff-v1",
      NOTIFICATIONS_WORKER_LEASE_ID: "notifications-worker-watchdog-isolated",
      NOTIFICATIONS_WORKER_INSTANCE_LABEL: "github-actions"
    },
    fetchImpl
  });
  assert.equal(result.action, "dispatch");
  assert.equal(result.runId, 501);
  assert.equal(requests.filter((request) => request.method === "POST").length, 1);
});

test("watchdog database outage is fail-closed before any GitHub orchestration", async () => {
  let githubCalled = false;
  await assert.rejects(
    () => runWatchdog({
      env: {
        DATABASE_URL: "postgresql://isolated:isolated@127.0.0.1:1/unavailable",
        PSQL_BIN: "/usr/bin/false",
        RELEASE_SHA: releaseSha,
        GH_TOKEN: "isolated-test-token",
        GITHUB_REPOSITORY: "example/worker",
        SELF_HANDOFF_WORKFLOW_FILE: "notifications-worker-self-handoff.yml",
        SELF_HANDOFF_WORKFLOW_REF: "notifications-self-handoff-v1"
      },
      fetchImpl: async () => {
        githubCalled = true;
        return new Response("{}", { status: 200 });
      }
    }),
    /watchdog query failed/
  );
  assert.equal(githubCalled, false);
});

test("watchdog snapshot is PostgreSQL-time based and read-only", {
  skip: !databaseUrl,
  timeout: 10_000
}, async () => {
  const leaseId = `notifications-watchdog-snapshot-${Date.now()}`;
  try {
    await sql(`
      INSERT INTO "NotificationWorkerLease"
        ("id", "ownerId", "ownerLabel", "heartbeatAt", "expiresAt", "capabilities", "startedAt", "updatedAt")
      VALUES
        ('${leaseId}', 'snapshot-owner', 'github-actions', transaction_timestamp(),
         transaction_timestamp() + INTERVAL '30 seconds',
         jsonb_build_object('release', '${releaseSha}'), transaction_timestamp(), transaction_timestamp());
    `);
    const before = await sql(`SELECT COUNT(*) FROM "NotificationWorkerLease" WHERE "id" = '${leaseId}';`);
    const snapshot = await readPostgresWatchdogSnapshot({
      env: {
        DATABASE_URL: databaseUrl,
        PSQL_BIN: psql,
        NOTIFICATIONS_WORKER_LEASE_ID: leaseId
      }
    });
    const after = await sql(`SELECT COUNT(*) FROM "NotificationWorkerLease" WHERE "id" = '${leaseId}';`);
    assert.equal(snapshot.active, true);
    assert.equal(snapshot.ownerLabel, "github-actions");
    assert.equal(snapshot.capabilities.release, releaseSha);
    assert.equal(snapshot.processing, 0);
    assert.equal(before, after);
  } finally {
    await sql(`DELETE FROM "NotificationWorkerLease" WHERE "id" = '${leaseId}';`);
  }
});
