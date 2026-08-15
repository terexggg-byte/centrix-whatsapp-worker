import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  GitHubActionsClient,
  evaluateWatchdog,
  runWorkerSession,
  waitForOrchestrationReadiness
} from "../scripts/notifications-self-handoff.mjs";

const releaseSha = "2c920190719a4d6634ecfdc809817d5905d1a2d7";
const orchestratorSha = "a".repeat(40);

function createGitHubFixture(handler) {
  const requests = [];
  return {
    apiUrl: "https://api.example.test",
    requests,
    fetchImpl: async (url, options = {}) => {
      const parsed = new URL(url);
      const request = {
        method: options.method || "GET",
        url: `${parsed.pathname}${parsed.search}`,
        body: options.body || ""
      };
      requests.push(request);
      const result = await handler(request);
      return new Response(result.body === undefined ? "" : JSON.stringify(result.body), {
        status: result.status || 200,
        headers: { "content-type": "application/json" }
      });
    }
  };
}

async function makeFakeRc(workerSource) {
  const directory = await mkdtemp(path.join(os.tmpdir(), "centrix-handoff-worker-"));
  await mkdir(path.join(directory, "scripts"), { recursive: true });
  await writeFile(path.join(directory, "scripts", "notifications-worker.mjs"), workerSource);
  return directory;
}

function sessionEnv(rcDir, apiUrl, overrides = {}) {
  return {
    CENTRIX_RC_DIR: rcDir,
    RELEASE_SHA: releaseSha,
    GH_TOKEN: "test-token",
    GITHUB_REPOSITORY: "example/worker",
    GITHUB_API_URL: apiUrl,
    GITHUB_RUN_ID: "1234",
    GITHUB_RUN_ATTEMPT: "1",
    SELF_HANDOFF_WORKFLOW_FILE: "notifications-worker-self-handoff.yml",
    SELF_HANDOFF_WORKFLOW_REF: "notifications-self-handoff-v1",
    SELF_HANDOFF_ORCHESTRATOR_SHA: orchestratorSha,
    SELF_HANDOFF_REMAINING: "unlimited",
    SELF_HANDOFF_START_MS: "30",
    SELF_HANDOFF_TARGET_MS: "70",
    SELF_HANDOFF_MAX_MS: "150",
    SELF_HANDOFF_READINESS_TIMEOUT_MS: "1000",
    SELF_HANDOFF_POLL_MS: "5",
    SELF_HANDOFF_GRACEFUL_TIMEOUT_MS: "1000",
    SELF_HANDOFF_STARTUP_TIMEOUT_MS: "1000",
    ...overrides
  };
}

const leaderWorker = `
const type = "centrix.notifications-worker.status.v1";
process.send?.({ type, state: "leader", at: new Date().toISOString() });
const timer = setInterval(() => process.send?.({ type, state: "leader", at: new Date().toISOString() }), 20);
process.on("SIGTERM", () => { clearInterval(timer); process.exit(0); });
`;

test("workflow dispatch returns a traceable run and Jobs API is readiness only", async () => {
  let jobPolls = 0;
  const fixture = createGitHubFixture(({ method, url }) => {
    if (method === "POST" && url.includes("/dispatches")) {
      return { body: { workflow_run_id: 99, html_url: "https://example.test/runs/99" } };
    }
    if (url === "/repos/example/worker/actions/runs/99") {
      return { body: { id: 99, head_sha: orchestratorSha, status: "in_progress" } };
    }
    if (url.startsWith("/repos/example/worker/actions/runs/99/jobs")) {
      jobPolls += 1;
      return {
        body: {
          jobs: [{
            steps: [{
              name: "Wait for predecessor lease expiration",
              status: jobPolls > 1 ? "in_progress" : "queued"
            }]
          }]
        }
      };
    }
    return { status: 404, body: {} };
  });
  try {
    const github = new GitHubActionsClient({
      token: "test-token",
      repository: "example/worker",
      apiUrl: fixture.apiUrl,
      fetchImpl: fixture.fetchImpl
    });
    const dispatched = await github.dispatchSession({
      workflowFile: "notifications-worker-self-handoff.yml",
      ref: "notifications-self-handoff-v1",
      sessionId: "session-1",
      predecessorRunId: "1",
      trigger: "handoff"
    });
    assert.equal(dispatched.runId, 99);
    const ready = await waitForOrchestrationReadiness({
      github,
      runId: 99,
      expectedOrchestratorSha: orchestratorSha,
      timeoutMs: 1_000,
      pollMs: 1
    });
    assert.equal(ready.orchestrationReady, true);
    assert.equal(jobPolls, 2);
  } finally {}
});

test("Jobs API readiness rejects an unexpected orchestration SHA", async () => {
  const github = {
    getRun: async () => ({ head_sha: "b".repeat(40), status: "in_progress" }),
    getRunJobs: async () => []
  };
  await assert.rejects(
    () => waitForOrchestrationReadiness({
      github,
      runId: 7,
      expectedOrchestratorSha: orchestratorSha,
      timeoutMs: 100,
      pollMs: 1
    }),
    /SHA mismatch/
  );
});

test("a leader dispatches one successor and shuts down gracefully only after orchestration readiness", async () => {
  const fixture = createGitHubFixture(({ method, url }) => {
    if (method === "POST" && url.includes("/dispatches")) {
      return { body: { workflow_run_id: 88 } };
    }
    if (url === "/repos/example/worker/actions/runs/88") {
      return { body: { id: 88, head_sha: orchestratorSha, status: "in_progress" } };
    }
    if (url.startsWith("/repos/example/worker/actions/runs/88/jobs")) {
      return {
        body: {
          jobs: [{ steps: [{ name: "Wait for predecessor lease expiration", status: "in_progress" }] }]
        }
      };
    }
    return { status: 404, body: {} };
  });
  const rcDir = await makeFakeRc(leaderWorker);
  try {
    const result = await runWorkerSession({
      env: sessionEnv(rcDir, fixture.apiUrl, { SELF_HANDOFF_REMAINING: "1" }),
      fetchImpl: fixture.fetchImpl
    });
    assert.equal(result.outcome, "handed-off");
    assert.equal(result.successorRunId, 88);
    assert.equal(result.orchestrationReady, true);
    assert.equal(result.forced, false);
    assert.equal(fixture.requests.filter((request) => request.method === "POST").length, 1);
    const dispatchBody = JSON.parse(fixture.requests.find((request) => request.method === "POST").body);
    assert.equal(dispatchBody.inputs.handoffs_remaining, "0");
  } finally {
    await rm(rcDir, { recursive: true, force: true });
  }
});

test("the terminal Canary session stops gracefully without dispatching a third run", async () => {
  const fixture = createGitHubFixture(() => ({ status: 500, body: {} }));
  const rcDir = await makeFakeRc(leaderWorker);
  try {
    const result = await runWorkerSession({
      env: sessionEnv(rcDir, fixture.apiUrl, { SELF_HANDOFF_REMAINING: "0" }),
      fetchImpl: fixture.fetchImpl
    });
    assert.equal(result.outcome, "terminal-session-complete");
    assert.equal(result.hadLeadership, true);
    assert.equal(result.forced, false);
    assert.equal(fixture.requests.length, 0);
  } finally {
    await rm(rcDir, { recursive: true, force: true });
  }
});

test("GitHub dispatch failure never grants leadership and the current worker stops at the safe maximum", async () => {
  const fixture = createGitHubFixture(() => ({ status: 500, body: { message: "failed" } }));
  const rcDir = await makeFakeRc(leaderWorker);
  try {
    const result = await runWorkerSession({
      env: sessionEnv(rcDir, fixture.apiUrl),
      fetchImpl: fixture.fetchImpl
    });
    assert.equal(result.outcome, "maximum-boundary");
    assert.equal(result.orchestrationReady, false);
    assert.match(result.handoffError, /HTTP 500/);
    assert.equal(result.forced, false);
  } finally {
    await rm(rcDir, { recursive: true, force: true });
  }
});

test("a worker that observes PostgreSQL standby exits as a superseded candidate without dispatching", async () => {
  const standbyWorker = `
  process.send?.({ type: "centrix.notifications-worker.status.v1", state: "standby", at: new Date().toISOString() });
  process.on("SIGTERM", () => process.exit(0));
  setInterval(() => {}, 1000);
  `;
  const fixture = createGitHubFixture(() => ({ status: 500, body: {} }));
  const rcDir = await makeFakeRc(standbyWorker);
  try {
    const result = await runWorkerSession({
      env: sessionEnv(rcDir, fixture.apiUrl),
      fetchImpl: fixture.fetchImpl
    });
    assert.equal(result.outcome, "superseded");
    assert.equal(result.hadLeadership, false);
    assert.equal(fixture.requests.length, 0);
  } finally {
    await rm(rcDir, { recursive: true, force: true });
  }
});

test("a leader crash before handoff fails the session instead of creating an unowned successor", async () => {
  const crashingWorker = `
  process.send?.({ type: "centrix.notifications-worker.status.v1", state: "leader", at: new Date().toISOString() });
  setTimeout(() => process.exit(17), 10);
  `;
  const fixture = createGitHubFixture(() => ({ status: 500, body: {} }));
  const rcDir = await makeFakeRc(crashingWorker);
  try {
    await assert.rejects(
      () => runWorkerSession({
        env: sessionEnv(rcDir, fixture.apiUrl, {
          SELF_HANDOFF_START_MS: "100",
          SELF_HANDOFF_TARGET_MS: "150",
          SELF_HANDOFF_MAX_MS: "200"
        }),
        fetchImpl: fixture.fetchImpl
      }),
      /exited unexpectedly/
    );
    assert.equal(fixture.requests.length, 0);
  } finally {
    await rm(rcDir, { recursive: true, force: true });
  }
});

test("a leader crash after dispatch still fails closed while the prepared successor remains lease-gated", async () => {
  const crashingWorker = `
  process.send?.({ type: "centrix.notifications-worker.status.v1", state: "leader", at: new Date().toISOString() });
  setTimeout(() => process.exit(19), 75);
  `;
  const fixture = createGitHubFixture(({ method, url }) => {
    if (method === "POST" && url.includes("/dispatches")) return { body: { workflow_run_id: 77 } };
    if (url === "/repos/example/worker/actions/runs/77") {
      return { body: { id: 77, head_sha: orchestratorSha, status: "in_progress" } };
    }
    if (url.startsWith("/repos/example/worker/actions/runs/77/jobs")) {
      return { body: { jobs: [{ steps: [{ name: "Wait for predecessor lease expiration", status: "queued" }] }] } };
    }
    return { status: 404, body: {} };
  });
  const rcDir = await makeFakeRc(crashingWorker);
  try {
    await assert.rejects(
      () => runWorkerSession({
        env: sessionEnv(rcDir, fixture.apiUrl, {
          SELF_HANDOFF_START_MS: "20",
          SELF_HANDOFF_TARGET_MS: "120",
          SELF_HANDOFF_MAX_MS: "180"
        }),
        fetchImpl: fixture.fetchImpl
      }),
      /exited unexpectedly/
    );
    assert.equal(fixture.requests.filter((request) => request.method === "POST").length, 1);
  } finally {
    await rm(rcDir, { recursive: true, force: true });
  }
});

test("external SIGTERM is propagated to the RC worker for graceful shutdown", async () => {
  const fixture = createGitHubFixture(() => ({ status: 500, body: {} }));
  const rcDir = await makeFakeRc(leaderWorker);
  const signalTarget = new EventEmitter();
  try {
    const session = runWorkerSession({
      env: sessionEnv(rcDir, fixture.apiUrl, {
        SELF_HANDOFF_START_MS: "1000",
        SELF_HANDOFF_TARGET_MS: "1100",
        SELF_HANDOFF_MAX_MS: "1200"
      }),
      fetchImpl: fixture.fetchImpl,
      signalTarget
    });
    await new Promise((resolve) => setTimeout(resolve, 50));
    signalTarget.emit("SIGTERM");
    const result = await session;
    assert.equal(result.outcome, "external-stop");
    assert.equal(result.signal, "SIGTERM");
    assert.equal(result.forced, false);
    assert.equal(fixture.requests.length, 0);
  } finally {
    await rm(rcDir, { recursive: true, force: true });
  }
});

test("watchdog decisions use GitHub metadata only for orchestration deduplication", () => {
  const dbNow = "2026-08-15T06:00:00.000Z";
  assert.deepEqual(evaluateWatchdog({
    snapshot: {
      dbNow,
      active: true,
      ownerLabel: "github-actions",
      capabilities: { release: releaseSha },
      processing: 0
    },
    runs: []
  }), { action: "none", reason: "active-leader" });

  assert.deepEqual(evaluateWatchdog({
    snapshot: { dbNow, active: false, processing: 0 },
    runs: [{ id: 11, status: "queued", created_at: "2026-08-15T05:50:00.000Z" }]
  }), { action: "none", reason: "recent-candidate", runId: 11 });

  assert.deepEqual(evaluateWatchdog({
    snapshot: { dbNow, active: false, processing: 0 },
    runs: [{ id: 10, status: "queued", created_at: "2026-08-15T05:30:00.000Z" }]
  }), { action: "dispatch", reason: "no-leader-no-candidate" });

  assert.throws(() => evaluateWatchdog({
    snapshot: { dbNow, active: false, processing: 1 },
    runs: []
  }), /PROCESSING deliveries/);

  assert.throws(() => evaluateWatchdog({
    snapshot: {
      dbNow,
      active: true,
      ownerLabel: "unexpected",
      capabilities: { release: releaseSha },
      processing: 0
    },
    runs: []
  }), /unexpected owner or release/);
});

test("the workflow keeps the immutable RC and places the PostgreSQL gate before worker startup", async () => {
  const workflow = await readFile(
    new URL("../.github/workflows/notifications-worker-self-handoff.yml", import.meta.url),
    "utf8"
  );
  assert.match(workflow, new RegExp(releaseSha));
  const gateIndex = workflow.indexOf("- name: Wait for predecessor lease expiration");
  const workerIndex = workflow.indexOf("- name: Run self-handoff worker session");
  assert.ok(gateIndex > 0);
  assert.ok(workerIndex > gateIndex);
  assert.doesNotMatch(workflow, /^concurrency:/m);
  assert.match(workflow, /SELF_HANDOFF_REMAINING:/);
  assert.match(workflow, /CANARY_DATABASE_URL/);
  assert.match(workflow, /2099-01-01T00:00:00\.000Z/);
  assert.match(workflow, /CARD_EXPORT_WORKER_ENABLED: "false"/);
  assert.doesNotMatch(workflow, /secrets\.DATABASE_URL/);
});
