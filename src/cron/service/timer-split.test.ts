// Cron service timer tests — script pacing, task ledger, and delivery coverage.
// Split from timer.test.ts to stay under the oxlint max-lines budget.
import { afterEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { setupCronServiceSuite, writeCronStoreSnapshot } from "../../cron/service.test-harness.js";
import { createCronServiceState as createCronServiceStateBase } from "../../cron/service/state.js";
import { onTimer } from "../../cron/service/timer.test-support.js";
import { loadCronStore } from "../../cron/store.js";
import type { CronJob } from "../../cron/types.js";
import { getActiveGatewayRootWorkCount } from "../../process/gateway-work-admission.js";
import * as taskExecutor from "../../tasks/task-executor.js";
import { findTaskByRunId, listTaskRecordsUnsorted } from "../../tasks/task-registry.js";
import { resetTaskRegistryForTests } from "../../tasks/task-runtime.test-helpers.js";
import { formatTaskStatusDetail } from "../../tasks/task-status.js";
import { getSuspensionVisibleCronTaskRunCount } from "./active-run-cancellation.js";
import { stop } from "./ops-lifecycle.js";
import { executeJobCore } from "./timer-execution.js";

const { logger, makeStorePath } = setupCronServiceSuite({
  prefix: "cron-service-timer-split",
});

function createCronServiceState(
  params: Parameters<typeof createCronServiceStateBase>[0],
): ReturnType<typeof createCronServiceStateBase> {
  return createCronServiceStateBase({ defaultAgentId: "main", ...params });
}

function createDueMainJob(params: { now: number; wakeMode: CronJob["wakeMode"] }): CronJob {
  return {
    id: "main-heartbeat-job",
    name: "main heartbeat job",
    enabled: true,
    createdAtMs: params.now - 60_000,
    updatedAtMs: params.now - 60_000,
    schedule: { kind: "every", everyMs: 60_000, anchorMs: params.now - 60_000 },
    sessionTarget: "main",
    wakeMode: params.wakeMode,
    payload: { kind: "systemEvent", text: "heartbeat seam tick" },
    sessionKey: "agent:main:main",
    state: { nextRunAtMs: params.now - 1 },
  };
}

function createDueIsolatedAgentJob(params: { now: number }): CronJob {
  return {
    id: "isolated-agent-job",
    agentId: "finn",
    name: "isolated agent job",
    enabled: true,
    createdAtMs: params.now - 60_000,
    updatedAtMs: params.now - 60_000,
    schedule: { kind: "every", everyMs: 60_000, anchorMs: params.now - 60_000 },
    sessionTarget: "isolated",
    wakeMode: "now",
    payload: { kind: "agentTurn", message: "run isolated cron" },
    state: { nextRunAtMs: params.now - 1 },
  };
}

function createDueScriptJob(params: {
  now: number;
  sessionTarget?: "main" | "isolated";
  pacing?: CronJob["pacing"];
}): CronJob {
  return {
    id: "script-job",
    agentId: "finn",
    name: "script job",
    enabled: true,
    createdAtMs: params.now - 60_000,
    updatedAtMs: params.now - 60_000,
    schedule: { kind: "every", everyMs: 60_000, anchorMs: params.now - 60_000 },
    pacing: params.pacing,
    sessionTarget: params.sessionTarget ?? "isolated",
    wakeMode: "now",
    payload: {
      kind: "script",
      script: "return { notify: 'done' }",
      timeoutSeconds: 300,
      toolBudget: 50,
    },
    state: { nextRunAtMs: params.now - 1, triggerState: { revision: 1 } },
  };
}

function findCronTaskByBaseRunId(baseRunId: string) {
  return (
    findTaskByRunId(baseRunId) ??
    listTaskRecordsUnsorted().find((task) => task.runId?.startsWith(`${baseRunId}:`))
  );
}

afterEach(() => {
  resetTaskRegistryForTests();
});

describe("cron service timer script and task ledger coverage", () => {
  it.each(["main", "isolated"] as const)(
    "does not resolve an owner for a quiet %s script without side effects",
    async (sessionTarget) => {
      const { storePath } = await makeStorePath();
      const now = Date.parse("2026-08-24T12:00:00.000Z");
      const enqueueSystemEvent = vi.fn();
      const requestHeartbeat = vi.fn();
      const resolveDefaultAgentId = vi.fn(() => undefined);
      const job = {
        ...createDueScriptJob({ now, sessionTarget }),
        agentId: undefined,
      };
      const state = createCronServiceState({
        storePath,
        cronEnabled: true,
        cronConfig: { triggers: { enabled: true } },
        log: logger,
        nowMs: () => now,
        defaultAgentId: undefined,
        resolveDefaultAgentId,
        enqueueSystemEvent,
        requestHeartbeat,
        runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
        runScriptJob: vi.fn(async () => ({ status: "ok" as const })),
      });

      await expect(executeJobCore(state, job)).resolves.toMatchObject({ status: "ok" });
      expect(resolveDefaultAgentId).not.toHaveBeenCalled();
      expect(enqueueSystemEvent).not.toHaveBeenCalled();
      expect(requestHeartbeat).not.toHaveBeenCalled();
    },
  );

  it("delivers nothing and enqueues nothing when notify and wake are absent", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-07-18T12:00:00.000Z");
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();
    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      cronConfig: { triggers: { enabled: true } },
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent,
      requestHeartbeat,
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
      runScriptJob: vi.fn(async () => ({
        status: "ok" as const,
        stateChanged: true,
        state: { revision: 2 },
        delivered: false,
        deliveryAttempted: false,
      })),
    });

    await expect(
      executeJobCore(state, createDueScriptJob({ now, sessionTarget: "main" })),
    ).resolves.toMatchObject({ status: "ok", scriptStateChanged: true });
    expect(enqueueSystemEvent).not.toHaveBeenCalled();
    expect(requestHeartbeat).not.toHaveBeenCalled();
  });

  it("rejects nextCheck without pacing before applying state", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-07-18T12:00:00.000Z");
    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      cronConfig: { triggers: { enabled: true } },
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent: vi.fn(),
      requestHeartbeat: vi.fn(),
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
      runScriptJob: vi.fn(async () => ({
        status: "ok" as const,
        stateChanged: true,
        state: { revision: 2 },
        nextCheck: { delayMs: 5_000 },
      })),
    });

    await expect(executeJobCore(state, createDueScriptJob({ now }))).resolves.toEqual({
      status: "error",
      error: "cron script payload returned nextCheck, but this job has no pacing bounds",
      errorClassification: { kind: "permanent" },
      failureNotificationDetail: {
        kind: "script-failure",
        source: "payload",
        code: "invalid_input",
      },
    });
  });

  it.each([
    ["ok", { status: "ok" as const, stateChanged: true, state: { revision: 2 } }, 2, 0],
    [
      "error",
      {
        status: "error" as const,
        error: "script threw",
        stateChanged: true,
        state: { revision: 2 },
      },
      1,
      1,
    ],
  ] as const)(
    "persists script state on %s runs only",
    async (_label, outcome, revision, errors) => {
      const { storePath } = await makeStorePath();
      const now = Date.parse("2026-07-18T12:00:00.000Z");
      const job = createDueScriptJob({ now });
      await writeCronStoreSnapshot({ storePath, jobs: [job] });
      const state = createCronServiceState({
        storePath,
        cronEnabled: true,
        cronConfig: { triggers: { enabled: true } },
        log: logger,
        nowMs: () => now,
        enqueueSystemEvent: vi.fn(),
        requestHeartbeat: vi.fn(),
        runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
        runScriptJob: vi.fn(async () => outcome),
      });

      await onTimer(state);

      const stored = await loadCronStore(storePath);
      expect(stored.jobs[0]?.state.triggerState).toEqual({ revision });
      expect(stored.jobs[0]?.state.consecutiveErrors ?? 0).toBe(errors);
    },
  );

  it("clamps a script nextCheck through the shared pacing path", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-07-18T12:00:00.000Z");
    const job = createDueScriptJob({ now, pacing: { min: "15m", max: "4h" } });
    await writeCronStoreSnapshot({ storePath, jobs: [job] });
    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      cronConfig: { triggers: { enabled: true } },
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent: vi.fn(),
      requestHeartbeat: vi.fn(),
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
      runScriptJob: vi.fn(async () => ({
        status: "ok" as const,
        nextCheck: { delayMs: 5 * 60_000 },
      })),
    });

    await onTimer(state);

    const stored = await loadCronStore(storePath);
    expect(stored.jobs[0]?.state.nextRunAtMs).toBe(now + 15 * 60_000);
    expect(stored.jobs[0]?.state.pacedNextRunAtMs).toBe(now + 15 * 60_000);
  });

  it("records isolated cron task runs against the backing cron session", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T12:00:00.000Z");
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();
    const runIsolatedAgentJob = vi.fn(async () => ({
      status: "ok" as const,
      summary: "done",
      sessionId: "session-run-1",
      delivered: true,
      sessionKey: "agent:finn:cron:isolated-agent-job:run:run-1",
      delivery: { intended: { channel: "telegram", to: "42" } },
      model: "gpt-test",
      provider: "openai",
      usage: { input_tokens: 5, output_tokens: 3, total_tokens: 8 },
    }));

    await writeCronStoreSnapshot({
      storePath,
      jobs: [createDueIsolatedAgentJob({ now })],
    });

    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent,
      requestHeartbeat,
      runIsolatedAgentJob,
    });

    await onTimer(state);

    expect(runIsolatedAgentJob).toHaveBeenCalledWith(
      expect.objectContaining({
        job: expect.objectContaining({ id: "isolated-agent-job" }),
        message: "run isolated cron",
      }),
    );
    const task = findCronTaskByBaseRunId(`cron:isolated-agent-job:${now}`);
    if (!task) {
      throw new Error("expected isolated cron task ledger record");
    }
    expect(task.childSessionKey).toBe("agent:finn:cron:isolated-agent-job:run:run-1");
    expect(task.status).toBe("succeeded");
    expect(task.terminalSummary).toBe("done");
    expect(task.detail).toMatchObject({
      kind: "cron-run",
      status: "ok",
      sessionId: "session-run-1",
      durationMs: 0,
      nextRunAtMs: now + 60_000,
      delivery: { intended: { channel: "telegram", to: "42" } },
      model: "gpt-test",
      provider: "openai",
      usage: { input_tokens: 5, output_tokens: 3, total_tokens: 8 },
    });
  });

  it("records current-bound cron task runs against the backing cron session", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T12:00:00.000Z");
    const runIsolatedAgentJob = vi.fn(async () => ({
      status: "ok" as const,
      summary: "done",
      sessionKey: "agent:finn:cron:isolated-agent-job:run:run-1",
      delivered: true,
    }));

    await writeCronStoreSnapshot({
      storePath,
      jobs: [
        {
          ...createDueIsolatedAgentJob({ now }),
          sessionTarget: "current",
          sessionKey: "agent:finn:telegram:direct:42",
        },
      ],
    });

    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent: vi.fn(),
      requestHeartbeat: vi.fn(),
      runIsolatedAgentJob,
    });

    await onTimer(state);

    const task = findCronTaskByBaseRunId(`cron:isolated-agent-job:${now}`);
    if (!task) {
      throw new Error("expected current-bound cron task ledger record");
    }
    expect(task.childSessionKey).toBe("agent:finn:cron:isolated-agent-job:run:run-1");
    expect(task.status).toBe("succeeded");
  });

  it("seeds active scheduled cron task progress for status surfaces", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T12:00:00.000Z");
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();
    const runResult = createDeferred<{ status: "ok"; summary: string }>();
    const runIsolatedAgentJob = vi.fn(() => runResult.promise);

    await writeCronStoreSnapshot({
      storePath,
      jobs: [createDueIsolatedAgentJob({ now })],
    });

    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent,
      requestHeartbeat,
      runIsolatedAgentJob,
    });

    const timerRun = onTimer(state);
    try {
      await vi.waitFor(() => {
        expect(runIsolatedAgentJob).toHaveBeenCalledTimes(1);
      });

      const task = findCronTaskByBaseRunId(`cron:isolated-agent-job:${now}`);
      if (!task) {
        throw new Error("expected active cron task ledger record");
      }
      expect(task.status).toBe("running");
      expect(task.progressSummary).toBe("Running automation.");
      expect(formatTaskStatusDetail(task)).toBe("Running automation.");

      runResult.resolve({ status: "ok", summary: "done" });
      await timerRun;
    } finally {
      // Stop new ticks and settle this core before the shared hooks reset its state.
      stop(state);
      runResult.resolve({ status: "ok", summary: "done" });
      try {
        await timerRun;
      } finally {
        await vi.waitFor(() => {
          expect(getSuspensionVisibleCronTaskRunCount()).toBe(0);
          expect(getActiveGatewayRootWorkCount()).toBe(0);
        });
      }
    }
  });

  it("keeps scheduler progress when task ledger creation fails", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T12:00:00.000Z");
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();
    const ledgerError = new Error("disk full");

    await writeCronStoreSnapshot({
      storePath,
      jobs: [createDueMainJob({ now, wakeMode: "next-heartbeat" })],
    });

    const createTaskRecordSpy = vi
      .spyOn(taskExecutor, "createRunningTaskRunCore")
      .mockImplementation(() => {
        throw ledgerError;
      });

    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent,
      requestHeartbeat,
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
    });

    await onTimer(state);

    expect(logger.warn).toHaveBeenCalledWith(
      { jobId: "main-heartbeat-job", error: ledgerError },
      "cron: failed to create task ledger record",
    );
    expect(enqueueSystemEvent).toHaveBeenCalledWith("heartbeat seam tick", {
      agentId: "main",
      contextKey: "cron:main-heartbeat-job",
    });

    createTaskRecordSpy.mockRestore();
  });
});
