// Cron service timer tests cover timer scheduling, cancellation, and wakeups.
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { upsertSessionEntryCore } from "../../config/sessions/session-accessor.js";
import { setupCronServiceSuite, writeCronStoreSnapshot } from "../../cron/service.test-harness.js";
import { createCronServiceState as createCronServiceStateBase } from "../../cron/service/state.js";
import { onTimer } from "../../cron/service/timer.test-support.js";
import { loadCronStore } from "../../cron/store.js";
import type { CronJob } from "../../cron/types.js";
import { openOpenClawStateDatabase } from "../../state/openclaw-state-db.js";
import { findTaskByRunId, listTaskRecordsUnsorted } from "../../tasks/task-registry.js";
import { resetTaskRegistryForTests } from "../../tasks/task-runtime.test-helpers.js";
import { normalizeSessionDeliveryState } from "../../utils/delivery-context.shared.js";
import { start, stop } from "./ops-lifecycle.js";
import { add as addJob, update as updateJob } from "./ops-mutations.js";
import { status as cronStatus } from "./ops-read.js";
import { run } from "./ops-run.js";
import { executeJobCore } from "./timer-execution.js";

const { logger, makeStorePath } = setupCronServiceSuite({
  prefix: "cron-service-timer-seam",
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

function createDueCommandJob(params: { now: number }): CronJob {
  return {
    id: "command-job",
    agentId: "finn",
    name: "command job",
    enabled: true,
    createdAtMs: params.now - 60_000,
    updatedAtMs: params.now - 60_000,
    schedule: { kind: "every", everyMs: 60_000, anchorMs: params.now - 60_000 },
    sessionTarget: "isolated",
    wakeMode: "now",
    payload: { kind: "command", argv: ["sh", "-lc", "echo ok"] },
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

describe("cron service timer seam coverage", () => {
  it.each(["timer", "startup"] as const)("%s ignores stale event schedule slots", async (entry) => {
    const { storePath } = await makeStorePath();
    const now = Date.now();
    const schedules: CronJob["schedule"][] = [
      { kind: "on-exit", command: "true" },
      { kind: "stream", command: ["true"], mode: "line" },
    ];
    const jobs = schedules.map((schedule) => {
      const job = createDueMainJob({ now, wakeMode: "next-heartbeat" });
      job.id = `stale-${schedule.kind}`;
      job.schedule = schedule;
      job.state = {
        nextRunAtMs: now - 1,
        startupCatchupAtMs: now - 1,
        pacedNextRunAtMs: now - 1,
        forcePreservedNextRunAtMs: now - 1,
      };
      return job;
    });
    await writeCronStoreSnapshot({ storePath, jobs });
    const enqueueSystemEvent = vi.fn();
    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent,
      requestHeartbeat: vi.fn(),
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
    });

    try {
      await (entry === "startup" ? start(state) : onTimer(state));
      expect(enqueueSystemEvent).not.toHaveBeenCalled();
      const stored = await loadCronStore(storePath);
      expect(stored.jobs).toHaveLength(2);
      for (const job of stored.jobs) {
        expect(job.enabled).toBe(true);
        expect(job.state.nextRunAtMs).toBeUndefined();
        expect(job.state.startupCatchupAtMs).toBeUndefined();
        expect(job.state.pacedNextRunAtMs).toBeUndefined();
        expect(job.state.forcePreservedNextRunAtMs).toBeUndefined();
        await expect(run(state, job.id, "due")).resolves.toEqual({
          ok: true,
          ran: false,
          reason: "not-due",
        });
        await expect(run(state, job.id, "force")).resolves.toEqual({ ok: true, ran: true });
      }
      expect(enqueueSystemEvent).toHaveBeenCalledTimes(2);
    } finally {
      stop(state);
    }
  });

  it("routes main cron jobs to the owning agent's main session", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T12:00:00.000Z");
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();
    const requestHeartbeatAndWait = vi.fn(async () => ({ status: "ran" as const, durationMs: 1 }));
    const job = {
      ...createDueMainJob({ now, wakeMode: "now" }),
      sessionKey: "agent:main-pr-router:main",
      state: { runningAtMs: now },
    };
    const sessionStorePath = path.join(path.dirname(path.dirname(storePath)), "sessions.json");
    await upsertSessionEntryCore(
      { storePath: sessionStorePath, sessionKey: "agent:main-pr-router:main" },
      {
        sessionId: "main-pr-router-session",
        updatedAt: now,
        delivery: normalizeSessionDeliveryState({
          context: { channel: "discord", to: "channel-1", accountId: "default" },
        }),
      },
    );

    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      defaultAgentId: "main-pr-router",
      resolveSessionStorePath: () => sessionStorePath,
      enqueueSystemEvent,
      requestHeartbeat,
      requestHeartbeatAndWait,
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
    });

    const result = await executeJobCore(state, job);

    expect(result).toMatchObject({ status: "ok" });
    expect(result.sessionKey).toBeUndefined();
    expect(enqueueSystemEvent).toHaveBeenCalledWith("heartbeat seam tick", {
      agentId: "main-pr-router",
      contextKey: "cron:main-heartbeat-job",
      deliveryContext: { channel: "discord", to: "channel-1", accountId: "default" },
    });
    expect(requestHeartbeatAndWait).toHaveBeenCalledWith(
      {
        source: "cron",
        intent: "immediate",
        reason: "cron:main-heartbeat-job",
        agentId: "main-pr-router",
        heartbeat: { target: "last" },
      },
      expect.objectContaining({ stopWaitingOnRetry: expect.any(Function) }),
    );
  });

  it("persists the next schedule and hands off next-heartbeat main jobs", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T12:00:00.000Z");
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();
    const timeoutSpy = vi.spyOn(globalThis, "setTimeout");

    const jobWithoutExplicitOwner = createDueMainJob({ now, wakeMode: "next-heartbeat" });
    delete jobWithoutExplicitOwner.sessionKey;
    await writeCronStoreSnapshot({ storePath, jobs: [jobWithoutExplicitOwner] });

    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      defaultAgentId: "stale-default",
      resolveDefaultAgentId: () => "ops",
      enqueueSystemEvent,
      requestHeartbeat,
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
    });

    await onTimer(state);

    expect(enqueueSystemEvent).toHaveBeenCalledWith("heartbeat seam tick", {
      agentId: "ops",
      contextKey: "cron:main-heartbeat-job",
    });
    expect(requestHeartbeat).toHaveBeenCalledWith({
      source: "cron",
      intent: "event",
      reason: "cron:main-heartbeat-job",
      agentId: "ops",
      heartbeat: { target: "last" },
    });

    const persisted = await loadCronStore(storePath);
    const job = persisted.jobs[0];
    if (!job) {
      throw new Error("expected persisted heartbeat cron job");
    }
    expect(job.state.lastStatus).toBe("ok");
    expect(job.state.runningAtMs).toBeUndefined();
    expect(job.state.nextRunAtMs).toBe(now + 60_000);
    const task = findCronTaskByBaseRunId(`cron:main-heartbeat-job:${now}`);
    if (!task) {
      throw new Error("expected cron task ledger record");
    }
    expect(task.runtime).toBe("cron");
    expect(task.sourceId).toBe("main-heartbeat-job");
    expect(task.agentId).toBe("ops");
    expect(task.ownerKey).toBe("");
    expect(task.scopeKind).toBe("system");
    expect(task.childSessionKey).toBeUndefined();
    expect(task.runId).toMatch(new RegExp(`^cron:main-heartbeat-job:${now}:`));
    expect(task.label).toBe("main heartbeat job");
    expect(task.task).toBe("main heartbeat job");
    expect(task.status).toBe("succeeded");
    expect(task.deliveryStatus).toBe("not_applicable");
    expect(task.notifyPolicy).toBe("silent");
    expect(task.startedAt).toBe(now);
    expect(task.lastEventAt).toBe(now);
    expect(task.endedAt).toBe(now);
    expect(task.cleanupAfter).toBe(now + 7 * 24 * 60 * 60_000);

    const delays = timeoutSpy.mock.calls
      .map(([, delay]) => delay)
      .filter((delay): delay is number => typeof delay === "number");
    const positiveDelays = delays.filter((delay) => delay > 0);
    expect(positiveDelays.length).toBeGreaterThan(0);

    timeoutSpy.mockRestore();
  });

  it("uses the persisted execution timestamp for the canonical timer task", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T12:00:00.000Z");
    let clock = now;
    let persistedReservation: number | undefined;
    let liveReservation: number | undefined;
    let liveError: string | undefined;
    let emittedStartedAt: number | undefined;
    let reservedAt: number | undefined;
    const job = createDueIsolatedAgentJob({ now });
    job.state.lastError = "previous failure";
    await writeCronStoreSnapshot({
      storePath,
      jobs: [job],
    });
    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => clock++,
      enqueueSystemEvent: vi.fn(),
      requestHeartbeat: vi.fn(),
      runIsolatedAgentJob: vi.fn(async () => {
        persistedReservation = (await loadCronStore(storePath)).jobs[0]?.state.runningAtMs;
        liveReservation = state.store?.jobs[0]?.state.runningAtMs;
        liveError = state.store?.jobs[0]?.state.lastError;
        return { status: "ok" as const, delivered: true };
      }),
      onEvent: (event) => {
        if (event.action === "started") {
          emittedStartedAt = event.runAtMs;
        }
      },
    });
    const database = openOpenClawStateDatabase().db;
    database.function("observe_timer_reservation", (stateJson) => {
      if (typeof stateJson === "string") {
        const marker = (JSON.parse(stateJson) as CronJob["state"]).queuedAtMs;
        if (reservedAt === undefined && typeof marker === "number") {
          reservedAt = marker;
        }
      }
      return 0;
    });
    database.exec(`
      CREATE TEMP TRIGGER observe_timer_reservation
      AFTER UPDATE ON cron_jobs
      WHEN NEW.job_id = '${job.id}'
      BEGIN
        SELECT observe_timer_reservation(NEW.state_json);
      END;
    `);

    try {
      await onTimer(state);
    } finally {
      database.exec("DROP TRIGGER IF EXISTS observe_timer_reservation");
    }

    expect(reservedAt).toEqual(expect.any(Number));
    expect(persistedReservation).toEqual(expect.any(Number));
    expect(reservedAt).not.toBe(persistedReservation);
    expect(liveReservation).toBe(persistedReservation);
    expect(liveError).toBeUndefined();
    expect(emittedStartedAt).toBe(persistedReservation);
    expect(
      findCronTaskByBaseRunId(`cron:isolated-agent-job:${persistedReservation}`),
    ).toMatchObject({
      startedAt: emittedStartedAt,
      status: "succeeded",
    });
  });

  it.each(["command", "script", "systemEvent", "heartbeat"] as const)(
    "does not run a %s payload when trigger evaluation resolves after cancellation",
    async (kind) => {
      const { storePath } = await makeStorePath();
      const now = Date.parse("2026-07-27T12:00:00.000Z");
      const evaluation = createDeferred<{
        kind: "evaluated";
        fire: true;
        state: { revision: number };
      }>();
      const evaluateCronTrigger = vi.fn(() => evaluation.promise);
      const enqueueSystemEvent = vi.fn();
      const requestHeartbeat = vi.fn();
      const runCommandJob = vi.fn(() => Promise.resolve({ status: "ok" as const }));
      const runScriptJob = vi.fn(() => Promise.resolve({ status: "ok" as const }));
      const runIsolatedAgentJob = vi.fn(() => Promise.resolve({ status: "ok" as const }));
      const state = createCronServiceState({
        storePath,
        cronEnabled: true,
        cronConfig: { triggers: { enabled: true } },
        log: logger,
        nowMs: () => now,
        enqueueSystemEvent,
        requestHeartbeat,
        evaluateCronTrigger,
        runCommandJob,
        runScriptJob,
        runIsolatedAgentJob,
      });
      const baseJob =
        kind === "command"
          ? createDueCommandJob({ now })
          : kind === "script"
            ? createDueScriptJob({ now })
            : kind === "heartbeat"
              ? {
                  ...createDueMainJob({ now, wakeMode: "next-heartbeat" }),
                  payload: { kind },
                }
              : createDueMainJob({ now, wakeMode: "next-heartbeat" });
      const job: CronJob = {
        ...baseJob,
        trigger: { script: "json({ fire: true })" },
      };
      const controller = new AbortController();

      const result = executeJobCore(state, job, controller.signal);
      try {
        expect(evaluateCronTrigger).toHaveBeenCalledOnce();
        controller.abort(new Error("operator cancelled the scheduled run"));
        evaluation.resolve({ kind: "evaluated", fire: true, state: { revision: 2 } });

        await expect(result).resolves.toMatchObject({ status: "error" });
        expect(enqueueSystemEvent).not.toHaveBeenCalled();
        expect(requestHeartbeat).not.toHaveBeenCalled();
        expect(runCommandJob).not.toHaveBeenCalled();
        expect(runScriptJob).not.toHaveBeenCalled();
        expect(runIsolatedAgentJob).not.toHaveBeenCalled();
      } finally {
        // Abort before releasing evaluation so failed assertions cannot start payload work.
        controller.abort(new Error("operator cancelled the scheduled run"));
        evaluation.resolve({ kind: "evaluated", fire: true, state: { revision: 2 } });
        await result;
      }
    },
  );

  it("runs command cron jobs without isolated agent setup", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T12:00:00.000Z");
    const runIsolatedAgentJob = vi.fn(async () => ({ status: "ok" as const }));
    const runCommandJob = vi.fn(async () => ({
      status: "ok" as const,
      summary: "command ok",
    }));
    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent: vi.fn(),
      requestHeartbeat: vi.fn(),
      runIsolatedAgentJob,
      runCommandJob,
    });
    const job = createDueCommandJob({ now });

    const result = await executeJobCore(state, job);

    expect(result).toMatchObject({ status: "ok", summary: "command ok" });
    expect(runCommandJob).toHaveBeenCalledWith({
      job,
      abortSignal: undefined,
    });
    expect(runIsolatedAgentJob).not.toHaveBeenCalled();
  });

  it.each([
    { kind: "on-exit", command: "true" },
    { kind: "stream", command: ["true"] },
  ] satisfies CronJob["schedule"][])(
    "keeps $kind jobs event-driven after a next-run state update",
    async (schedule) => {
      const { storePath } = await makeStorePath();
      const now = Date.parse("2026-03-23T12:00:00.000Z");
      const runPayload = vi.fn(async () => ({ status: "ok" as const }));
      const state = createCronServiceState({
        storePath,
        cronEnabled: true,
        log: logger,
        nowMs: () => now,
        enqueueSystemEvent: vi.fn(),
        requestHeartbeat: vi.fn(),
        runIsolatedAgentJob: runPayload,
        runCommandJob: runPayload,
      });

      try {
        const job = await addJob(state, {
          agentId: "finn",
          name: "event command",
          enabled: true,
          schedule,
          sessionTarget: "isolated",
          wakeMode: "now",
          payload:
            schedule.kind === "stream"
              ? { kind: "agentTurn", message: "Handle stream events" }
              : { kind: "command", argv: ["true"] },
        });
        await updateJob(state, job.id, { state: { nextRunAtMs: now - 1 } });
        await onTimer(state);
        expect(runPayload).not.toHaveBeenCalled();
        await expect(cronStatus(state)).resolves.toMatchObject({
          enabled: true,
          jobs: 1,
          nextWakeAtMs: null,
        });
        await expect(run(state, job.id, "due")).resolves.toEqual({
          ok: true,
          ran: false,
          reason: "not-due",
        });
        await expect(run(state, job.id, "force")).resolves.toEqual({ ok: true, ran: true });
        expect(runPayload).toHaveBeenCalledOnce();
      } finally {
        stop(state);
      }
    },
  );

  it("records an execution error when script payloads are disabled", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-07-18T12:00:00.000Z");
    const runScriptJob = vi.fn(async () => ({ status: "ok" as const }));
    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      cronConfig: { triggers: { enabled: false } },
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent: vi.fn(),
      requestHeartbeat: vi.fn(),
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
      runScriptJob,
    });

    await expect(executeJobCore(state, createDueScriptJob({ now }))).resolves.toMatchObject({
      status: "error",
      error: expect.stringContaining("the operator set cron.triggers.enabled: false"),
    });
    expect(runScriptJob).not.toHaveBeenCalled();
  });

  it.each([
    ["now", "immediate"],
    ["next-heartbeat", "event"],
  ] as const)("turns a main script notify and %s wake into one event", async (wake, intent) => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-07-18T12:00:00.000Z");
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();
    const job = createDueScriptJob({ now, sessionTarget: "main" });
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
        notify: "queue changed",
        wake,
      })),
    });

    await expect(executeJobCore(state, job)).resolves.toMatchObject({
      status: "ok",
      summary: "queue changed",
    });
    expect(enqueueSystemEvent).toHaveBeenCalledExactlyOnceWith("queue changed", {
      agentId: "finn",
      contextKey: "cron:script-job:script",
    });
    expect(requestHeartbeat).toHaveBeenCalledExactlyOnceWith({
      source: wake === "now" ? "notifications-event" : "cron",
      intent,
      reason: wake === "now" ? "wake" : "cron:script-job:script",
      agentId: "finn",
    });
  });

  it.each([
    {
      name: "main notification and immediate wake use the session owner and thread",
      sessionTarget: "main",
      sessionKey: "agent:ops:telegram:group:42:topic:77",
      defaultAgentId: "main",
      notify: "queue changed",
      wake: "now",
      expectedAgentId: "ops",
      expectedIntent: "immediate",
      expectDeliveryContext: true,
    },
    {
      name: "main notification and deferred wake use the current configured owner",
      sessionTarget: "main",
      defaultAgentId: "stale-main",
      currentDefaultAgentId: "ops",
      notify: "queue changed",
      wake: "next-heartbeat",
      expectedAgentId: "ops",
      expectedIntent: "event",
    },
    {
      name: "isolated script wake uses the session owner without main delivery context",
      sessionTarget: "isolated",
      sessionKey: "agent:ops:telegram:group:42:topic:77",
      defaultAgentId: "main",
      notify: "queue changed",
      wake: "now",
      expectedAgentId: "ops",
      expectedIntent: "immediate",
    },
    {
      name: "explicit script owner wins over the current configured default",
      sessionTarget: "main",
      agentId: "ops",
      sessionKey: "agent:ops:telegram:group:42:topic:77",
      defaultAgentId: "main",
      currentDefaultAgentId: "other",
      notify: "queue changed",
      wake: "next-heartbeat",
      expectedAgentId: "ops",
      expectedIntent: "event",
      expectDeliveryContext: true,
    },
    {
      name: "main wake-only completion keeps its session owner and thread",
      sessionTarget: "main",
      sessionKey: "agent:ops:telegram:group:42:topic:77",
      defaultAgentId: "main",
      wake: "now",
      expectedAgentId: "ops",
      expectedIntent: "immediate",
      expectDeliveryContext: true,
    },
    {
      name: "main notification without a wake keeps its session owner and thread",
      sessionTarget: "main",
      sessionKey: "agent:ops:telegram:group:42:topic:77",
      defaultAgentId: "main",
      notify: "queue changed",
      expectedAgentId: "ops",
      expectDeliveryContext: true,
    },
  ] as const)("routes script side effects: $name", async (testCase) => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-08-24T12:00:00.000Z");
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();
    const sessionKey = "sessionKey" in testCase ? testCase.sessionKey : undefined;
    const explicitAgentId = "agentId" in testCase ? testCase.agentId : undefined;
    const currentDefaultAgentId =
      "currentDefaultAgentId" in testCase ? testCase.currentDefaultAgentId : undefined;
    const notify = "notify" in testCase ? testCase.notify : undefined;
    const wake = "wake" in testCase ? testCase.wake : undefined;
    const job = {
      ...createDueScriptJob({ now, sessionTarget: testCase.sessionTarget }),
      agentId: explicitAgentId,
      ...(sessionKey ? { sessionKey } : {}),
    };
    const sessionStorePath = path.join(path.dirname(path.dirname(storePath)), "sessions.json");
    const deliveryContext = {
      channel: "telegram",
      to: "telegram:42",
      accountId: "ops-bot",
      threadId: 77,
    };
    if (sessionKey) {
      await upsertSessionEntryCore(
        { storePath: sessionStorePath, sessionKey },
        {
          sessionId: "ops-telegram-session",
          updatedAt: now,
          delivery: normalizeSessionDeliveryState({ context: deliveryContext }),
        },
      );
    }
    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      cronConfig: { triggers: { enabled: true } },
      log: logger,
      nowMs: () => now,
      defaultAgentId: testCase.defaultAgentId,
      ...(currentDefaultAgentId ? { resolveDefaultAgentId: () => currentDefaultAgentId } : {}),
      resolveSessionStorePath: () => sessionStorePath,
      enqueueSystemEvent,
      requestHeartbeat,
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
      runScriptJob: vi.fn(async () => ({
        status: "ok" as const,
        ...(notify ? { notify } : {}),
        ...(wake ? { wake } : {}),
      })),
    });

    await expect(executeJobCore(state, job)).resolves.toMatchObject({ status: "ok" });

    expect(enqueueSystemEvent).toHaveBeenCalledOnce();
    const [eventText, eventOptions] = enqueueSystemEvent.mock.calls[0] as [
      string,
      {
        agentId?: string;
        contextKey?: string;
        deliveryContext?: typeof deliveryContext;
      },
    ];
    expect(eventText).toBe(notify ?? "script job script job completed");
    expect(eventOptions.agentId).toBe(testCase.expectedAgentId);
    if ("expectDeliveryContext" in testCase && testCase.expectDeliveryContext) {
      expect(eventOptions.deliveryContext).toEqual(deliveryContext);
    } else {
      expect(eventOptions).not.toHaveProperty("deliveryContext");
    }
    if ("expectedIntent" in testCase) {
      expect(requestHeartbeat).toHaveBeenCalledExactlyOnceWith({
        source: wake === "now" ? "notifications-event" : "cron",
        intent: testCase.expectedIntent,
        reason: wake === "now" ? "wake" : "cron:script-job:script",
        agentId: testCase.expectedAgentId,
      });
    } else {
      expect(requestHeartbeat).not.toHaveBeenCalled();
    }
  });
});
