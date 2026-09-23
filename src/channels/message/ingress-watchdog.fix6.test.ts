import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { closeOpenClawStateDatabaseForTest } from "../../state/openclaw-state-db.js";
import { createTestIngressQueue, withTempState } from "./ingress-drain.test-helpers.js";
import type { IngressPriorityMetadata } from "./ingress-priority.js";
import { createDirectUserWatchdog, type DirectUserStuckMessage } from "./ingress-watchdog.js";

afterEach(() => {
  closeOpenClawStateDatabaseForTest();
});

describe("FIX 6: direct-user message watchdog", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  it("logs WARNING when a direct-user message is undispatched > 60s", async () => {
    await withTempState(async (stateDir) => {
      let clock = 100_000;
      const queue = createTestIngressQueue(stateDir, { now: () => clock });
      // Enqueue a direct-user message.
      await queue.enqueue(
        "dm-stuck",
        { text: "stuck DM" },
        {
          laneKey: "dm-lane",
          receivedAt: clock,
          metadata: { priority: "direct", senderId: "eric" } as IngressPriorityMetadata,
        },
      );
      closeOpenClawStateDatabaseForTest();

      const logs: Array<{ level: string; message: string }> = [];
      const recoveryCalls: DirectUserStuckMessage[] = [];

      const watchdog = createDirectUserWatchdog<
        { text: string },
        IngressPriorityMetadata | undefined
      >({
        queue: createTestIngressQueue(stateDir, { now: () => clock }),
        now: () => clock,
        warningThresholdMs: 60_000,
        errorThresholdMs: 300_000,
        recoveryRateLimitMs: 300_000,
        onLog: (level, message) => {
          logs.push({ level, message });
        },
        onRecovery: (stuck) => {
          recoveryCalls.push(stuck);
        },
      });

      // Advance time to 61 seconds — should trigger WARNING.
      clock += 61_000;
      await watchdog.checkOnce();

      expect(logs).toContainEqual(
        expect.objectContaining({
          level: "warn",
          message: expect.stringContaining("dm-stuck"),
        }),
      );
      expect(recoveryCalls).toHaveLength(0);

      watchdog.stop();
    });
  });

  it("logs ERROR and triggers recovery when a direct-user message is undispatched > 300s", async () => {
    await withTempState(async (stateDir) => {
      let clock = 100_000;
      const queue = createTestIngressQueue(stateDir, { now: () => clock });
      await queue.enqueue(
        "dm-critical",
        { text: "very stuck DM" },
        {
          laneKey: "dm-lane",
          receivedAt: clock,
          metadata: { priority: "direct", senderId: "eric" } as IngressPriorityMetadata,
        },
      );
      closeOpenClawStateDatabaseForTest();

      const logs: Array<{ level: string; message: string }> = [];
      const recoveryCalls: DirectUserStuckMessage[] = [];

      const watchdog = createDirectUserWatchdog<
        { text: string },
        IngressPriorityMetadata | undefined
      >({
        queue: createTestIngressQueue(stateDir, { now: () => clock }),
        now: () => clock,
        warningThresholdMs: 60_000,
        errorThresholdMs: 300_000,
        recoveryRateLimitMs: 300_000,
        onLog: (level, message) => {
          logs.push({ level, message });
        },
        onRecovery: (stuck) => {
          recoveryCalls.push(stuck);
        },
      });

      // Advance time to 301 seconds — should trigger ERROR + recovery.
      clock += 301_000;
      await watchdog.checkOnce();

      expect(logs).toContainEqual(
        expect.objectContaining({
          level: "error",
          message: expect.stringContaining("dm-critical"),
        }),
      );
      expect(recoveryCalls).toHaveLength(1);
      expect(recoveryCalls[0]).toMatchObject({
        eventId: "dm-critical",
        senderId: "eric",
      });

      watchdog.stop();
    });
  });

  it("rate-limits recovery to 1 per 5 minutes", async () => {
    await withTempState(async (stateDir) => {
      let clock = 100_000;
      const queue = createTestIngressQueue(stateDir, { now: () => clock });
      // Enqueue two direct-user messages on different lanes.
      await queue.enqueue(
        "dm-1",
        { text: "first stuck" },
        {
          laneKey: "dm-lane-1",
          receivedAt: clock,
          metadata: { priority: "direct" } as IngressPriorityMetadata,
        },
      );
      await queue.enqueue(
        "dm-2",
        { text: "second stuck" },
        {
          laneKey: "dm-lane-2",
          receivedAt: clock,
          metadata: { priority: "direct" } as IngressPriorityMetadata,
        },
      );
      closeOpenClawStateDatabaseForTest();

      const recoveryCalls: DirectUserStuckMessage[] = [];

      const watchdog = createDirectUserWatchdog<
        { text: string },
        IngressPriorityMetadata | undefined
      >({
        queue: createTestIngressQueue(stateDir, { now: () => clock }),
        now: () => clock,
        warningThresholdMs: 60_000,
        errorThresholdMs: 300_000,
        recoveryRateLimitMs: 300_000, // 5 minutes
        onRecovery: (stuck) => {
          recoveryCalls.push(stuck);
        },
      });

      // Advance to 301s — first recovery triggers (for oldest message).
      clock += 301_000;
      await watchdog.checkOnce();
      expect(recoveryCalls).toHaveLength(1);

      // Advance 60 more seconds — recovery should be rate-limited.
      clock += 60_000;
      await watchdog.checkOnce();
      expect(recoveryCalls).toHaveLength(1); // Still only 1 recovery

      // Advance past rate limit (5 minutes total) — recovery should fire again.
      clock += 240_001; // Total: 301 + 60 + 240.001 = 601.001s, recovery gap = 300.001s
      await watchdog.checkOnce();
      expect(recoveryCalls).toHaveLength(2);

      watchdog.stop();
    });
  });

  it("ignores normal-priority messages", async () => {
    await withTempState(async (stateDir) => {
      let clock = 100_000;
      const queue = createTestIngressQueue(stateDir, { now: () => clock });
      await queue.enqueue(
        "cron-1",
        { text: "cron job" },
        {
          laneKey: "cron-lane",
          receivedAt: clock,
          metadata: { priority: "normal" } as IngressPriorityMetadata,
        },
      );
      closeOpenClawStateDatabaseForTest();

      const logs: Array<{ level: string; message: string }> = [];
      const watchdog = createDirectUserWatchdog<
        { text: string },
        IngressPriorityMetadata | undefined
      >({
        queue: createTestIngressQueue(stateDir, { now: () => clock }),
        now: () => clock,
        warningThresholdMs: 60_000,
        errorThresholdMs: 300_000,
        onLog: (level, message) => {
          logs.push({ level, message });
        },
      });

      clock += 301_000;
      await watchdog.checkOnce();

      // Normal-priority messages should not trigger any watchdog logs.
      expect(logs).toEqual([]);

      watchdog.stop();
    });
  });

  it("respects prioritySenders config", async () => {
    await withTempState(async (stateDir) => {
      let clock = 100_000;
      const queue = createTestIngressQueue(stateDir, { now: () => clock });
      // No explicit priority, but senderId matches prioritySenders.
      await queue.enqueue(
        "dm-from-eric",
        { text: "from eric" },
        {
          laneKey: "dm-lane",
          receivedAt: clock,
          metadata: { senderId: "eric" } as IngressPriorityMetadata,
        },
      );
      closeOpenClawStateDatabaseForTest();

      const logs: Array<{ level: string; message: string }> = [];
      const watchdog = createDirectUserWatchdog<
        { text: string },
        IngressPriorityMetadata | undefined
      >({
        queue: createTestIngressQueue(stateDir, { now: () => clock }),
        now: () => clock,
        prioritySenders: new Set(["eric"]),
        warningThresholdMs: 60_000,
        errorThresholdMs: 300_000,
        onLog: (level, message) => {
          logs.push({ level, message });
        },
      });

      clock += 61_000;
      await watchdog.checkOnce();

      // Should trigger WARNING because senderId is in prioritySenders.
      expect(logs.some((l) => l.level === "warn" && l.message.includes("dm-from-eric"))).toBe(true);

      watchdog.stop();
    });
  });

  it("runs periodic checks and can be stopped", async () => {
    await withTempState(async (stateDir) => {
      const clock = 100_000;
      const queue = createTestIngressQueue(stateDir, { now: () => clock });
      await queue.enqueue(
        "dm-1",
        { text: "stuck" },
        {
          laneKey: "dm-lane",
          receivedAt: clock,
          metadata: { priority: "direct" } as IngressPriorityMetadata,
        },
      );
      closeOpenClawStateDatabaseForTest();

      let checkCount = 0;
      const watchdog = createDirectUserWatchdog<
        { text: string },
        IngressPriorityMetadata | undefined
      >({
        queue: createTestIngressQueue(stateDir, { now: () => clock }),
        now: () => clock + 61_000, // Advance time so message is > 60s old.
        checkIntervalMs: 30_000,
        warningThresholdMs: 60_000,
        errorThresholdMs: 300_000,
        onLog: (level, _message) => {
          if (level === "warn") {
            checkCount++;
          }
        },
      });

      // Start and run one check manually to verify it works.
      watchdog.start();
      await watchdog.checkOnce();
      expect(checkCount).toBeGreaterThan(0);

      // Stop and verify it stops.
      watchdog.stop();
      expect(watchdog.isRunning()).toBe(false);
    });
  });

  it("does not trigger recovery for messages that have been dispatched", async () => {
    await withTempState(async (stateDir) => {
      let clock = 100_000;
      const queue = createTestIngressQueue(stateDir, { now: () => clock });
      await queue.enqueue(
        "dm-dispatched",
        { text: "dispatched" },
        {
          laneKey: "dm-lane",
          receivedAt: clock,
          metadata: { priority: "direct" } as IngressPriorityMetadata,
        },
      );
      // Claim and complete the message.
      const claim = await queue.claimNext({});
      expect(claim).not.toBeNull();
      await queue.complete(claim!);
      closeOpenClawStateDatabaseForTest();

      const logs: Array<{ level: string; message: string }> = [];
      const watchdog = createDirectUserWatchdog<
        { text: string },
        IngressPriorityMetadata | undefined
      >({
        queue: createTestIngressQueue(stateDir, { now: () => clock }),
        now: () => clock,
        warningThresholdMs: 60_000,
        errorThresholdMs: 300_000,
        onLog: (level, message) => {
          logs.push({ level, message });
        },
      });

      clock += 301_000;
      await watchdog.checkOnce();

      // No pending messages → no logs.
      expect(logs).toEqual([]);

      watchdog.stop();
    });
  });
});
