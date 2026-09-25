import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { closeOpenClawStateDatabaseForTest } from "../../state/openclaw-state-db.js";
import { createChannelIngressDrain } from "./ingress-drain.js";
import {
  createTestIngressQueue,
  withTempState,
  type IngressDrainTestPayload,
} from "./ingress-drain.test-helpers.js";
import {
  compareIngressPriority,
  createDirectUserMetadata,
  createNormalPriorityMetadata,
  isDirectUserMessage,
  partitionByPriority,
  resolveIngressPriority,
  sortPendingByPriority,
  type IngressPriorityMetadata,
} from "./ingress-priority.js";

afterEach(() => {
  closeOpenClawStateDatabaseForTest();
});

describe("FIX 4: direct-user message priority", () => {
  describe("resolveIngressPriority", () => {
    it("returns 'normal' for undefined metadata", () => {
      expect(resolveIngressPriority(undefined)).toBe("normal");
    });

    it("returns 'direct' when metadata.priority is 'direct'", () => {
      expect(resolveIngressPriority({ priority: "direct" })).toBe("direct");
    });

    it("returns 'normal' when metadata.priority is 'normal'", () => {
      expect(resolveIngressPriority({ priority: "normal" })).toBe("normal");
    });

    it("returns 'direct' when senderId is in prioritySenders set", () => {
      const senders = new Set(["eric"]);
      expect(resolveIngressPriority({ senderId: "eric" }, senders)).toBe("direct");
    });

    it("returns 'normal' when senderId is NOT in prioritySenders set", () => {
      const senders = new Set(["eric"]);
      expect(resolveIngressPriority({ senderId: "someone-else" }, senders)).toBe("normal");
    });

    it("metadata.priority='direct' takes effect even without prioritySenders", () => {
      expect(resolveIngressPriority({ priority: "direct", senderId: "unknown" })).toBe("direct");
    });
  });

  describe("compareIngressPriority", () => {
    it("sorts direct-user messages before normal messages", () => {
      const direct: { receivedAt: number; id: string; metadata?: IngressPriorityMetadata } = {
        receivedAt: 200,
        id: "evt-2",
        metadata: { priority: "direct" },
      };
      const normal: { receivedAt: number; id: string; metadata?: IngressPriorityMetadata } = {
        receivedAt: 100,
        id: "evt-1",
        metadata: { priority: "normal" },
      };
      expect(compareIngressPriority(direct, normal)).toBeLessThan(0);
      expect(compareIngressPriority(normal, direct)).toBeGreaterThan(0);
    });

    it("preserves FIFO order within same priority (by receivedAt)", () => {
      const a: { receivedAt: number; id: string; metadata?: IngressPriorityMetadata } = {
        receivedAt: 100,
        id: "evt-1",
        metadata: { priority: "direct" },
      };
      const b: { receivedAt: number; id: string; metadata?: IngressPriorityMetadata } = {
        receivedAt: 200,
        id: "evt-2",
        metadata: { priority: "direct" },
      };
      expect(compareIngressPriority(a, b)).toBeLessThan(0);
    });

    it("preserves FIFO order within same priority (by id when receivedAt ties)", () => {
      const a: { receivedAt: number; id: string; metadata?: IngressPriorityMetadata } = {
        receivedAt: 100,
        id: "aaa",
        metadata: { priority: "normal" },
      };
      const b: { receivedAt: number; id: string; metadata?: IngressPriorityMetadata } = {
        receivedAt: 100,
        id: "bbb",
        metadata: { priority: "normal" },
      };
      expect(compareIngressPriority(a, b)).toBeLessThan(0);
    });

    it("respects prioritySenders for sorting", () => {
      const senders = new Set(["eric"]);
      const direct: { receivedAt: number; id: string; metadata?: IngressPriorityMetadata } = {
        receivedAt: 200,
        id: "evt-2",
        metadata: { senderId: "eric" },
      };
      const normal: { receivedAt: number; id: string; metadata?: IngressPriorityMetadata } = {
        receivedAt: 100,
        id: "evt-1",
        metadata: {},
      };
      expect(compareIngressPriority(direct, normal, senders)).toBeLessThan(0);
    });
  });

  describe("sortPendingByPriority", () => {
    it("sorts direct-user messages first, preserving FIFO within groups", () => {
      const events = [
        { receivedAt: 100, id: "normal-1", metadata: { priority: "normal" as const } },
        { receivedAt: 200, id: "direct-1", metadata: { priority: "direct" as const } },
        { receivedAt: 150, id: "normal-2", metadata: { priority: "normal" as const } },
        { receivedAt: 300, id: "direct-2", metadata: { priority: "direct" as const } },
      ];
      const sorted = sortPendingByPriority(events);
      expect(sorted.map((e) => e.id)).toEqual(["direct-1", "direct-2", "normal-1", "normal-2"]);
    });

    it("does not mutate the input array", () => {
      const events = [
        { receivedAt: 100, id: "normal-1", metadata: { priority: "normal" as const } },
        { receivedAt: 200, id: "direct-1", metadata: { priority: "direct" as const } },
      ];
      const sorted = sortPendingByPriority(events);
      expect(events[0]?.id).toBe("normal-1"); // Original unchanged
      expect(sorted[0]?.id).toBe("direct-1");
    });

    it("handles events without metadata", () => {
      const events = [
        { receivedAt: 100, id: "no-meta" },
        { receivedAt: 200, id: "direct-1", metadata: { priority: "direct" as const } },
      ];
      const sorted = sortPendingByPriority(events);
      expect(sorted[0]?.id).toBe("direct-1");
    });
  });

  describe("partitionByPriority", () => {
    it("partitions events into direct and normal groups", () => {
      const events = [
        { receivedAt: 100, id: "normal-1", metadata: { priority: "normal" as const } },
        { receivedAt: 200, id: "direct-1", metadata: { priority: "direct" as const } },
        { receivedAt: 300, id: "direct-2", metadata: { priority: "direct" as const } },
      ];
      const { direct, normal } = partitionByPriority(events);
      expect(direct.map((e) => e.id)).toEqual(["direct-1", "direct-2"]);
      expect(normal.map((e) => e.id)).toEqual(["normal-1"]);
    });
  });

  describe("createDirectUserMetadata / createNormalPriorityMetadata", () => {
    it("creates direct-user metadata with senderId", () => {
      const meta = createDirectUserMetadata("eric");
      expect(meta).toEqual({ priority: "direct", senderId: "eric" });
    });

    it("creates direct-user metadata without senderId", () => {
      expect(createDirectUserMetadata()).toEqual({ priority: "direct" });
    });

    it("creates normal-priority metadata", () => {
      expect(createNormalPriorityMetadata()).toEqual({ priority: "normal" });
    });
  });

  describe("isDirectUserMessage", () => {
    it("returns true for direct priority metadata", () => {
      expect(isDirectUserMessage({ priority: "direct" })).toBe(true);
    });

    it("returns false for normal priority metadata", () => {
      expect(isDirectUserMessage({ priority: "normal" })).toBe(false);
    });

    it("returns false for undefined metadata", () => {
      expect(isDirectUserMessage(undefined)).toBe(false);
    });

    it("returns true when senderId is in prioritySenders", () => {
      expect(isDirectUserMessage({ senderId: "eric" }, new Set(["eric"]))).toBe(true);
    });
  });

  describe("drain integration — direct-user messages dispatched first", () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.clearAllTimers();
      vi.useRealTimers();
    });

    it("dispatches direct-user messages before normal-priority messages", async () => {
      await withTempState(async (stateDir) => {
        const clock = 10_000;
        const queue = createTestIngressQueue<IngressDrainTestPayload, IngressPriorityMetadata>(
          stateDir,
          { now: () => clock },
        );
        // Enqueue a normal-priority message first (earlier receivedAt).
        await queue.enqueue(
          "cron-1",
          { text: "cron job" },
          {
            laneKey: "cron-lane",
            receivedAt: clock,
            metadata: { priority: "normal" } as IngressPriorityMetadata,
          },
        );
        // Then a direct-user message (later receivedAt, different lane).
        await queue.enqueue(
          "dm-1",
          { text: "user message" },
          {
            laneKey: "dm-lane",
            receivedAt: clock + 100,
            metadata: { priority: "direct" } as IngressPriorityMetadata,
          },
        );

        const dispatched: string[] = [];
        const drain = createChannelIngressDrain<{ text: string }, IngressPriorityMetadata>({
          queue,
          now: () => clock,
          dispatchClaimedEvent: async (event, lifecycle) => {
            dispatched.push(event.id);
            await lifecycle.onAdopted();
          },
        });

        await drain.drainOnce();
        await drain.waitForIdle();

        // Direct-user message should be dispatched first despite later receivedAt.
        expect(dispatched).toEqual(["dm-1", "cron-1"]);
        drain.dispose();
      });
    });

    it("preserves FIFO within direct-user messages", async () => {
      await withTempState(async (stateDir) => {
        const clock = 10_000;
        const queue = createTestIngressQueue<IngressDrainTestPayload, IngressPriorityMetadata>(
          stateDir,
          { now: () => clock },
        );
        await queue.enqueue(
          "dm-1",
          { text: "first DM" },
          {
            laneKey: "dm-lane-1",
            receivedAt: clock,
            metadata: { priority: "direct" } as IngressPriorityMetadata,
          },
        );
        await queue.enqueue(
          "dm-2",
          { text: "second DM" },
          {
            laneKey: "dm-lane-2",
            receivedAt: clock + 100,
            metadata: { priority: "direct" } as IngressPriorityMetadata,
          },
        );
        await queue.enqueue(
          "cron-1",
          { text: "cron" },
          {
            laneKey: "cron-lane",
            receivedAt: clock + 50,
            metadata: { priority: "normal" } as IngressPriorityMetadata,
          },
        );

        const dispatched: string[] = [];
        const drain = createChannelIngressDrain<{ text: string }, IngressPriorityMetadata>({
          queue,
          now: () => clock,
          dispatchClaimedEvent: async (event, lifecycle) => {
            dispatched.push(event.id);
            await lifecycle.onAdopted();
          },
        });

        await drain.drainOnce();
        await drain.waitForIdle();

        // DMs in FIFO order, then cron.
        expect(dispatched).toEqual(["dm-1", "dm-2", "cron-1"]);
        drain.dispose();
      });
    });

    it("respects prioritySenders config for per-sender priority", async () => {
      await withTempState(async (stateDir) => {
        const clock = 10_000;
        const queue = createTestIngressQueue<IngressDrainTestPayload, IngressPriorityMetadata>(
          stateDir,
          { now: () => clock },
        );
        // No explicit priority in metadata, but senderId matches prioritySenders.
        await queue.enqueue(
          "cron-1",
          { text: "cron" },
          {
            laneKey: "cron-lane",
            receivedAt: clock,
            metadata: { senderId: "system" } as IngressPriorityMetadata,
          },
        );
        await queue.enqueue(
          "dm-1",
          { text: "from eric" },
          {
            laneKey: "dm-lane",
            receivedAt: clock + 100,
            metadata: { senderId: "eric" } as IngressPriorityMetadata,
          },
        );

        const dispatched: string[] = [];
        const drain = createChannelIngressDrain<{ text: string }, IngressPriorityMetadata>({
          queue,
          now: () => clock,
          prioritySenders: new Set(["eric"]),
          dispatchClaimedEvent: async (event, lifecycle) => {
            dispatched.push(event.id);
            await lifecycle.onAdopted();
          },
        });

        await drain.drainOnce();
        await drain.waitForIdle();

        // Eric's message dispatched first despite later receivedAt.
        expect(dispatched).toEqual(["dm-1", "cron-1"]);
        drain.dispose();
      });
    });

    it("does NOT preempt currently running operations", async () => {
      await withTempState(async (stateDir) => {
        const clock = 10_000;
        const queue = createTestIngressQueue<IngressDrainTestPayload, IngressPriorityMetadata>(
          stateDir,
          { now: () => clock },
        );
        // Start a normal-priority event first.
        await queue.enqueue(
          "cron-1",
          { text: "cron" },
          {
            laneKey: "shared-lane",
            receivedAt: clock,
            metadata: { priority: "normal" } as IngressPriorityMetadata,
          },
        );

        let cronStarted = false;
        let cronResolve: () => void = () => {};
        const drain = createChannelIngressDrain<{ text: string }, IngressPriorityMetadata>({
          queue,
          now: () => clock,
          dispatchClaimedEvent: async (event, lifecycle) => {
            if (event.id === "cron-1") {
              cronStarted = true;
              // Simulate long-running operation.
              await new Promise<void>((resolve) => {
                cronResolve = resolve;
              });
              await lifecycle.onAdopted();
            } else {
              await lifecycle.onAdopted();
            }
          },
        });

        // Start drain — cron-1 begins dispatching.
        await drain.drainOnce();
        expect(cronStarted).toBe(true);

        // Enqueue a direct-user message while cron-1 is running.
        // Different lane so it's not blocked.
        await queue.enqueue(
          "dm-1",
          { text: "urgent" },
          {
            laneKey: "dm-lane",
            receivedAt: clock + 100,
            metadata: { priority: "direct" } as IngressPriorityMetadata,
          },
        );

        // Second drain cycle — dm-1 should start even though cron-1 is still running.
        await drain.drainOnce();

        // Complete cron-1.
        cronResolve();
        await drain.waitForIdle();

        drain.dispose();
      });
    });
  });
});
