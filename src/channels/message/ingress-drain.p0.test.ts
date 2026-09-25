import path from "node:path";
import { describe, expect, it } from "vitest";
import { createReplyRestartRecoveryClaimController } from "../../auto-reply/reply/restart-recovery-claim.js";
import { replaceSessionEntry } from "../../config/sessions/session-accessor.js";
import type { InternalSessionEntry } from "../../config/sessions/types.js";
import { closeOpenClawStateDatabaseForTest } from "../../state/openclaw-state-db.js";
import { createChannelIngressDrain } from "./ingress-drain.js";
import { createTestIngressQueue, withTempState } from "./ingress-drain.test-helpers.js";
import { resolveIngressRetryDelayMs } from "./ingress-retry-policy.js";

describe("bounded ingress failure recovery", () => {
  it.each(["dispatch", "abandon", "admission"] as const)(
    "isolates a young %s failure across restart and admits later same-lane messages",
    async (failure) => {
      await withTempState(async (stateDir) => {
        let now = 10_000;
        let queue = createTestIngressQueue(stateDir);
        const storePath = path.join(stateDir, "agents/main/sessions/sessions.json");
        const sessionKey = "agent:main:telegram:direct:test";
        const entry: InternalSessionEntry = {
          sessionId: "test-session",
          updatedAt: now,
          status: "running",
          abortedLastRun: true,
          restartRecoveryDeliveryRunId: "stale-recovery",
        };
        if (failure === "admission") {
          await replaceSessionEntry({ storePath, sessionKey }, entry);
        }
        const dispatched: string[] = [];
        const errors: string[] = [];
        const createDrain = () =>
          createChannelIngressDrain<{ text: string }>({
            queue,
            now: () => now,
            onLog: (line) => errors.push(line),
            dispatchClaimedEvent: async (event, lifecycle) => {
              dispatched.push(event.id);
              if (event.id === "poison") {
                if (failure === "abandon") {
                  await lifecycle.onAbandoned();
                  return { kind: "deferred" };
                }
                if (failure === "admission") {
                  const controller = createReplyRestartRecoveryClaimController({
                    getEntry: () => entry,
                    getSessionId: () => entry.sessionId,
                    setEntry: () => {},
                    lifecycleGeneration: undefined,
                    isRestartAbort: () => false,
                    resolveDeliveryContext: () => undefined,
                    sessionKey,
                    storePath,
                  });
                  await controller.admitUserTurn();
                }
                throw new Error("permanent dispatch failure");
              }
              await lifecycle.onAdopted();
              return { kind: "completed" };
            },
          });
        let drain = createDrain();
        try {
          await queue.enqueue(
            "poison",
            { text: "preserve evidence" },
            { laneKey: "same", receivedAt: now },
          );
          const limit = failure === "admission" ? 3 : 5;
          for (let attempt = 1; attempt <= limit; attempt++) {
            await drain.drainOnce();
            await drain.waitForIdle();
            if (attempt === 1) {
              await queue.enqueue(
                "later-1",
                { text: "first" },
                { laneKey: "same", receivedAt: now + 1 },
              );
              await queue.enqueue(
                "later-2",
                { text: "second" },
                { laneKey: "same", receivedAt: now + 2 },
              );
              drain.dispose();
              closeOpenClawStateDatabaseForTest();
              queue = createTestIngressQueue(stateDir);
              drain = createDrain();
            }
            const pending = (await queue.listPending({ limit: "all" })).find(
              (event) => event.id === "poison",
            );
            if (pending) {
              const delay = resolveIngressRetryDelayMs(pending, undefined, now);
              expect(delay).toBeGreaterThanOrEqual(1_000);
              expect(await drain.drainOnce()).toEqual({ started: 0 });
              now += delay;
            }
          }
          expect(await queue.listFailed?.()).toEqual([
            expect.objectContaining({
              id: "poison",
              payload: { text: "preserve evidence" },
              reason: "retry-limit-exceeded",
            }),
          ]);
          await drain.drainOnce();
          await drain.waitForIdle();
          await drain.drainOnce();
          await drain.waitForIdle();
          expect(dispatched).toEqual([
            ...Array<string>(limit).fill("poison"),
            "later-1",
            "later-2",
          ]);
          expect(await queue.listPending({ limit: "all" })).toEqual([]);
          expect(errors.some((line) => line.includes("dead-lettered"))).toBe(true);
          drain.dispose();
          closeOpenClawStateDatabaseForTest();
          queue = createTestIngressQueue(stateDir);
          expect(await queue.listFailed?.()).toHaveLength(1);
        } finally {
          drain.dispose();
        }
      });
    },
  );
});
