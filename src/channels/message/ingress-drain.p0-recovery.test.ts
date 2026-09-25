import path from "node:path";
import { describe, expect, it } from "vitest";
import { createReplyRestartRecoveryClaimController } from "../../auto-reply/reply/restart-recovery-claim.js";
import { loadSessionEntry, replaceSessionEntry } from "../../config/sessions/session-accessor.js";
import type { InternalSessionEntry as SessionEntry } from "../../config/sessions/types.js";
import { closeOpenClawStateDatabaseForTest } from "../../state/openclaw-state-db.js";
import { createChannelIngressDrain } from "./ingress-drain.js";
import { createTestIngressQueue, withTempState } from "./ingress-drain.test-helpers.js";
import { resolveIngressRetryDelayMs } from "./ingress-retry-policy.js";

// P0-01..08,12: exercise durable queue -> drain -> actual admission failure.
// Database reopen proves persistence, not an OS/Gateway process restart (P0-10).
describe("P0 durable poison recovery", () => {
  it.each(["dispatch", "abandon", "admission", "wrapped-admission"] as const)(
    "%s cannot monopolize a lane across 51 drain cycles and database reopen",
    async (failure) => {
      await withTempState(async (stateDir) => {
        let now = 10_000;
        let queue = createTestIngressQueue(stateDir, { now: () => now });
        const storePath = path.join(stateDir, "agents/main/sessions/sessions.json");
        const sessionKey = "agent:main:telegram:direct:p0-fixture";
        const entry: SessionEntry = {
          sessionId: "p0-test-session",
          updatedAt: now,
          status: "running",
          abortedLastRun: true,
          restartRecoveryDeliveryRunId: "stale-recovery",
        };
        const isAdmission = failure === "admission" || failure === "wrapped-admission";
        if (isAdmission) {
          await replaceSessionEntry({ storePath, sessionKey }, entry);
        }
        const dispatched: string[] = [];
        const logs: string[] = [];
        const createDrain = () =>
          createChannelIngressDrain<{ text: string }>({
            queue,
            now: () => now,
            onLog: (line) => logs.push(line),
            dispatchClaimedEvent: async (event, lifecycle) => {
              dispatched.push(event.id);
              if (event.id === "poison") {
                if (failure === "abandon") {
                  await lifecycle.onAbandoned();
                  return { kind: "deferred" };
                }
                if (isAdmission) {
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
                  let wrappedMessage: string | undefined;
                  try {
                    await controller.admitUserTurn();
                  } catch (error) {
                    if (failure === "wrapped-admission" && error instanceof Error) {
                      // Middleware may coerce a typed error into a generic Error.
                      wrappedMessage = `wrapped admission: ${error.message}`;
                    } else {
                      throw error;
                    }
                  }
                  if (wrappedMessage !== undefined) {
                    throw new Error(wrappedMessage);
                  }
                  throw new Error("Expected durable admission to remain fenced");
                }
                throw new Error("permanent dispatch failure");
              }
              await lifecycle.onAdopted();
              return { kind: "completed" };
            },
          });
        let drain = createDrain();
        const limit = isAdmission ? 3 : 5;
        try {
          await queue.enqueue(
            "poison",
            { text: "preserve evidence" },
            {
              laneKey: "same",
              receivedAt: now,
              metadata: { source: "synthetic-p0" },
            },
          );
          for (let cycle = 0; cycle < 51; cycle++) {
            await drain.drainOnce();
            await drain.waitForIdle();
            if (cycle === 0) {
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
              queue = createTestIngressQueue(stateDir, { now: () => now });
              drain = createDrain();
            }
            if (cycle === limit - 1) {
              // P0-03/07: quarantine a young row, retaining the original evidence.
              expect(await queue.listFailed?.()).toEqual([
                expect.objectContaining({
                  id: "poison",
                  payload: { text: "preserve evidence" },
                  metadata: { source: "synthetic-p0" },
                  reason: "retry-limit-exceeded",
                }),
              ]);
              expect(now - 10_000).toBeLessThan(24 * 60 * 60 * 1000);
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
          expect(dispatched).toEqual([
            ...Array<string>(limit).fill("poison"),
            "later-1",
            "later-2",
          ]);
          expect(await queue.listPending({ limit: "all" })).toEqual([]);
          expect(await queue.listClaims()).toEqual([]);
          expect(logs.some((line) => line.includes("dead-lettered"))).toBe(true);
          // P0-08: each accepted ID has either a retained failure or a completion tombstone.
          expect(await queue.resubmit?.("later-1")).toMatchObject({ kind: "completed" });
          expect(await queue.resubmit?.("later-2")).toMatchObject({ kind: "completed" });
          if (isAdmission) {
            // Containment must not reset the stale session or override ownership (P0-09 limit).
            expect(loadSessionEntry({ storePath, sessionKey })).toMatchObject({
              sessionId: entry.sessionId,
              restartRecoveryDeliveryRunId: entry.restartRecoveryDeliveryRunId,
              abortedLastRun: true,
            });
          }
          drain.dispose();
          closeOpenClawStateDatabaseForTest();
          queue = createTestIngressQueue(stateDir, { now: () => now });
          expect(await queue.listFailed?.()).toEqual([
            expect.objectContaining({ id: "poison", payload: { text: "preserve evidence" } }),
          ]);
          expect(await queue.resubmit?.("later-1")).toMatchObject({ kind: "completed" });
          expect(await queue.resubmit?.("later-2")).toMatchObject({ kind: "completed" });
        } finally {
          drain.dispose();
        }
      });
    },
  );
});
