import { channel } from "node:diagnostics_channel";
import path from "node:path";
import { expectDefined } from "@openclaw/normalization-core/expect";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import { readOpenClawAgentDatabaseIdentity } from "../../state/openclaw-agent-db-identity.js";
import {
  closeOpenClawAgentDatabasesForTest,
  runOpenClawAgentWriteTransaction,
} from "../../state/openclaw-agent-db.js";
import {
  deleteSessionEntryLifecycle,
  loadSessionEntry,
  loadTranscriptEvents,
  replaceSessionEntry,
  replaceTranscriptEventsSync,
} from "./session-accessor.js";
import {
  readSqliteSessionGenerationClaim,
  readSqliteSessionGenerationWindows,
} from "./session-accessor.sqlite-generation-copy.js";
import { appendTranscriptEventInTransaction } from "./session-accessor.sqlite-transcript-store.js";
import { replaceTranscriptEvents } from "./session-accessor.sqlite-transcript-write.js";
import { resolveSqliteTargetFromSessionStorePath } from "./session-sqlite-target.js";

const archiveMaterializationHook = vi.hoisted<{
  afterMaterialize: (() => void) | undefined;
  onMaterialize: ((sessionIds: string[]) => void) | undefined;
}>(() => ({ afterMaterialize: undefined, onMaterialize: undefined }));

vi.mock("./session-accessor.sqlite-archive.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./session-accessor.sqlite-archive.js")>();
  return {
    ...actual,
    materializeSessionStateDeletePlans: async (
      ...args: Parameters<typeof actual.materializeSessionStateDeletePlans>
    ) => {
      archiveMaterializationHook.onMaterialize?.(args[0].map((plan) => plan.sessionId));
      const result = await actual.materializeSessionStateDeletePlans(...args);
      archiveMaterializationHook.afterMaterialize?.();
      return result;
    },
  };
});

const tempDirs = useAutoCleanupTempDirTracker(afterEach);

describe("SQLite lifecycle generation cleanup races", () => {
  let storePath: string;

  beforeEach(() => {
    storePath = path.join(
      tempDirs.make("openclaw-session-generation-cleanup-race-"),
      "agents",
      "main",
      "sessions",
      "sessions.json",
    );
  });

  afterEach(() => {
    archiveMaterializationHook.afterMaterialize = undefined;
    archiveMaterializationHook.onMaterialize = undefined;
    closeOpenClawAgentDatabasesForTest();
  });

  it.each(["target history", "other history", "current", "new history"] as const)(
    "preserves exact generation claims when %s changes after cleanup planning",
    async (changed) => {
      const sessionKey = "agent:main:generation-claim-race";
      const historicalIds = ["claim-history-first", "claim-history-second"];
      const currentId = "claim-current";
      const addedId = "claim-added-after-planning";
      const sessionIds = [...historicalIds, currentId];
      const event = (sessionId: string) => ({
        type: "message" as const,
        id: "answer",
        message: {
          role: "assistant" as const,
          content: `${sessionId} transcript`,
          idempotencyKey: "recorded-owner",
        },
      });
      for (const [index, sessionId] of sessionIds.entries()) {
        await replaceSessionEntry({ sessionKey, storePath }, { sessionId, updatedAt: index + 1 });
        await replaceTranscriptEvents({ sessionKey, sessionId, storePath }, [event(sessionId)]);
      }
      const currentEntry = expectDefined(
        loadSessionEntry({ sessionKey, storePath }),
        "Expected current generation before cleanup",
      );
      const databaseOptions = {
        agentId: "main",
        path: expectDefined(
          resolveSqliteTargetFromSessionStorePath(storePath, { agentId: "main" }).path,
          "Expected generation cleanup database path",
        ),
      };
      const expected = runOpenClawAgentWriteTransaction(
        (database) => ({
          expectedDatabaseIdentity: readOpenClawAgentDatabaseIdentity(database).identity,
          expectedGenerations: readSqliteSessionGenerationWindows(database, [sessionKey], []).map(
            (window) => readSqliteSessionGenerationClaim(database, window),
          ),
        }),
        databaseOptions,
      );
      let plannedId: string | undefined;
      let injected = false;
      let mutation: { sessionId: string; rowsChanged: number } | undefined;
      let mutationError: unknown;
      archiveMaterializationHook.onMaterialize = (ids) => {
        [plannedId] = ids;
      };
      const diagnostics = channel("openclaw.session.write");
      const onPlanningComplete = (message: unknown) => {
        if (
          injected ||
          !plannedId ||
          !isRecord(message) ||
          message.operation !== "session.lifecycle.reclamation-plan" ||
          message.writer !== "foreground" ||
          message.outcome !== "ok" ||
          (changed === "new history") !== (plannedId === currentId)
        ) {
          return;
        }
        injected = true;
        const sessionId =
          changed === "new history"
            ? addedId
            : changed === "current"
              ? currentId
              : changed === "other history"
                ? historicalIds.find((id) => id !== plannedId)!
                : plannedId;
        // Planning has released its admission; the reclamation Worker has not started.
        try {
          mutation = runOpenClawAgentWriteTransaction((database) => {
            const rowsChanged =
              changed === "new history"
                ? Number(
                    appendTranscriptEventInTransaction(
                      database,
                      { ...databaseOptions, sessionKey, sessionId },
                      event(sessionId),
                    ) !== false,
                  )
                : Number(
                    database.db
                      .prepare(
                        "UPDATE transcript_event_identities SET message_idempotency_key = NULL WHERE session_id = ? AND event_id = 'answer'",
                      )
                      .run(sessionId).changes,
                  );
            return { sessionId, rowsChanged };
          }, databaseOptions);
        } catch (error) {
          // diagnostics_channel subscribers cannot propagate failures to the awaited deletion.
          mutationError = error;
        }
      };
      diagnostics.subscribe(onPlanningComplete);
      try {
        const result = await deleteSessionEntryLifecycle({
          ...expected,
          archiveTranscript: false,
          deleteTranscriptWithoutArchive: true,
          expectedEntry: currentEntry,
          storePath,
          target: { canonicalKey: sessionKey, storeKeys: [sessionKey] },
        });
        expect(mutationError).toBeUndefined();
        expect(mutation).toMatchObject({ rowsChanged: 1 });
        expect(result).toMatchObject({ deleted: false, expectedEntryMismatch: true });
        expect(loadSessionEntry({ sessionKey, storePath })).toEqual(currentEntry);
        const retainedIds = new Set([currentId]);
        if (changed === "target history") {
          for (const sessionId of historicalIds) {
            retainedIds.add(sessionId);
          }
        } else if (changed !== "current") {
          retainedIds.add(mutation!.sessionId);
        }
        const remaining = runOpenClawAgentWriteTransaction(
          (database) => ({
            windows: readSqliteSessionGenerationWindows(database, [sessionKey], []).map(
              (window) => window.session_id,
            ),
            identity: database.db
              .prepare(
                "SELECT message_idempotency_key FROM transcript_event_identities WHERE session_id = ? AND event_id = 'answer'",
              )
              .get(mutation!.sessionId),
          }),
          databaseOptions,
        );
        expect(remaining.windows).toEqual([...retainedIds].toSorted());
        expect(remaining.identity).toEqual({
          message_idempotency_key: changed === "new history" ? "recorded-owner" : null,
        });
        for (const sessionId of [...sessionIds, addedId]) {
          await expect(loadTranscriptEvents({ sessionKey, sessionId, storePath })).resolves.toEqual(
            retainedIds.has(sessionId) ? [event(sessionId)] : [],
          );
        }
      } finally {
        diagnostics.unsubscribe(onPlanningComplete);
      }
    },
  );

  it("reports a transcript guard mismatch after publishing earlier history", async () => {
    const sessionKey = "agent:main:historical-guard-race";
    const sessionIds = ["historical-guard-first", "historical-guard-second", "guard-current"];
    const events = sessionIds.map((sessionId) => ({
      type: "session" as const,
      id: sessionId,
      content: `${sessionId} transcript`,
    }));
    for (const [index, sessionId] of sessionIds.entries()) {
      await replaceSessionEntry({ sessionKey, storePath }, { sessionId, updatedAt: index + 1 });
      await replaceTranscriptEvents({ sessionKey, sessionId, storePath }, [events[index]!]);
    }
    const currentEntry = loadSessionEntry({ sessionKey, storePath });
    if (!currentEntry) {
      throw new Error("expected current guarded entry");
    }
    let materializations = 0;
    archiveMaterializationHook.afterMaterialize = () => {
      materializations += 1;
      if (materializations === 2) {
        replaceTranscriptEventsSync({ sessionKey, sessionId: sessionIds[2]!, storePath }, [
          { ...events[2]!, content: "concurrent transcript" },
        ]);
      }
    };

    const result = await deleteSessionEntryLifecycle({
      archiveTranscript: true,
      expectedEntry: currentEntry,
      expectedTranscript: {
        eventJson: [JSON.stringify(events[2])],
        sessionId: sessionIds[2]!,
      },
      storePath,
      target: { canonicalKey: sessionKey, storeKeys: [sessionKey] },
    });

    expect(result).toMatchObject({ deleted: false, expectedEntryMismatch: true });
    expect(result.archivedTranscripts).toHaveLength(1);
    expect(materializations).toBe(2);
    expect(loadSessionEntry({ sessionKey, storePath })).toEqual(currentEntry);
    const archivedSessionId = result.archivedTranscripts[0]?.sessionId;
    expect(sessionIds.slice(0, 2)).toContain(archivedSessionId);
    for (const [index, historicalSessionId] of sessionIds.slice(0, 2).entries()) {
      await expect(
        loadTranscriptEvents({ sessionKey, sessionId: historicalSessionId, storePath }),
      ).resolves.toEqual(historicalSessionId === archivedSessionId ? [] : [events[index]!]);
    }
    await expect(
      loadTranscriptEvents({ sessionKey, sessionId: sessionIds[2]!, storePath }),
    ).resolves.toEqual([{ ...events[2]!, content: "concurrent transcript" }]);
  });
});
