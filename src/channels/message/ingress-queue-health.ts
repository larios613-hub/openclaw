/** Redacted health diagnostics for durable channel ingress queues. */
import { executeSqliteQuerySync } from "../../infra/kysely-sync.js";
import { INGRESS_CLAIM_LEASE_MS } from "./ingress-claim-owner.js";
import { getChannelIngressKysely, openChannelIngressDatabase } from "./ingress-queue.js";
import { DEFAULT_INGRESS_RETRY_MAX_ATTEMPTS } from "./ingress-retry-policy.js";

/**
 * Ingress backlog health metrics for a single channel account.
 *
 * FIX 5: Surface received-but-undispatched message counts, oldest pending age,
 * dispatch success/failure counts, and quarantine count for operator health surfaces.
 */
export type IngressBacklogHealth = {
  channelId: string;
  accountId: string;
  ingressPendingCount: number;
  ingressOldestPendingAge: number;
  ingressDispatchSuccessCount: number;
  ingressDispatchFailureCount: number;
  ingressQuarantineCount: number;
  ingressHealthStatus: "healthy" | "degraded" | "unhealthy";
};

/**
 * Per-interval dispatch counters tracked in memory for health reporting.
 * The drain calls `recordDispatchSuccess` / `recordDispatchFailure` on each settle.
 */
type DispatchCounters = {
  successCount: number;
  failureCount: number;
};

const dispatchCountersByQueue = new Map<string, DispatchCounters>();

/** Record a successful dispatch for health reporting. Called by the drain after settle. */
export function recordIngressDispatchSuccess(queueName: string): void {
  const counters = dispatchCountersByQueue.get(queueName);
  if (counters) {
    counters.successCount += 1;
  } else {
    dispatchCountersByQueue.set(queueName, { successCount: 1, failureCount: 0 });
  }
}

/** Record a failed dispatch (dead-letter) for health reporting. Called by the drain after settle. */
export function recordIngressDispatchFailure(queueName: string): void {
  const counters = dispatchCountersByQueue.get(queueName);
  if (counters) {
    counters.failureCount += 1;
  } else {
    dispatchCountersByQueue.set(queueName, { successCount: 0, failureCount: 1 });
  }
}

/** Record a quarantined update for health reporting. Called when an event is dead-lettered. */
export function recordIngressQuarantine(queueName: string): void {
  // Quarantine is tracked via failed events count in the DB; no separate counter needed.
  // The health collector reads failed events directly from the queue.
  // This function exists as a seam for future in-memory quarantine tracking.
  void queueName;
}

/** Reset dispatch counters after they have been consumed by a health snapshot. */
export function resetIngressDispatchCounters(queueName: string): void {
  dispatchCountersByQueue.delete(queueName);
}

/**
 * Collect ingress backlog health for all channel accounts.
 *
 * Health status rules:
 * - HEALTHY: no pending OR (pending < 60s AND dispatch_success > 0)
 * - DEGRADED: pending 60-300s
 * - UNHEALTHY: pending > 300s OR quarantine_count > 0
 */
export function collectIngressBacklogHealth(stateDir?: string): IngressBacklogHealth[] {
  const database = openChannelIngressDatabase(stateDir);
  const queueDb = getChannelIngressKysely(database.db);
  const now = Date.now();

  // Count pending (received-but-undispatched) events per channel account.
  const pendingRows = executeSqliteQuerySync(
    database.db,
    queueDb
      .selectFrom("channel_ingress_events")
      .select((eb) => [
        "channel_id as channelId",
        "account_id as accountId",
        eb.fn.countAll<number>().as("pendingCount"),
        eb.fn.min<number>("received_at").as("oldestReceivedAt"),
      ])
      .where("status", "=", "pending")
      .groupBy(["channel_id", "account_id"])
      .orderBy("channel_id", "asc")
      .orderBy("account_id", "asc"),
  ).rows;

  // Count quarantined (failed/dead-lettered) events per channel account.
  const quarantineRows = executeSqliteQuerySync(
    database.db,
    queueDb
      .selectFrom("channel_ingress_events")
      .select((eb) => [
        "channel_id as channelId",
        "account_id as accountId",
        eb.fn.countAll<number>().as("quarantineCount"),
      ])
      .where("status", "=", "failed")
      .groupBy(["channel_id", "account_id"])
      .orderBy("channel_id", "asc")
      .orderBy("account_id", "asc"),
  ).rows;

  // Build a lookup for quarantine counts.
  const quarantineByKey = new Map<string, number>();
  for (const row of quarantineRows) {
    const key = `${row.channelId}\0${row.accountId}`;
    quarantineByKey.set(key, row.quarantineCount as number); // SAFETY: SQLite returns unknown type
  }

  // Build health summaries from pending data + dispatch counters + quarantine counts.
  const results: IngressBacklogHealth[] = [];
  for (const row of pendingRows) {
    const channelId = row.channelId as string; // SAFETY: column is TEXT
    const accountId = row.accountId as string; // SAFETY: column is TEXT
    const queueName = JSON.stringify([channelId, accountId]);
    const pendingCount = row.pendingCount as number; // SAFETY: column is INTEGER
    const oldestReceivedAt = row.oldestReceivedAt as number | null; // SAFETY: column is INTEGER or NULL
    const oldestPendingAge = oldestReceivedAt !== null ? Math.max(0, Math.floor((now - oldestReceivedAt) / 1000)) : 0;
    const quarantineCount = quarantineByKey.get(`${channelId}\0${accountId}`) ?? 0;
    const counters = dispatchCountersByQueue.get(queueName);
    const dispatchSuccessCount = counters?.successCount ?? 0;
    const dispatchFailureCount = counters?.failureCount ?? 0;

    let healthStatus: IngressBacklogHealth["ingressHealthStatus"];
    if (quarantineCount > 0 || oldestPendingAge > 300) {
      healthStatus = "unhealthy";
    } else if (oldestPendingAge >= 60) {
      healthStatus = "degraded";
    } else if (pendingCount === 0 || (oldestPendingAge < 60 && dispatchSuccessCount > 0)) {
      healthStatus = "healthy";
    } else {
      // Pending exists but no recent dispatch success — still healthy if young.
      healthStatus = oldestPendingAge < 60 ? "healthy" : "degraded";
    }

    results.push({
      channelId,
      accountId,
      ingressPendingCount: pendingCount,
      ingressOldestPendingAge: oldestPendingAge,
      ingressDispatchSuccessCount: dispatchSuccessCount,
      ingressDispatchFailureCount: dispatchFailureCount,
      ingressQuarantineCount: quarantineCount,
      ingressHealthStatus: healthStatus,
    });
  }

  // Also include accounts that have quarantine events but no pending events.
  for (const row of quarantineRows) {
    const channelId = row.channelId as string; // SAFETY: column is TEXT
    const accountId = row.accountId as string; // SAFETY: column is TEXT
    
    if (results.some((r) => r.channelId === channelId && r.accountId === accountId)) {
      continue;
    }
    const queueName = JSON.stringify([channelId, accountId]);
    const quarantineCount = row.quarantineCount as number; // SAFETY: column is INTEGER
    const counters = dispatchCountersByQueue.get(queueName);
    results.push({
      channelId,
      accountId,
      ingressPendingCount: 0,
      ingressOldestPendingAge: 0,
      ingressDispatchSuccessCount: counters?.successCount ?? 0,
      ingressDispatchFailureCount: counters?.failureCount ?? 0,
      ingressQuarantineCount: quarantineCount,
      ingressHealthStatus: "unhealthy",
    });
  }

  return results;
}

/** Count failed channel ingress events per channel account for operator health surfaces. */
export function countFailedChannelIngressQueueEntries(stateDir?: string) {
  const database = openChannelIngressDatabase(stateDir);
  const queueDb = getChannelIngressKysely(database.db);
  const rows = executeSqliteQuerySync(
    database.db,
    queueDb
      .selectFrom("channel_ingress_events")
      .select((eb) => [
        "channel_id as channelId",
        "account_id as accountId",
        eb.fn.countAll<number>().as("count"),
        eb.fn.min<number>("failed_at").as("oldestFailedAt"),
      ])
      .where("status", "=", "failed")
      .groupBy(["channel_id", "account_id"])
      .orderBy("channel_id", "asc")
      .orderBy("account_id", "asc"),
  ).rows;
  return rows.map(({ oldestFailedAt, ...row }) =>
    oldestFailedAt == null ? row : Object.assign(row, { oldestFailedAt }),
  );
}

/** Aggregate active lanes whose retry or claim state can block later ingress. */
export function countChannelIngressQueuePressure(stateDir?: string) {
  const database = openChannelIngressDatabase(stateDir);
  const queueDb = getChannelIngressKysely(database.db);
  const staleClaimCutoff = Date.now() - INGRESS_CLAIM_LEASE_MS;
  const laneTotals = queueDb
    .selectFrom("channel_ingress_events")
    .select((eb) => [
      "channel_id",
      "account_id",
      eb.fn.countAll<number>().as("activeCount"),
      eb.fn.countAll<number>().filterWhere("status", "=", "pending").as("pendingCount"),
      eb.fn.countAll<number>().filterWhere("status", "=", "claimed").as("claimedCount"),
      eb.fn.min<number>("received_at").as("oldestReceivedAt"),
    ])
    .where("status", "in", ["pending", "claimed"])
    .where("lane_key", "is not", null)
    .groupBy(["queue_name", "lane_key", "channel_id", "account_id"])
    .having((eb) =>
      eb.or([
        eb(
          eb.fn
            .countAll<number>()
            .filterWhere((filter) =>
              filter.and([
                filter("attempts", ">=", DEFAULT_INGRESS_RETRY_MAX_ATTEMPTS),
                filter("last_error", "is not", null),
              ]),
            ),
          ">",
          0,
        ),
        eb(
          eb.fn
            .countAll<number>()
            .filterWhere((filter) =>
              filter.and([
                filter("status", "=", "claimed"),
                filter("claimed_at", "<=", staleClaimCutoff),
              ]),
            ),
          ">",
          0,
        ),
      ]),
    )
    .as("lanes");
  return executeSqliteQuerySync(
    database.db,
    queueDb
      .selectFrom(laneTotals)
      .select((eb) => [
        "lanes.channel_id as channelId",
        "lanes.account_id as accountId",
        eb.fn.countAll<number>().as("laneCount"),
        eb.fn.sum<number>("lanes.pendingCount").as("pendingCount"),
        eb.fn.sum<number>("lanes.claimedCount").as("claimedCount"),
        eb(eb.fn.sum<number>("lanes.activeCount"), "-", eb.fn.countAll<number>()).as(
          "blockedCount",
        ),
        eb.fn.min<number>("lanes.oldestReceivedAt").as("oldestReceivedAt"),
      ])
      .groupBy(["lanes.channel_id", "lanes.account_id"])
      .orderBy("lanes.channel_id", "asc")
      .orderBy("lanes.account_id", "asc"),
  ).rows;
}
