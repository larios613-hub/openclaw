/**
 * Direct-user message priority for ingress dispatch.
 *
 * FIX 4: Direct-user messages (from authorized senders) must have dispatch
 * priority over cron/autonomous work. This module provides metadata tagging,
 * priority resolution, and drain integration helpers.
 *
 * Priority is stored in the queue's metadata_json field as:
 *   { "priority": "direct" | "normal" }
 *
 * Direct-user messages are sorted before normal-priority messages in the
 * drain's candidate window. FIFO is preserved within the same priority level.
 */

/** Priority level for an ingress event. */
export type IngressPriority = "direct" | "normal";

/** Metadata shape for priority-tagged ingress events. */
export type IngressPriorityMetadata = {
  priority?: IngressPriority;
  /** Authorized sender identifier for per-sender configurability. */
  senderId?: string;
};

/** Default priority for cron/autonomous work. */
export const DEFAULT_PRIORITY: IngressPriority = "normal";

/** Priority rank: lower number = higher priority (dispatched first). */
const PRIORITY_RANK: Record<IngressPriority, number> = {
  direct: 0,
  normal: 1,
};

/**
 * Resolve the priority of an ingress queue record from its metadata.
 * Returns "normal" if metadata is absent or lacks a priority field.
 *
 * If `prioritySenders` is provided, events whose metadata.senderId is in the
 * set are treated as "direct" priority regardless of the metadata.priority field.
 */
export function resolveIngressPriority<TMetadata extends IngressPriorityMetadata | undefined>(
  metadata: TMetadata,
  prioritySenders?: ReadonlySet<string>,
): IngressPriority {
  // When prioritySenders allowlist is configured, it is authoritative —
  // deny-by-default: only senders in the allowlist get direct priority.
  // metadata.priority field is ignored when the allowlist is set.
  if (prioritySenders) {
    if (metadata?.senderId && prioritySenders.has(metadata.senderId)) {
      return "direct";
    }
    return "normal";
  }
  // When no allowlist is configured, fall back to metadata.priority (caller-supplied).
  return metadata?.priority === "direct" ? "direct" : "normal";
}

/**
 * Create metadata for a direct-user message.
 * Use this when enqueuing a message from an authorized sender.
 */
export function createDirectUserMetadata(senderId?: string): IngressPriorityMetadata {
  return { priority: "direct", ...(senderId !== undefined ? { senderId } : {}) };
}

/**
 * Create metadata for a normal-priority (cron/autonomous) message.
 */
export function createNormalPriorityMetadata(): IngressPriorityMetadata {
  return { priority: "normal" };
}

/**
 * Compare two ingress events by priority for sorting.
 * Direct-user events sort before normal events.
 * Within the same priority, FIFO order is preserved (by receivedAt, then id).
 *
 * Returns negative if a should be dispatched before b.
 */
export function compareIngressPriority<
  TRecord extends { receivedAt: number; id: string; metadata?: IngressPriorityMetadata },
>(
  a: TRecord,
  b: TRecord,
  prioritySenders?: ReadonlySet<string>,
): number {
  const pa = PRIORITY_RANK[resolveIngressPriority(a.metadata, prioritySenders)];
  const pb = PRIORITY_RANK[resolveIngressPriority(b.metadata, prioritySenders)];
  if (pa !== pb) {
    return pa - pb;
  }
  // FIFO within same priority: sort by receivedAt, then id.
  if (a.receivedAt !== b.receivedAt) {
    return a.receivedAt - b.receivedAt;
  }
  return a.id.localeCompare(b.id);
}

/**
 * Sort a list of pending ingress events by priority.
 * Direct-user messages appear first, then normal-priority messages.
 * Within each priority level, FIFO order is preserved.
 *
 * This does NOT mutate the input array — it returns a new sorted array.
 */
export function sortPendingByPriority<
  TRecord extends { receivedAt: number; id: string; metadata?: IngressPriorityMetadata },
>(pending: readonly TRecord[], prioritySenders?: ReadonlySet<string>): TRecord[] {
  return [...pending].toSorted((a, b) => compareIngressPriority(a, b, prioritySenders));
}

/**
 * Partition pending events into direct-user and normal-priority groups.
 * Each group is FIFO-sorted by receivedAt, then id.
 */
export function partitionByPriority<
  TRecord extends { receivedAt: number; id: string; metadata?: IngressPriorityMetadata },
>(pending: readonly TRecord[], prioritySenders?: ReadonlySet<string>): { direct: TRecord[]; normal: TRecord[] } {
  const direct: TRecord[] = [];
  const normal: TRecord[] = [];
  for (const event of pending) {
    if (resolveIngressPriority(event.metadata, prioritySenders) === "direct") {
      direct.push(event);
    } else {
      normal.push(event);
    }
  }
  // Each group is already in original order (FIFO by receivedAt from listPending).
  return { direct, normal };
}

/**
 * Check whether an event is a direct-user message.
 */
export function isDirectUserMessage<TMetadata extends IngressPriorityMetadata | undefined>(
  metadata: TMetadata,
  prioritySenders?: ReadonlySet<string>,
): boolean {
  return resolveIngressPriority(metadata, prioritySenders) === "direct";
}