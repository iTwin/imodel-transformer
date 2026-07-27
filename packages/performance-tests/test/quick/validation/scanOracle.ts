/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String } from "@itwin/core-bentley";
import { canonicalSha256 } from "../FixtureManifest";

/**
 * The six collections carried by `ChangedInstanceIds`. Verifying only `element` would let aspect and
 * relationship regressions pass silently, so the scan oracle covers all of them.
 */
export const scanCollections = [
  "aspect",
  "codeSpec",
  "element",
  "font",
  "model",
  "relationship",
] as const;

export type ScanCollection = (typeof scanCollections)[number];

/** Mirrors `SqliteChangeOp` without importing it, so the oracle stays independent of transformer internals. */
export type ScanOp = "Deleted" | "Inserted" | "Updated";

export interface ScanLedgerEntry {
  readonly collection: ScanCollection;
  readonly id: Id64String;
  readonly op: ScanOp;
}

export interface ScanOps {
  readonly insertIds: ReadonlySet<Id64String>;
  readonly updateIds: ReadonlySet<Id64String>;
  readonly deleteIds: ReadonlySet<Id64String>;
}

export type ScanExpectation = Readonly<Record<ScanCollection, ScanOps>>;

/**
 * Ordered record of every change a recipe performs, used to predict the scan result.
 *
 * Repeating an op for the same instance is idempotent under all three squash rules, so the recorder
 * drops repeats. That keeps an update-heavy recipe's ledger proportional to touched instances rather
 * than to touched instances times changesets, without changing the squashed outcome.
 */
export class ScanLedger {
  private readonly _entries: ScanLedgerEntry[] = [];
  private readonly _lastOp = new Map<string, ScanOp>();

  public record(
    collection: ScanCollection,
    op: ScanOp,
    ids: Id64String | Iterable<Id64String>
  ): void {
    const iterable = typeof ids === "string" ? [ids] : ids;
    for (const id of iterable) {
      const key = `${collection}\u0000${id}`;
      if (this._lastOp.get(key) === op) continue;
      this._lastOp.set(key, op);
      this._entries.push({ collection, id, op });
    }
  }

  public get entries(): readonly ScanLedgerEntry[] {
    return this._entries;
  }

  /**
   * Rebuilds a ledger from serialized entries.
   *
   * The recipe runs while the fixture artifact is built; the oracle runs later, against a copy. The
   * ledger therefore has to survive a round trip through JSON. Replaying through `record` keeps the
   * deduplication invariant even if the serialized form was hand-written or concatenated.
   */
  public static fromEntries(entries: readonly ScanLedgerEntry[]): ScanLedger {
    const ledger = new ScanLedger();
    for (const entry of entries)
      ledger.record(entry.collection, entry.op, entry.id);
    return ledger;
  }
}

interface MutableScanOps {
  insertIds: Set<Id64String>;
  updateIds: Set<Id64String>;
  deleteIds: Set<Id64String>;
}

/**
 * Squash semantics, written independently of `ChangedInstanceIds.handleChange`.
 *
 * Reusing the transformer's implementation would check the code against itself. The rules are:
 * an insert clears a pending delete; an update is dropped when the instance is already an insert;
 * a delete cancels a pending insert outright and otherwise supersedes a pending update.
 */
function applyOp(ops: MutableScanOps, op: ScanOp, id: Id64String): void {
  switch (op) {
    case "Inserted":
      ops.insertIds.add(id);
      ops.deleteIds.delete(id);
      return;
    case "Updated":
      if (!ops.insertIds.has(id)) ops.updateIds.add(id);
      return;
    case "Deleted":
      if (ops.insertIds.has(id)) {
        ops.insertIds.delete(id);
        return;
      }
      ops.updateIds.delete(id);
      ops.deleteIds.add(id);
      return;
  }
}

export function squashLedger(ledger: ScanLedger): ScanExpectation {
  const result = Object.fromEntries(
    scanCollections.map((collection) => [
      collection,
      {
        insertIds: new Set<Id64String>(),
        updateIds: new Set<Id64String>(),
        deleteIds: new Set<Id64String>(),
      },
    ])
  ) as Record<ScanCollection, MutableScanOps>;
  for (const entry of ledger.entries)
    applyOp(result[entry.collection], entry.op, entry.id);
  return result;
}

/** Numeric ordering for hex `Id64String`s, so the digest does not depend on string collation. */
export function compareIds(left: Id64String, right: Id64String): number {
  const leftValue = BigInt(left);
  const rightValue = BigInt(right);
  return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}

function sortedIds(ids: ReadonlySet<Id64String>): Id64String[] {
  return [...ids].sort(compareIds);
}

export function normalizeScanResult(
  result: ScanExpectation
): Record<ScanCollection, Record<"delete" | "insert" | "update", string[]>> {
  return Object.fromEntries(
    scanCollections.map((collection) => [
      collection,
      {
        delete: sortedIds(result[collection].deleteIds),
        insert: sortedIds(result[collection].insertIds),
        update: sortedIds(result[collection].updateIds),
      },
    ])
  ) as Record<ScanCollection, Record<"delete" | "insert" | "update", string[]>>;
}

function difference(
  actual: ReadonlySet<Id64String>,
  expected: ReadonlySet<Id64String>
): { missing: Id64String[]; unexpected: Id64String[] } {
  return {
    missing: sortedIds(new Set([...expected].filter((id) => !actual.has(id)))),
    unexpected: sortedIds(
      new Set([...actual].filter((id) => !expected.has(id)))
    ),
  };
}

/**
 * Asserts the scan result against what the recipe says it produced. This is strictly stronger than
 * comparing a digest to its own previous value, which can only detect a change of behaviour and not
 * a behaviour that was wrong from the start.
 */
export function assertScanMatchesOracle(
  actual: ScanExpectation,
  expected: ScanExpectation
): void {
  const mismatches: string[] = [];
  for (const collection of scanCollections) {
    for (const op of ["insertIds", "updateIds", "deleteIds"] as const) {
      const { missing, unexpected } = difference(
        actual[collection][op],
        expected[collection][op]
      );
      if (missing.length === 0 && unexpected.length === 0) continue;
      mismatches.push(
        `${collection}.${op}: expected ${
          expected[collection][op].size
        }, got ${actual[collection][op].size}; missing=[${missing.join(
          ","
        )}], unexpected=[${unexpected.join(",")}]`
      );
    }
  }
  if (mismatches.length > 0)
    throw new Error(
      `Changeset scan did not match the recipe oracle:\n  ${mismatches.join(
        "\n  "
      )}`
    );
}

/**
 * Stable fingerprint over all six collections. Every sample scans a copy of one immutable artifact,
 * so ids are identical across samples and equality is meaningful; it also serves as a cross-arm
 * behaviour gate for A/B comparison.
 */
export function scanDigest(result: ScanExpectation): string {
  return canonicalSha256(normalizeScanResult(result));
}
