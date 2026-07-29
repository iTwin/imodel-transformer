/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import {
  assertScanMatchesOracle,
  normalizeScanResult,
  ScanCollection,
  scanCollections,
  scanDigest,
  ScanLedger,
  squashLedger,
} from "./validation/scanOracle";

function ledgerOf(
  ...entries: readonly [
    ScanCollection,
    "Deleted" | "Inserted" | "Updated",
    string,
  ][]
): ScanLedger {
  const ledger = new ScanLedger();
  for (const [collection, op, id] of entries) ledger.record(collection, op, id);
  return ledger;
}

describe("changeset scan oracle", () => {
  it("keeps a repeatedly updated instance in updateIds", () => {
    const squashed = squashLedger(
      ledgerOf(
        ["element", "Updated", "0x20"],
        ["element", "Updated", "0x20"],
        ["element", "Updated", "0x20"]
      )
    );
    expect([...squashed.element.updateIds]).to.deep.equal(["0x20"]);
    expect(squashed.element.insertIds.size).to.equal(0);
    expect(squashed.element.deleteIds.size).to.equal(0);
  });

  it("turns update-then-delete into a delete", () => {
    const squashed = squashLedger(
      ledgerOf(["element", "Updated", "0x21"], ["element", "Deleted", "0x21"])
    );
    expect([...squashed.element.deleteIds]).to.deep.equal(["0x21"]);
    expect(squashed.element.updateIds.size).to.equal(0);
  });

  it("keeps insert-then-update as an insert", () => {
    const squashed = squashLedger(
      ledgerOf(["element", "Inserted", "0x22"], ["element", "Updated", "0x22"])
    );
    expect([...squashed.element.insertIds]).to.deep.equal(["0x22"]);
    expect(squashed.element.updateIds.size).to.equal(0);
  });

  it("cancels insert-then-delete entirely", () => {
    const squashed = squashLedger(
      ledgerOf(["element", "Inserted", "0x23"], ["element", "Deleted", "0x23"])
    );
    for (const op of ["insertIds", "updateIds", "deleteIds"] as const)
      expect(squashed.element[op].size, op).to.equal(0);
  });

  it("reinstates an insert after a delete", () => {
    const squashed = squashLedger(
      ledgerOf(
        ["relationship", "Deleted", "0x24"],
        ["relationship", "Inserted", "0x24"]
      )
    );
    expect([...squashed.relationship.insertIds]).to.deep.equal(["0x24"]);
    expect(squashed.relationship.deleteIds.size).to.equal(0);
  });

  it("keeps collections independent", () => {
    const squashed = squashLedger(
      ledgerOf(
        ["element", "Inserted", "0x30"],
        ["model", "Inserted", "0x30"],
        ["aspect", "Deleted", "0x30"],
        ["codeSpec", "Updated", "0x30"]
      )
    );
    expect([...squashed.element.insertIds]).to.deep.equal(["0x30"]);
    expect([...squashed.model.insertIds]).to.deep.equal(["0x30"]);
    expect([...squashed.aspect.deleteIds]).to.deep.equal(["0x30"]);
    expect([...squashed.codeSpec.updateIds]).to.deep.equal(["0x30"]);
    expect(squashed.font.insertIds.size).to.equal(0);
  });

  it("drops repeated inserts and updates without changing the outcome", () => {
    const ledger = new ScanLedger();
    for (let index = 0; index < 50; index++)
      ledger.record("element", "Updated", "0x40");
    ledger.record("element", "Deleted", "0x40");
    expect(ledger.entries).to.have.length(2);
    expect([...squashLedger(ledger).element.deleteIds]).to.deep.equal(["0x40"]);
  });

  it("rejects a repeated delete instead of deduplicating it", () => {
    const ledger = new ScanLedger();
    ledger.record("element", "Inserted", "0x41");
    ledger.record("element", "Deleted", "0x41");
    expect(() => ledger.record("element", "Deleted", "0x41")).to.throw(
      /second "Deleted"/
    );
  });

  // Pins the reason the recorder rejects repeated deletes rather than deduplicating them: the
  // Deleted branch is conditional on insertIds.has(id), and the first delete falsifies that
  // condition. Deduplicating would collapse this to the empty result, which is wrong.
  it("does not treat a repeated delete as idempotent", () => {
    const insertThenDelete = squashLedger([
      { collection: "element", id: "0x42", op: "Inserted" },
      { collection: "element", id: "0x42", op: "Deleted" },
    ]);
    expect(insertThenDelete.element.insertIds.size).to.equal(0);
    expect(insertThenDelete.element.deleteIds.size).to.equal(0);

    const withSecondDelete = squashLedger([
      { collection: "element", id: "0x42", op: "Inserted" },
      { collection: "element", id: "0x42", op: "Deleted" },
      { collection: "element", id: "0x42", op: "Deleted" },
    ]);
    expect([...withSecondDelete.element.deleteIds]).to.deep.equal(["0x42"]);
  });

  // The transformer's Inserted branch clears deleteIds but not updateIds, and its Updated branch
  // guards on insertIds but not deleteIds, so each leaves an id in two collections at once. Neither
  // sequence is producible from a changeset, but both are reachable through addCustomElementChange.
  // The oracle mirrors the behaviour rather than normalizing it, so that it keeps agreeing with the
  it("mirrors the transformer's incomplete reconciliation rather than normalizing it", () => {
    const updateThenInsert = squashLedger([
      { collection: "element", id: "0x43", op: "Updated" },
      { collection: "element", id: "0x43", op: "Inserted" },
    ]);
    expect([...updateThenInsert.element.insertIds]).to.deep.equal(["0x43"]);
    expect([...updateThenInsert.element.updateIds]).to.deep.equal(["0x43"]);

    const deleteThenUpdate = squashLedger([
      { collection: "element", id: "0x44", op: "Deleted" },
      { collection: "element", id: "0x44", op: "Updated" },
    ]);
    expect([...deleteThenUpdate.element.updateIds]).to.deep.equal(["0x44"]);
    expect([...deleteThenUpdate.element.deleteIds]).to.deep.equal(["0x44"]);
  });

  it("records batches of ids", () => {
    const ledger = new ScanLedger();
    ledger.record("aspect", "Inserted", ["0x50", "0x51"]);
    expect([...squashLedger(ledger).aspect.insertIds]).to.deep.equal([
      "0x50",
      "0x51",
    ]);
  });

  it("orders digest ids numerically rather than lexically", () => {
    const ledger = new ScanLedger();
    ledger.record("element", "Inserted", ["0x100", "0x9", "0x20"]);
    expect(
      normalizeScanResult(squashLedger(ledger)).element.insert
    ).to.deep.equal(["0x9", "0x20", "0x100"]);
  });

  it("produces an insertion-order-independent digest", () => {
    const forward = new ScanLedger();
    forward.record("element", "Inserted", ["0x60", "0x61"]);
    const reverse = new ScanLedger();
    reverse.record("element", "Inserted", ["0x61", "0x60"]);
    expect(scanDigest(squashLedger(forward))).to.equal(
      scanDigest(squashLedger(reverse))
    );
  });

  it("covers all six ChangedInstanceIds collections", () => {
    expect([...scanCollections].sort()).to.deep.equal([
      "aspect",
      "codeSpec",
      "element",
      "font",
      "model",
      "relationship",
    ]);
  });

  it("accepts a scan that matches the recipe", () => {
    const expected = squashLedger(ledgerOf(["element", "Updated", "0x70"]));
    expect(() =>
      assertScanMatchesOracle(
        squashLedger(ledgerOf(["element", "Updated", "0x70"])),
        expected
      )
    ).to.not.throw();
  });

  it("reports missing and unexpected ids per collection", () => {
    const expected = squashLedger(
      ledgerOf(["aspect", "Updated", "0x80"], ["aspect", "Updated", "0x81"])
    );
    const actual = squashLedger(
      ledgerOf(["aspect", "Updated", "0x80"], ["aspect", "Updated", "0x82"])
    );
    let message = "";
    try {
      assertScanMatchesOracle(actual, expected);
    } catch (error) {
      message = (error as Error).message;
    }
    expect(message).to.match(/aspect\.updateIds/);
    expect(message).to.match(/missing=\[0x81\]/);
    expect(message).to.match(/unexpected=\[0x82\]/);
  });

  it("fails when a collection outside element regresses", () => {
    const expected = squashLedger(ledgerOf(["element", "Updated", "0x90"]));
    const actual = squashLedger(
      ledgerOf(
        ["element", "Updated", "0x90"],
        ["relationship", "Deleted", "0x91"]
      )
    );
    expect(() => assertScanMatchesOracle(actual, expected)).to.throw(
      /relationship\.deleteIds/
    );
  });
});
