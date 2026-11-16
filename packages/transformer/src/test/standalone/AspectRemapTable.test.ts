/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelJsFs, SnapshotDb } from "@itwin/core-backend";
import { Id64 } from "@itwin/core-bentley";
import { expect } from "vitest";
import * as path from "node:path";
import * as v8 from "node:v8";
import * as vm from "node:vm";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations.js";
import { AspectRemapTable } from "../../AspectRemapTable.js";
import { IModelCloneContext } from "../../IModelCloneContext.js";

describe("AspectRemapTable", () => {
  let table: AspectRemapTable;

  beforeEach(() => {
    table = new AspectRemapTable();
  });

  afterEach(() => {
    table[Symbol.dispose]();
  });

  it("returns invalid for missing mappings before and after the first write", () => {
    expect(table.get("0x10")).toBe(Id64.invalid);
    table.set("0x11", "0x21");
    expect(table.get("0x10")).toBe(Id64.invalid);
  });

  it("stores, replaces, and deletes mappings", () => {
    table.set("0x10", "0x20");
    table.set("0x11", "0x21");
    expect(table.get("0x10")).toBe("0x20");
    expect(table.get("0x11")).toBe("0x21");

    table.set("0x10", "0x30");
    expect(table.get("0x10")).toBe("0x30");

    table.delete("0x10");
    expect(table.get("0x10")).toBe(Id64.invalid);
    expect(table.get("0x11")).toBe("0x21");

    table.delete("0x10");
    table.delete("0x12");
    expect(table.get("0x11")).toBe("0x21");
  });

  it("treats an invalid target id as removing the mapping", () => {
    table.set("0x10", "0x20");
    table.set("0x10", Id64.invalid);
    expect(table.get("0x10")).toBe(Id64.invalid);
  });

  it("ignores invalid source ids", () => {
    table.set(Id64.invalid, "0x20");
    table.delete(Id64.invalid);
    expect(table.get(Id64.invalid)).toBe(Id64.invalid);
  });

  it("round-trips ids that use the full 64-bit range", () => {
    const sourceId = Id64.fromLocalAndBriefcaseIds(0xffffffff, 0xffffff);
    const targetId = Id64.fromLocalAndBriefcaseIds(1, 0xfffffe);
    table.set(sourceId, targetId);
    expect(table.get(sourceId)).toBe(targetId);
  });

  it("handles many mappings", () => {
    const count = 20_000;
    const sourceId = (i: number) => Id64.fromLocalAndBriefcaseIds(i + 1, 2);
    const targetId = (i: number) => Id64.fromLocalAndBriefcaseIds(i + 1, 3);
    for (let i = 0; i < count; i++) table.set(sourceId(i), targetId(i));
    for (let i = 0; i < count; i++)
      expect(table.get(sourceId(i))).toBe(targetId(i));
  });

  it("isolates mappings between instances", () => {
    using other = new AspectRemapTable();
    table.set("0x10", "0x20");
    other.set("0x10", "0x30");
    expect(table.get("0x10")).toBe("0x20");
    expect(other.get("0x10")).toBe("0x30");
  });

  it("discards mappings on dispose and can be disposed repeatedly", () => {
    table.set("0x10", "0x20");
    table[Symbol.dispose]();
    expect(table.get("0x10")).toBe(Id64.invalid);
    table[Symbol.dispose]();

    table.set("0x11", "0x21");
    expect(table.get("0x10")).toBe(Id64.invalid);
    expect(table.get("0x11")).toBe("0x21");
  });

  it("closes undisposed databases safely when garbage collected", async () => {
    v8.setFlagsFromString("--expose-gc");
    const gc = vm.runInNewContext("gc") as () => void;
    for (let i = 0; i < 100; i++) {
      const undisposed = new AspectRemapTable();
      undisposed.set("0x10", "0x20");
      expect(undisposed.get("0x10")).toBe("0x20");
    }
    for (let i = 0; i < 5; i++) {
      gc();
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    table.set("0x10", "0x20");
    expect(table.get("0x10")).toBe("0x20");
  });

  it("is released when its IModelCloneContext is disposed", () => {
    const outputDir = path.join(
      KnownTestLocations.outputDir,
      "AspectRemapTable"
    );
    IModelJsFs.recursiveMkDirSync(outputDir);
    const dbPath = path.join(outputDir, "Context.bim");
    if (IModelJsFs.existsSync(dbPath)) IModelJsFs.removeSync(dbPath);
    const db = SnapshotDb.createEmpty(dbPath, {
      rootSubject: { name: "AspectRemapTable" },
    });
    try {
      const context = new IModelCloneContext(db);
      context.remapElementAspect("0x10", "0x20");
      expect(context.findTargetAspectId("0x10")).toBe("0x20");
      context.removeElementAspect("0x10");
      expect(context.findTargetAspectId("0x10")).toBe(Id64.invalid);
      context.remapElementAspect("0x10", "0x30");
      context[Symbol.dispose]();
      expect(context.findTargetAspectId("0x10")).toBe(Id64.invalid);
    } finally {
      db.close();
    }
  });
});
