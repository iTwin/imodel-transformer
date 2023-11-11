/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { CompactRemapTable } from "../../CompactRemapTable";
import { expect } from "chai";

describe("CompactRemapTable", () => {
  const _immutableBaseTable = new CompactRemapTable();
  _immutableBaseTable.remap(3, 100);
  _immutableBaseTable.remap(5, 100);
  _immutableBaseTable.remap(6, 100);
  for (let i = 7; i < 7 + 50; ++i) {
    _immutableBaseTable.remap(i, i + 500);
  }
  // eslint-disable-next-line @typescript-eslint/dot-notation
  Object.freeze(_immutableBaseTable["_array"]);

  let table!: CompactRemapTable;

  beforeEach(() => {
    table = _immutableBaseTable.clone();
  });

  it.only("init", async () => {
    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 1,
      5, 100, 1,
      6, 100, 1,
      7, 500, 100,
    ]);
  });

  it("merges adjacent following run", () => {
    table.remap(5, 99);
    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 1,
      5, 99, 2,
      7, 500, 100,
    ]);
  });

  it("merges filled gap", () => {
    table.remap(4, 99);
    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 3,
      6, 100, 1,
      7, 500, 100,
    ]);
  });

  it("merges altered gaps", () => {
    table.remap(4, 1000);
    table.remap(4, 99);
    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 3,
      6, 100, 1,
      7, 500, 100,
    ]);
  });


  it("appends to adjacent run", () => {
    table.remap(107, 607);
    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 1,
      5, 100, 1,
      6, 100, 1,
      7, 500, 101,
    ]);
  });

  it("splits run", () => {
    table.remap(27, 107);

    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 1,
      5, 100, 1,
      6, 100, 1,
      7, 500, 19,
      27, 107, 1,
      58, 551, 80,
    ]);
  });

  it("shortens run when remapping end", () => {
    table.remap(107, 200);

    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 1,
      5, 100, 1,
      6, 100, 1,
      7, 500, 99,
      107, 200, 1,
    ]);
  });

  it("pushes up run when remapping beginning", () => {
    table.remap(7, 200);

    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 1,
      5, 100, 1,
      6, 100, 1,
      7, 200, 1,
      8, 501, 99,
    ]);
  });

  it("add before anything", () => {
    table.remap(0, 0);

    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      0, 0, 1,
      3, 98, 1,
      5, 100, 1,
      6, 100, 1,
      7, 500, 100,
    ]);
  });

  it("add after everything", () => {
    table.remap(1000, 500);

    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      0, 0, 1,
      3, 98, 1,
      5, 100, 1,
      6, 100, 1,
      7, 500, 100,
      1000, 500, 1,
    ]);
  });
});
