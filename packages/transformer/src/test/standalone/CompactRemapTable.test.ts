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

  it("init", async () => {
    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 1,
      5, 100, 1,
      6, 100, 1,
      7, 500, 100,
    ]);
  });

  it("merges adjacent following segment", () => {
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

  it("appends to adjacent segment", () => {
    table.remap(107, 607);
    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 1,
      5, 100, 1,
      6, 100, 1,
      7, 500, 101,
    ]);
  });

  it("splits adjacent following segment", () => {
    table.remap(57, 107);

    // eslint-disable-next-line @typescript-eslint/dot-notation
    expect(table["_array"]).to.equal([
      3, 98, 1,
      5, 100, 1,
      6, 100, 1,
      7, 500, 49,
      57, 107, 1,
      58, 551, 50,
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
