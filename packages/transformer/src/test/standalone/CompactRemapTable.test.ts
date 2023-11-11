/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

/* eslint-disable @typescript-eslint/dot-notation */

import { CompactRemapTable } from "../../CompactRemapTable";
import { expect } from "chai";

describe("CompactRemapTable", () => {
  const _immutableBaseTable = new CompactRemapTable();
  _immutableBaseTable.remap(3, 98);
  _immutableBaseTable.remap(5, 100);
  _immutableBaseTable.remap(6, 100);
  for (let i = 7; i < 7 + 100; ++i) {
    _immutableBaseTable.remap(i, i + 493);
  }

  Object.freeze(_immutableBaseTable["_lengths"]);
  Object.freeze(_immutableBaseTable["_tos"]);
  Object.freeze(_immutableBaseTable["_froms"]);

  let table!: CompactRemapTable;

  beforeEach(() => {
    table = _immutableBaseTable.clone();
  });

  it("init", async () => {
    expect(table["_froms"]).to.deep.equal([3, 5, 6, 7]);
    expect(table["_tos"]).to.deep.equal([98, 100, 100, 500]);
    expect(table["_lengths"]).to.deep.equal([1, 1, 1, 100]);
  });

  it.only("merges adjacent following run", () => {
    table.remap(5, 99);
    expect(table["_froms"]).to.deep.equal([3, 5, 7]);
    expect(table["_tos"]).to.deep.equal([98, 99, 500]);
    expect(table["_lengths"]).to.deep.equal([1, 2, 100]);
  });

  it("merges filled gap", () => {
    table.remap(4, 99);
    expect(table["_froms"]).to.deep.equal([3, 6, 7]);
    expect(table["_tos"]).to.deep.equal([98, 100, 500]);
    expect(table["_lengths"]).to.deep.equal([3, 1, 100]);
  });

  it("merges altered gaps", () => {
    table.remap(4, 1000);
    table.remap(4, 99);
    expect(table["_froms"]).to.deep.equal([3, 6, 7]);
    expect(table["_tos"]).to.deep.equal([98, 100, 500]);
    expect(table["_lengths"]).to.deep.equal([3, 1, 100]);
  });

  it("appends to adjacent run", () => {
    table.remap(107, 607);
    expect(table["_froms"]).to.deep.equal([3, 5, 6, 7]);
    expect(table["_tos"]).to.deep.equal([98, 100, 100, 500]);
    expect(table["_lengths"]).to.deep.equal([1, 1, 1, 100]);
  });

  it("splits run", () => {
    table.remap(27, 107);
    expect(table["_froms"]).to.deep.equal([3, 5, 6, 7, 27, 58]);
    expect(table["_tos"]).to.deep.equal([98, 100, 100, 500, 107, 551]);
    expect(table["_lengths"]).to.deep.equal([1, 1, 1, 19, 1, 80]);
  });

  it("shortens run when remapping end", () => {
    table.remap(107, 200);
    expect(table["_froms"]).to.deep.equal([3, 5, 6, 7, 107]);
    expect(table["_tos"]).to.deep.equal([98, 100, 100, 500, 200]);
    expect(table["_lengths"]).to.deep.equal([1, 1, 1, 99, 1]);
  });

  it("pushes up run when remapping beginning", () => {
    table.remap(7, 200);
    expect(table["_froms"]).to.deep.equal([3, 5, 6, 7, 8]);
    expect(table["_tos"]).to.deep.equal([98, 100, 100, 200, 501]);
    expect(table["_lengths"]).to.deep.equal([1, 1, 1, 1, 99]);
  });

  it("add before anything", () => {
    table.remap(0, 0);
    expect(table["_froms"]).to.deep.equal([0, 3, 5, 6, 7]);
    expect(table["_tos"]).to.deep.equal([0, 98, 100, 100, 500]);
    expect(table["_lengths"]).to.deep.equal([1, 1, 1, 1, 100]);
  });

  it("add after everything", () => {
    table.remap(1000, 500);
    expect(table["_froms"]).to.deep.equal([3, 5, 6, 7, 1000]);
    expect(table["_tos"]).to.deep.equal([98, 100, 100, 500, 500]);
    expect(table["_lengths"]).to.deep.equal([1, 1, 1, 100, 1]);
  });
});
