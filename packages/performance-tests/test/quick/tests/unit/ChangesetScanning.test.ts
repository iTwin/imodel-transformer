/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { ChangedInstanceOps } from "@itwin/imodel-transformer";
import { describe, expect, it } from "vitest";
import { scanDigest } from "../../src/scenarios/changesetScanning.js";

function changedOps(ids: {
  delete?: string[];
  insert?: string[];
  update?: string[];
}): ChangedInstanceOps {
  const operations = new ChangedInstanceOps();
  ids.delete?.forEach((id) => operations.deleteIds.add(id));
  ids.insert?.forEach((id) => operations.insertIds.add(id));
  ids.update?.forEach((id) => operations.updateIds.add(id));
  return operations;
}

describe("changeset scanning", () => {
  it("digests the common cross-version scan result surface", () => {
    const oldResult = {
      aspect: changedOps({ insert: ["0x3"] }),
      codeSpec: changedOps({}),
      element: changedOps({ update: ["0x2", "0x1"] }),
      font: changedOps({}),
      model: changedOps({}),
      relationship: changedOps({ delete: ["0x4"] }),
    };
    const digest = scanDigest(oldResult);
    const newResult = {
      ...oldResult,
      aspectOwnerElementIds: new Set(["0x5"]),
    };

    expect(scanDigest(newResult)).to.equal(digest);
    expect(
      scanDigest({
        ...oldResult,
        element: changedOps({ update: ["0x1"] }),
      })
    ).not.to.equal(digest);
  });
});
