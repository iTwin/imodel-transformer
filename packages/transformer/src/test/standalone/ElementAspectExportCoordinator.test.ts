/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import { ElementAspectExportCoordinator } from "../../ElementAspectExportCoordinator";

describe("ElementAspectExportCoordinator", () => {
  it("rethrows the export error when completing the failed group also throws", async () => {
    const completions: boolean[] = [];
    const coordinator = new ElementAspectExportCoordinator(
      10,
      () => new Set(),
      async () => {
        throw new Error("aspect export failed");
      }
    );
    coordinator.setPreparation(async () => async (exported) => {
      completions.push(exported);
      throw new Error("completion failed");
    });

    await expect(coordinator.exportOwners(new Set(["0x1"]))).rejects.toThrow(
      "aspect export failed"
    );
    expect(completions).to.deep.equal([false]);
  });
});
