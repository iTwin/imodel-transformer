/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/* eslint-disable @itwin/no-internal */

import { expect } from "vitest";
import * as path from "node:path";
import {
  IModelJsFs,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { IModel } from "@itwin/core-common";
import { Logger } from "@itwin/core-bentley";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";
import { IModelImporter } from "../../IModelImporter";
import { createStartedEditTxn } from "../IModelTransformerUtils";

/**
 * This test reproduces the issue described in:
 * https://github.com/iTwin/itwinjs-core/issues/9176
 *
 * When an iModel has no geometry, computeProjectExtents returns a "null" range
 * with values like [1e+200, 1e+200, 1e+200] for low and [-1e+200, -1e+200, -1e+200] for high.
 * This causes issues when the transformer uses these extents.
 */
describe("computeProjectExtents with no geometry", () => {
  const outputDir = path.join(
    KnownTestLocations.outputDir,
    "QueryProjectExtents"
  );
  beforeAll(() => {
    IModelJsFs.recursiveMkDirSync(outputDir);
  });

  /**
   * Helper to create an iModel with no geometric elements
   */
  function createEmptyIModel(name: string): StandaloneDb {
    const fileName = path.join(outputDir, `${name}.bim`);
    IModelJsFs.removeSync(fileName);
    return StandaloneDb.createEmpty(fileName, {
      rootSubject: { name },
    });
  }

  it("should use IModelImporter.computeProjectExtents and verify the issue", async () => {
    const iModelDb = createEmptyIModel("ImporterExtentsTest");

    try {
      withEditTxn(iModelDb, "Added subject", (txn) => {
        Subject.insert(txn, IModel.rootSubjectId, "TestSubject");
      });

      const originalExtents = iModelDb.projectExtents.clone();
      Logger.logInfo(
        "QueryProjectExtents.test",
        `Original projectExtents=${JSON.stringify(originalExtents)}`
      );

      // Create an importer with autoExtendProjectExtents enabled (default behavior when not excluding outliers)
      const editTxn = createStartedEditTxn(iModelDb);
      const importer = new IModelImporter(editTxn, {
        autoExtendProjectExtents: true,
      });

      // This will compute and update project extents using the same logic as the transformer
      importer.computeProjectExtents();

      const updatedExtents = iModelDb.projectExtents;

      expect(
        !updatedExtents.isNull,
        "Project extents should not become null, but instead keep default extents"
      ).to.be.true;
    } finally {
      iModelDb.close();
    }
  });
});
