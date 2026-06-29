/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/* eslint-disable @itwin/no-internal */

import { expect } from "chai";
import * as path from "path";
import "./TransformerTestStartup"; // calls startup/shutdown IModelHost before/after all tests
import {
  BriefcaseDb,
  BriefcaseManager,
  IModelJsFs,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { HubMock } from "@itwin/core-backend/lib/cjs/internal/HubMock";
import { IModel } from "@itwin/core-common";
import { GuidString, Logger } from "@itwin/core-bentley";
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
  let iTwinId: GuidString;

  before(() => {
    HubMock.startup("QueryProjectExtents", KnownTestLocations.outputDir);
    iTwinId = HubMock.iTwinId;
    IModelJsFs.recursiveMkDirSync(outputDir);
  });

  after(() => {
    HubMock.shutdown();
  });

  /**
   * Helper to create an iModel with no geometric elements
   */
  async function createEmptyIModel(name: string): Promise<BriefcaseDb> {
    const iModelId = await HubMock.createNewIModel({
      iModelName: name,
      iTwinId,
    });
    const briefcaseProps = await BriefcaseManager.downloadBriefcase({
      accessToken: "test token",
      iTwinId,
      iModelId,
    });
    const iModelDb = await BriefcaseDb.open({
      fileName: briefcaseProps.fileName,
    });
    return iModelDb;
  }

  it("should use IModelImporter.computeProjectExtents and verify the issue", async () => {
    const iModelDb = await createEmptyIModel("ImporterExtentsTest");

    try {
      // Acquire locks and insert a subject (non-geometric element)
      await iModelDb.locks.acquireLocks({
        shared: [IModel.repositoryModelId],
        exclusive: [IModel.repositoryModelId],
      });
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
      const importer = new IModelImporter(iModelDb, editTxn, {
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
