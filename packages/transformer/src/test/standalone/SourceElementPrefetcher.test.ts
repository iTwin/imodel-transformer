/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { describe, expect, it } from "vitest";
import { EditTxn, SnapshotDb } from "@itwin/core-backend";
import { IModelTransformer } from "../../IModelTransformer";
import { SourceElementPrefetcher } from "../../SourceElementPrefetcher";
import {
  assertIdentityTransformation,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";
import * as TestUtils from "../TestUtils";

describe("SourceElementPrefetcher", () => {
  it("identity transform with experimentalSourceElementPrefetch produces the same target", async () => {
    const seedDb = SnapshotDb.openFile(
      TestUtils.IModelTestUtils.resolveAssetFile("CompatibilityTestSeed.bim")
    );
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "SourceElementPrefetcher",
      "PrefetchIdentitySource.bim"
    );
    const sourceDb = SnapshotDb.createFrom(seedDb, sourceDbPath);
    seedDb.close();

    const targetDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "SourceElementPrefetcher",
      "PrefetchIdentityTarget.bim"
    );
    const targetDb = SnapshotDb.createEmpty(targetDbPath, {
      rootSubject: sourceDb.rootSubject,
    });

    expect(SourceElementPrefetcher.isSupported(sourceDb)).to.be.true;

    const editTxn = new EditTxn(targetDb, "SourceElementPrefetcher");
    editTxn.start();
    const transformer = new IModelTransformer(
      {
        source: sourceDb,
        target: editTxn,
      },
      { experimentalSourceElementPrefetch: true }
    );
    try {
      await transformer.processSchemas();
      await transformer.process();
      editTxn.saveChanges();

      // the prefetcher must be stopped once processing completes
      expect(transformer.exporter.elementPrefetcher).to.be.undefined;

      await assertIdentityTransformation(sourceDb, targetDb, transformer, {
        compareElemGeom: true,
      });
    } finally {
      editTxn.end();
      transformer.dispose();
      sourceDb.close();
      targetDb.close();
    }
  });
});
