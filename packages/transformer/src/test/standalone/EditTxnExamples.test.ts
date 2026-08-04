/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn, SnapshotDb } from "@itwin/core-backend";
import { IModelImporter } from "../../IModelImporter";
import { IModelTransformer } from "../../IModelTransformer";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";

describe("EditTxn documentation examples", () => {
  function createDb(name: string): SnapshotDb {
    return SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "EditTxnExamples",
        `${name}.bim`
      ),
      { rootSubject: { name } }
    );
  }

  it("uses a caller-owned EditTxn", async () => {
    const sourceDb = createDb("DirectSource");
    const targetDb = createDb("DirectTarget");
    try {
      // __PUBLISH_EXTRACT_START__ EditTxnInTransformer.direct-transformer
      const editTxn = new EditTxn(targetDb, "transform source");
      editTxn.start();
      const transformer = new IModelTransformer({
        source: sourceDb,
        target: editTxn,
      });
      await transformer.process();
      transformer.dispose();
      // The caller that starts the transaction must also end it.
      editTxn.end();
      // __PUBLISH_EXTRACT_END__
    } finally {
      sourceDb.close();
      targetDb.close();
    }
  });

  it("constructs a transformer with a custom importer", () => {
    const sourceDb = createDb("ImporterSource");
    const targetDb = createDb("ImporterTarget");
    try {
      const editTxn = new EditTxn(targetDb, "transform source");
      editTxn.start();
      // __PUBLISH_EXTRACT_START__ EditTxnInTransformer.transformer-with-custom-importer
      const importer = new IModelImporter(editTxn, {
        autoExtendProjectExtents: false,
      });
      const transformer = new IModelTransformer({
        source: sourceDb,
        target: importer,
      });
      // __PUBLISH_EXTRACT_END__
      transformer.dispose();
      editTxn.end("abandon");
    } finally {
      sourceDb.close();
      targetDb.close();
    }
  });

  it("ends the transaction according to the processing outcome", async () => {
    const sourceDb = createDb("RollbackSource");
    const targetDb = createDb("RollbackTarget");
    try {
      // __PUBLISH_EXTRACT_START__ EditTxnInTransformer.rollback-on-failure
      const editTxn = new EditTxn(targetDb, "transform source");
      editTxn.start();
      const transformer = new IModelTransformer({
        source: sourceDb,
        target: editTxn,
      });
      let processSucceeded = false;
      try {
        await transformer.process();
        processSucceeded = true;
      } finally {
        transformer.dispose();
        editTxn.end(processSucceeded ? "save" : "abandon");
      }
      // __PUBLISH_EXTRACT_END__
    } finally {
      sourceDb.close();
      targetDb.close();
    }
  });
});
