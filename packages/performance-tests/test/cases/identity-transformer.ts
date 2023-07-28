/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/
/*
 * Tests where we perform "identity" transforms, that is just rebuilding an entire identical iModel (minus IDs)
 * through the transformation process.
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Logger, StopWatch } from "@itwin/core-bentley";
import { SnapshotDb } from "@itwin/core-backend";
import { TestCaseContext } from "./TestCaseContext";
import { initOutputFile, timed } from "../TestUtils";
import assert from "assert";

const loggerCategory = "Transformer Performance Tests Identity";
const outputDir = path.join(__dirname, ".output");

export default async function identityTransformer(context: TestCaseContext) {
  const { sourceDb, transformerModule, addReport } = context;
  const targetPath = initOutputFile(`identity-${sourceDb.iModelId}-target.bim`, outputDir);
  const targetDb = SnapshotDb.createEmpty(targetPath, { rootSubject: { name: sourceDb.name } });
  let schemaProcessingTimer: StopWatch | undefined;
  let entityProcessingTimer: StopWatch | undefined;
  assert(transformerModule.createIdentityTransform, "The createIdentityTransform method does not exist on the module.");
  const transformer = await transformerModule.createIdentityTransform(sourceDb, targetDb);
  try {
    [schemaProcessingTimer] = await timed(async () => {
      await transformer.processSchemas();
    });
    Logger.logInfo(loggerCategory, `schema processing time: ${schemaProcessingTimer.elapsedSeconds}`);
    [entityProcessingTimer] = await timed(async () => {
      await transformer.processAll();
    });
    Logger.logInfo(loggerCategory, `entity processing time: ${entityProcessingTimer.elapsedSeconds}`);
  } catch (err: any) {
    Logger.logInfo(loggerCategory, `An error was encountered: ${err.message}`);
    const schemaDumpDir = fs.mkdtempSync(path.join(os.tmpdir(), "identity-test-schemas-dump-"));
    sourceDb.nativeDb.exportSchemas(schemaDumpDir);
    Logger.logInfo(loggerCategory, `dumped schemas to: ${schemaDumpDir}`);
  } finally {
    addReport(
      "identity transform (provenance)",
      sourceDb.name,
      "time elapsed (seconds)",
      entityProcessingTimer?.elapsedSeconds ?? -1,
    );
    targetDb.close();
    transformer.dispose();
  }
}
