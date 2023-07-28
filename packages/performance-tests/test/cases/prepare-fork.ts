/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Logger, StopWatch } from "@itwin/core-bentley";
import { StandaloneDb } from "@itwin/core-backend";
import { TestCaseContext } from "./TestCaseContext";
import { initOutputFile, timed } from "../TestUtils";
import { initializeBranchProvenance } from "@itwin/imodel-transformer";
import { setToStandalone } from "../iModelUtils";
import assert from "assert";

const loggerCategory = "Transformer Performance Tests Prepare Fork";
const outputDir = path.join(__dirname, ".output");

export default async function prepareFork(context: TestCaseContext) {
  const { sourceDb, transformerModule, addReport } = context;
  // create a duplicate of master for branch
  const branchPath = initOutputFile(`PrepareFork-${sourceDb.name}-target.bim`, outputDir);
  const filePath = sourceDb.pathName;
  fs.copyFileSync(filePath, branchPath);
  setToStandalone(branchPath);
  const branchDb = StandaloneDb.openFile(branchPath);

  let entityProcessingTimer: StopWatch | undefined;
  assert(transformerModule.createForkInitTransform, "The createForkInitTransform method does not exist on the module.");
  // initialize the branch provenance
  const branchInitializer = await transformerModule.createForkInitTransform(sourceDb, branchDb);
  try {
    [entityProcessingTimer] = await timed(async () => {
      await branchInitializer.processAll();
    });
    // save+push our changes to whatever hub we're using
    const description = "initialized branch iModel";
    branchDb.saveChanges(description);

    Logger.logInfo(loggerCategory, `Prepare Fork time: ${entityProcessingTimer.elapsedSeconds}`);
  } catch (err: any) {
    Logger.logInfo(loggerCategory, `An error was encountered: ${err.message}`);
    const schemaDumpDir = fs.mkdtempSync(path.join(os.tmpdir(), "identity-test-schemas-dump-"));
    sourceDb.nativeDb.exportSchemas(schemaDumpDir);
    Logger.logInfo(loggerCategory, `dumped schemas to: ${schemaDumpDir}`);
  } finally {
    addReport(
      "Prepare Fork",
      sourceDb.name,
      "time elapsed (seconds)",
      entityProcessingTimer?.elapsedSeconds ?? -1,
    );
    branchDb.close();
    branchInitializer.dispose();
  }

  const noTransformForkPath = initOutputFile(`NoTransform-${sourceDb.name}-target.bim`, outputDir);
  fs.copyFileSync(filePath, noTransformForkPath);
  setToStandalone(noTransformForkPath);
  const noTransformForkDb = StandaloneDb.openFile(noTransformForkPath);

  const [branchProvenanceInitTimer] = await timed(async () => {
    await initializeBranchProvenance({
      master: sourceDb,
      branch: noTransformForkDb,
    });
  });

  addReport(
    "Init Fork no transform",
    sourceDb.name,
    "time elapsed (seconds)",
    branchProvenanceInitTimer?.elapsedSeconds ?? -1,
  );
  noTransformForkDb.close();

  const sourceCopy = initOutputFile(`RawFork-${sourceDb.name}-target.bim`, outputDir);
  fs.copyFileSync(filePath, sourceCopy);
  setToStandalone(sourceCopy);
  const sourceCopyDb = StandaloneDb.openFile(sourceCopy);

  const noTransformAddFedGuidsFork = initOutputFile(`NoTransform-AddFedGuidsFork-copy.bim`, outputDir);
  fs.copyFileSync(filePath, noTransformAddFedGuidsFork);
  setToStandalone(noTransformAddFedGuidsFork);
  const noTransformAddFedGuidsForkDb = StandaloneDb.openFile(noTransformAddFedGuidsFork);

  const [createFedGuidsForMasterTimer] = await timed(async () => {
    await initializeBranchProvenance({
      master: sourceCopyDb,
      branch: noTransformAddFedGuidsForkDb,
      createFedGuidsForMaster: true,
    });
  });

  addReport(
    "Init Fork raw createFedGuidsForMaster",
    sourceDb.name,
    "time elapsed (seconds)",
    createFedGuidsForMasterTimer?.elapsedSeconds ?? -1,
  );
  noTransformAddFedGuidsForkDb.close();
  sourceCopyDb.close();
}
