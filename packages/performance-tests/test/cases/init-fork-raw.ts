/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/
/*
 * Tests where we perform "identity" transforms, that is just rebuilding an entire identical iModel (minus IDs)
 * through the transformation process.
 */
import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import { Element, Relationship, SnapshotDb } from "@itwin/core-backend";
import { Logger, StopWatch } from "@itwin/core-bentley";
import { IModelTransformer, initializeBranchProvenance } from "@itwin/imodel-transformer";
import { TestIModel } from "../TestContext";
import { Reporter } from "@itwin/perf-tools";
import { initOutputFile, timed } from "../TestUtils";

const loggerCategory = "Transformer Performance Tests Identity";
const outputDir = path.join(__dirname, ".output");

export default async function initForkRaw(iModel: TestIModel, reporter: Reporter, branchName: string) {
  const targetPath = initOutputFile(`${iModel.iTwinId}-${iModel.name}-target.bim`, outputDir);
  const targetDb = SnapshotDb.createEmpty(targetPath, {rootSubject: {name: iModel.name}});

  const [branchProvenanceInitTimer] = await timed(async () => {
    await initializeBranchProvenance({
      master: iModel,
      branch: targetDb,
    });
  });
  const record = {
    /* eslint-disable @typescript-eslint/naming-convention */
    "Id": iModel.iModelId,
    "T-shirt size": iModel.tShirtSize,
    "Branch Name": branchName,
    /* eslint-enable @typescript-eslint/naming-convention */
  };

  reporter.addEntry(
    "identity transform (provenance)",
    `${branchName}: ${iModel.name}`,
    "time elapsed (seconds)",
    branchProvenanceInitTimer?.elapsedSeconds ?? -1,
    record
  );

  targetDb.close();
  return reporter;
}
