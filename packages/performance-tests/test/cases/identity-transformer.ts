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
import { IModelTransformer } from "@itwin/imodel-transformer";
import { TestIModel } from "../TestContext";
import { Reporter } from "@itwin/perf-tools";
import { initOutputFile, timed } from "../TestUtils";

const loggerCategory = "Transformer Performance Tests Identity";
const outputDir = path.join(__dirname, ".output");

export default async function identityTransformer(iModel: TestIModel, reporter: Reporter) {
  Logger.logInfo(loggerCategory, `processing iModel '${iModel.name}' of size '${iModel.tShirtSize.toUpperCase()}'`);
  const sourceDb = await iModel.load();
  const toGb = (bytes: number) => `${(bytes / 1024 **3).toFixed(2)}Gb`;
  const sizeInGb = toGb(fs.statSync(sourceDb.pathName).size);
  Logger.logInfo(loggerCategory, `loaded (${sizeInGb})'`);
  const targetPath = initOutputFile(`${iModel.iTwinId}-${iModel.name}-target.bim`, outputDir);
  const targetDb = SnapshotDb.createEmpty(targetPath, {rootSubject: {name: iModel.name}});
  class ProgressTransformer extends IModelTransformer {
    private _count = 0;
    private _increment() {
      this._count++;
      if (this._count % 1000 === 0)
        Logger.logInfo(loggerCategory, `exported ${this._count} entities`);
    }
    public override onExportElement(sourceElement: Element): void {
      this._increment();
      return super.onExportElement(sourceElement);
    }
    public override onExportRelationship(sourceRelationship: Relationship): void {
      this._increment();
      return super.onExportRelationship(sourceRelationship);
    }
  }
  const transformer = new ProgressTransformer(sourceDb, targetDb);
  let schemaProcessingTimer: StopWatch | undefined;
  let entityProcessingTimer: StopWatch | undefined;
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
    const record = {
      /* eslint-disable @typescript-eslint/naming-convention */
      "Id": iModel.iModelId,
      "T-shirt size": iModel.tShirtSize,
      "Gb size": sizeInGb,
      /* eslint-enable @typescript-eslint/naming-convention */
    };
    reporter.addEntry(
      "identity transform (provenance)",
      iModel.name,
      "time elapsed (seconds)",
      entityProcessingTimer?.elapsedSeconds ?? -1,
      record
    );
    targetDb.close();
    sourceDb.close();
    transformer.dispose();
  }
  return reporter;
}
