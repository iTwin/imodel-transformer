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
import { BriefcaseDb, Element, Relationship, SnapshotDb } from "@itwin/core-backend";
import { Logger, StopWatch } from "@itwin/core-bentley";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { initOutputFile, timed } from "../TestUtils";
import { ReporterEntry, ReporterInfo } from "../ReporterUtils";
import { Reporter } from "@itwin/perf-tools";

const loggerCategory = "Transformer Performance Tests Identity";
const outputDir = path.join(__dirname, ".output");

export default async function identityTransformer(sourceDb: BriefcaseDb, reporter: Reporter, reportInfo: ReporterInfo) {

  const targetPath = initOutputFile(`identity-${sourceDb.iModelId}-target.bim`, outputDir);
  const targetDb = SnapshotDb.createEmpty(targetPath, {rootSubject: {name: sourceDb.name}});
  let reporterData: ReporterEntry;
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
    reporter.addEntry(
      "identity transform (provenance)",
      `${reportInfo["Branch Name"]}: ${sourceDb.name}`,
      "time elapsed (seconds)",
      entityProcessingTimer?.elapsedSeconds ?? -1,
      reportInfo
    );
    targetDb.close();
    transformer.dispose();
  }
  return reporter;
}
