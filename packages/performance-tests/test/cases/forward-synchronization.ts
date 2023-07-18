import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import { BriefcaseDb, IModelDb, RepositoryLink, StandaloneDb } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { initOutputFile } from "../TestUtils";
import { setToStandalone } from "../iModelUtils";
import { Logger, StopWatch } from "@itwin/core-bentley";

const loggerCategory = "Transformer Performance Tests Prepare Fork";
const outputDir = path.join(__dirname, ".output");


export default async function forwardSynchronization(sourceDb: BriefcaseDb, addReport: (...smallReportSubset: [testName: string, iModelName: string, valDescription: string, value: number]) => void) {

  // Created the branchDb the same was as in prepare fork, don't know if this is correct
  const branchPath = initOutputFile(`ForwardSync-${sourceDb.name}-branch.bim`, outputDir);
  const filePath = sourceDb.pathName;
  fs.copyFileSync(filePath, branchPath);
  setToStandalone(branchPath);
  const branchDb = StandaloneDb.openFile(branchPath);

  let processChangesTimer: StopWatch | undefined;
  try {
    const masterExternalSourceId = branchDb.elements.queryElementIdByCode(
      RepositoryLink.createCode(sourceDb, IModelDb.repositoryModelId, "example-code-value"),
    );
    const synchronizer = new IModelTransformer(sourceDb, branchDb, {
      // read the synchronization provenance in the scope of our representation of the external source, master
      targetScopeElementId: masterExternalSourceId,
    });
    await synchronizer.processChanges(myAccessToken); // Don't know what to use here
    synchronizer.dispose();
    // save and push
    const description = "updated branch with recent master changes";
    branchDb.saveChanges(description);
  } catch (err: any) {
    Logger.logInfo(loggerCategory, `An error was encountered: ${err.message}`);
    const schemaDumpDir = fs.mkdtempSync(path.join(os.tmpdir(), "identity-test-schemas-dump-"));
    sourceDb.nativeDb.exportSchemas(schemaDumpDir);
    Logger.logInfo(loggerCategory, `dumped schemas to: ${schemaDumpDir}`);
  } finally {
    addReport(
      "Forward Synchronization",
      sourceDb.name,
      "time elapsed (seconds)",
      processChangesTimer?.elapsedSeconds ?? -1,
    );
  }

  branchDb.close();
}
