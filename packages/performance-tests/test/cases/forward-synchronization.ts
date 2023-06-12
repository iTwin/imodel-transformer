import * as path from "path";
import * as fs from "fs";
import { RepositoryLink, IModelDb, BriefcaseDb, BriefcaseManager, IModelHost } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { Reporter } from "@itwin/perf-tools";
import { TestIModel } from "../TestContext";
import { Logger, StopWatch } from "@itwin/core-bentley";
import { timed } from "../TestUtils";

const loggerCategory = "Transformer Performance Tests Forward Sync";
const outputDir = path.join(__dirname, ".output");

export default async function forwardSynchonization(iModel: TestIModel, os: any, reporter: Reporter){
  // download and open master
  Logger.logInfo(loggerCategory, `processing iModel '${iModel.name}' of size '${iModel.tShirtSize.toUpperCase()}'`);
  const masterDb = await iModel.load();
  const toGb = (bytes: number) => `${(bytes / 1024 **3).toFixed(2)}Gb`;
  const sizeInGb = toGb(fs.statSync(masterDb.pathName).size);
  Logger.logInfo(loggerCategory, `loaded (${sizeInGb})'`);

  // create a duplicate of master as a good starting point for our branch
  const branchIModelId = await IModelHost.hubAccess.createNewIModel({
    iTwinId: iModel.iTwinId,
    iModelName: "my-branch-imodel",
    version0: masterDb.pathName,
    noLocks: true, // you may prefer locks for your application
  });
  
  // download and open the new branch
  const branchDbProps = await BriefcaseManager.downloadBriefcase({
    accessToken: await IModelHost.authorizationClient!.getAccessToken(),
    iTwinId: iModel.iTwinId,
    iModelId: branchIModelId,
  });
  const branchDb = await BriefcaseDb.open({ fileName: branchDbProps.fileName });

  const masterExternalSourceId = branchDb.elements.queryElementIdByCode(
      RepositoryLink.createCode(masterDb, IModelDb.repositoryModelId, "example-code-value"),
  );
  const synchronizer = new IModelTransformer(masterDb, branchDb, {
      // read the synchronization provenance in the scope of our representation of the external source, master
      targetScopeElementId: masterExternalSourceId,
  });
  let entityProcessingTimer: StopWatch | undefined;
  try {
    [entityProcessingTimer] = await timed(async () => {
      await synchronizer.processChanges(await IModelHost.authorizationClient!.getAccessToken());
    });
    Logger.logInfo(loggerCategory, `time to process changes: ${entityProcessingTimer.elapsedSeconds}`);
  } catch (err: any) {
    Logger.logInfo(loggerCategory, `An error was encountered: ${err.message}`);
    const schemaDumpDir = fs.mkdtempSync(path.join(os.tmpdir(), "forward-sync-test-schemas-dump-"));
    masterDb.nativeDb.exportSchemas(schemaDumpDir);
    Logger.logInfo(loggerCategory, `dumped schemas to: ${schemaDumpDir}`);
  } finally {
    const record = {
      /* eslint-disable @typescript-eslint/naming-convention */
      "Id": iModel.iModelId,
      "T-shirt size": iModel.tShirtSize,
      "Gb size": sizeInGb,
      /* eslint-enable @typescript-eslint/naming-convention */
    };
    reporter.addEntry("Prepare Fork Regression Tests", iModel.name, "time", entityProcessingTimer?.elapsedSeconds ?? -1, record);
  
    masterDb.close();
    branchDb.close();
    synchronizer.dispose();
    // save and push
    const description = "updated branch with recent master changes";
    branchDb.saveChanges(description);
    // await branchDb.pushChanges({
    //     accessToken: await IModelHost.authorizationClient!.getAccessToken(),
    //     description,
    // });
  }
};