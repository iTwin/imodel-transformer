import * as path from "path";
import * as fs from "fs";
import { BriefcaseManager, BriefcaseDb, IModelHost, RepositoryLink, IModelDb, ExternalSource, ExternalSourceIsInRepository } from "@itwin/core-backend";
import { Code } from "@itwin/core-common";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { TestIModel } from "./TestContext";
import { Reporter } from "@itwin/perf-tools";
import { initOutputFile, timed } from "./TestUtils";
import { Logger, StopWatch } from "@itwin/core-bentley";

const loggerCategory = "Transformer Performance Tests Prepare Fork";
const outputDir = path.join(__dirname, ".output");

export default async function prepareFork(iModel: TestIModel, os: any, reporter: Reporter){
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
  
  // create an external source and owning repository link to use as our *Target Scope Element* for future synchronizations
  // const masterLinkRepoId = new RepositoryLink({
  //   classFullName: RepositoryLink.classFullName,
  //   code: RepositoryLink.createCode(branchDb, IModelDb.repositoryModelId, "example-code-value"),
  //   model: IModelDb.repositoryModelId,
  //   url: "https://wherever-you-got-your-imodel.net",
  //   format: "iModel",
  //   repositoryGuid: masterDb.iModelId,
  //   description: "master iModel repository",
  // }, branchDb).insert();
  
  const masterExternalSourceId = new ExternalSource({
    classFullName: ExternalSource.classFullName,
    model: IModelDb.rootSubjectId,
    code: Code.createEmpty(),
    // repository: new ExternalSourceIsInRepository(masterLinkRepoId),
    connectorName: "iModel Transformer",
    connectorVersion: require("@itwin/imodel-transformer/package.json").version,
  }, branchDb).insert();
  
  // initialize the branch provenance
  const branchInitializer = new IModelTransformer(masterDb, branchDb, {
    // tells the transformer that we have a raw copy of a source and the target should receive
    // provenance from the source that is necessary for performing synchronizations in the future
    wasSourceIModelCopiedToTarget: true,
    // store the synchronization provenance in the scope of our representation of the external source, master
    targetScopeElementId: masterExternalSourceId,
  });
  let entityProcessingTimer: StopWatch | undefined;
  try {
    [entityProcessingTimer] = await timed(async () => {
    await branchInitializer.processAll();
    });
    Logger.logInfo(loggerCategory, `entity processing time: ${entityProcessingTimer.elapsedSeconds}`);
  } catch (err: any) {
    Logger.logInfo(loggerCategory, `An error was encountered: ${err.message}`);
    const schemaDumpDir = fs.mkdtempSync(path.join(os.tmpdir(), "identity-test-schemas-dump-"));
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
  
    // save+push our changes to whatever hub we're using
    const description = "initialized branch iModel";
    branchDb.saveChanges(description);
    // await branchDb.pushChanges({
    //   accessToken: await IModelHost.authorizationClient!.getAccessToken(),
    //   description,
    // });
    branchDb.close();
    masterDb.close();
    branchInitializer.dispose();
  }
};
