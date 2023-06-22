import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import { BriefcaseDb, ExternalSource, ExternalSourceIsInRepository, IModelDb, RepositoryLink, StandaloneDb } from "@itwin/core-backend";
import { BriefcaseIdValue, Code } from "@itwin/core-common";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { initOutputFile, timed } from "../TestUtils";
import { Logger, StopWatch } from "@itwin/core-bentley";
import { setToStandalone } from "../iModelUtils";
import { BriefcaseArgs, ReporterEntry } from "../TransformerRegression.test";

const loggerCategory = "Transformer Performance Tests Prepare Fork";
const outputDir = path.join(__dirname, ".output");

export default async function prepareFork(sourceDb: BriefcaseDb, sourceBriefcaseArgs: BriefcaseArgs){
  let reporterData: ReporterEntry;

  if(!sourceDb.isOpen)
    sourceDb = await BriefcaseDb.open({
      fileName: sourceBriefcaseArgs.fileName,
      readonly: sourceBriefcaseArgs.briefcaseId ? sourceBriefcaseArgs.briefcaseId === BriefcaseIdValue.Unassigned : false,
    });

  // create a duplicate of master for branch
  const branchPath = initOutputFile(`PrepareFork-branch.bim`, outputDir);
  if (fs.existsSync(branchPath))
    fs.unlinkSync(branchPath);
  const filePath = sourceDb.pathName;
  fs.copyFileSync(filePath, branchPath);
  setToStandalone(branchPath);
  const branchDb = StandaloneDb.openFile(branchPath);

  let entityProcessingTimer: StopWatch | undefined;
  try {
    [entityProcessingTimer] = await timed(async () => {
      // create an external source and owning repository link to use as our *Target Scope Element* for future synchronizations
      const masterLinkRepoId = new RepositoryLink({
        classFullName: RepositoryLink.classFullName,
        code: RepositoryLink.createCode(branchDb, IModelDb.repositoryModelId, "test-imodel"),
        model: IModelDb.repositoryModelId,
        // url: "https://wherever-you-got-your-imodel.net",
        format: "iModel",
        repositoryGuid: sourceDb.iModelId,
        description: "master iModel repository",
      }, branchDb).insert();

      const masterExternalSourceId = new ExternalSource({
        classFullName: ExternalSource.classFullName,
        model: IModelDb.rootSubjectId,
        code: Code.createEmpty(),
        repository: new ExternalSourceIsInRepository(masterLinkRepoId),
        connectorName: "iModel Transformer",
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        connectorVersion: require("@itwin/imodel-transformer/package.json").version,
      }, branchDb).insert();

      // initialize the branch provenance
      const branchInitializer = new IModelTransformer(sourceDb, branchDb, {
        // tells the transformer that we have a raw copy of a source and the target should receive
        // provenance from the source that is necessary for performing synchronizations in the future
        wasSourceIModelCopiedToTarget: true,
        // store the synchronization provenance in the scope of our representation of the external source, master
        targetScopeElementId: masterExternalSourceId,
      });

      await branchInitializer.processAll();
      // save+push our changes to whatever hub we're using
      const description = "initialized branch iModel";
      branchDb.saveChanges(description);

      branchDb.close();
      sourceDb.close();
      branchInitializer.dispose();
    });
    Logger.logInfo(loggerCategory, `Preparefork time: ${entityProcessingTimer.elapsedSeconds}`);

  } catch (err: any) {
    Logger.logInfo(loggerCategory, `An error was encountered: ${err.message}`);
    const schemaDumpDir = fs.mkdtempSync(path.join(os.tmpdir(), "identity-test-schemas-dump-"));
    sourceDb.nativeDb.exportSchemas(schemaDumpDir);
    Logger.logInfo(loggerCategory, `dumped schemas to: ${schemaDumpDir}`);
  } finally {
    reporterData = {
      testSuite: "Prepare Fork",
      testName: sourceDb.name,
      valueDescription: "time elapsed (seconds)",
      value: entityProcessingTimer?.elapsedSeconds ?? -1,
    };
  }

  return reporterData;
}
