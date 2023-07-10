import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import { BriefcaseDb, ExternalSource, ExternalSourceIsInRepository, IModelDb, RepositoryLink, SnapshotDb, StandaloneDb } from "@itwin/core-backend";
import { Code } from "@itwin/core-common";
import { IModelTransformer, initializeBranchProvenance } from "@itwin/imodel-transformer";
import { initOutputFile, timed } from "../TestUtils";
import { Logger, StopWatch } from "@itwin/core-bentley";
import { setToStandalone } from "../iModelUtils";

const loggerCategory = "Transformer Performance Tests Prepare Fork";
const outputDir = path.join(__dirname, ".output");

export default async function prepareFork(sourceDb: BriefcaseDb, addReport: (...smallReportSubset: [testName: string, iModelName: string, valDescription: string, value: number]) => void){

  // create a duplicate of master for branch
  const branchPath = initOutputFile(`PrepareFork-${sourceDb.name}-target.bim`, outputDir);
  if (fs.existsSync(branchPath))
    fs.unlinkSync(branchPath);
  const filePath = sourceDb.pathName;
  fs.copyFileSync(filePath, branchPath);
  setToStandalone(branchPath);
  const branchDb = StandaloneDb.openFile(branchPath);

  let entityProcessingTimer: StopWatch | undefined;
  try {
    [entityProcessingTimer] = await timed(async () => {
      await classicalTransformerBranchInit(sourceDb, branchDb);
    });
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
  }

  const targetPath1 = initOutputFile(`RawFork-${sourceDb.name}-target.bim`, outputDir);
  if (fs.existsSync(targetPath1))
    fs.unlinkSync(targetPath1);
  fs.copyFileSync(filePath, targetPath1);
  setToStandalone(targetPath1);
  const targetDb1 = StandaloneDb.openFile(branchPath);

  const [branchProvenanceInitTimer] = await timed(async () => {
    await initializeBranchProvenance({
      master: sourceDb,
      branch: targetDb1,
    });
  });

  addReport(
    "Init Fork raw",
    sourceDb.name,
    "time elapsed (seconds)",
    branchProvenanceInitTimer?.elapsedSeconds ?? -1,
  );

  const sourceCopy = initOutputFile(`RawFork-${sourceDb.name}-target.bim`, outputDir);
  if (fs.existsSync(sourceCopy))
    fs.unlinkSync(sourceCopy);
  fs.copyFileSync(filePath, sourceCopy);
  setToStandalone(sourceCopy);
  const sourceCopyDb = StandaloneDb.openFile(sourceCopy);

  const targetPath2 = initOutputFile(`RawFork-${sourceDb.name}-copy.bim`, outputDir);
  if (fs.existsSync(targetPath2))
    fs.unlinkSync(targetPath2);
  fs.copyFileSync(filePath, targetPath2);
  setToStandalone(targetPath2);
  const targetDb2 = StandaloneDb.openFile(targetPath2);

  const [createFedGuidsForMasterTimer] = await timed(async () => {
    await initializeBranchProvenance({
      master: sourceCopyDb,
      branch: targetDb2,
      createFedGuidsForMaster: true,
    });
  });

  addReport(
    "Init Fork raw createFedGuidsForMaster",
    sourceDb.name,
    "time elapsed (seconds)",
    createFedGuidsForMasterTimer?.elapsedSeconds ?? -1,
  );
}

async function classicalTransformerBranchInit(sourceDb: BriefcaseDb, branchDb: StandaloneDb,) {
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
  branchInitializer.dispose();
}
