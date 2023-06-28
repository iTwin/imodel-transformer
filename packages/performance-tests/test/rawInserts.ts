import { Reporter, ReporterEntry } from "./ReporterUtils";
import { initOutputFile, timed } from "./TestUtils";
import { ElementGroupsMembers, IModelDb, PhysicalModel, PhysicalObject, SnapshotDb, SpatialCategory, StandaloneDb } from "@itwin/core-backend";
import { IModelTransformerTestUtils } from "@itwin/imodel-transformer/lib/cjs/test/IModelTransformerUtils";
import path from "path";
import fs from "fs";
import assert from "assert";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { ChangesetFileProps, Code } from "@itwin/core-common";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { Logger, OpenMode } from "@itwin/core-bentley";
import { generateTestIModel, setToStandalone } from "./iModelUtils";

const loggerCategory = "Raw Inserts";
const outputDir = path.join(__dirname, ".output");

const iModelName = "Many PhysicalObjects and Relationships";

const ELEM_COUNT = 100_000;
assert(ELEM_COUNT % 2 === 0, "elem count must be divisible by 2");

export default async function rawInserts(reporter: Reporter, branchName: string) {

  Logger.logInfo(loggerCategory, "starting 150k entity inserts");

  let sourceDb: StandaloneDb | undefined;
  const [insertsTimer] = timed(() => {
    sourceDb = generateTestIModel({ numElements: 100_000, fedGuids: true, fileName:`RawInserts-source.bim` });
  });

  if (sourceDb === undefined) throw Error

  let reportEntry: ReporterEntry = {
    iModelName: iModelName,
    branch: branchName,
  };

  reportEntry.testName = "populate by insert";
  reportEntry.value = insertsTimer?.elapsedSeconds ?? -1;
  reportEntry.info = {
    elementCount: IModelTransformerTestUtils.count(sourceDb, "Bis.ElementGroupsMembers"),
    relationshipCount: IModelTransformerTestUtils.count(sourceDb, "Bis.Element"),
    branchName: reportEntry.branch,
  }
  reporter.addEntry(reportEntry);

  sourceDb.saveChanges();

  Logger.logInfo(loggerCategory, "Done. Starting changeset application of same content");

  const changeset1 = createChangeset(sourceDb);
  const changesetDbPath = initOutputFile(`RawInsertsApply.bim`, outputDir);
  if (fs.existsSync(changesetDbPath))
    fs.unlinkSync(changesetDbPath);
  const changesetDb = StandaloneDb.createEmpty(changesetDbPath, { rootSubject: { name: "RawInsertsApply" }});

  const [applyChangeSetTimer] = timed(() => {
    changesetDb.nativeDb.applyChangeset(changeset1);
  });

  reportEntry.testName = "populate by applying changeset";
  reportEntry.value = applyChangeSetTimer?.elapsedSeconds ?? -1;
  reportEntry.info = {
    elementCount: IModelTransformerTestUtils.count(changesetDb, "Bis.ElementGroupsMembers"),
    relationshipCount: IModelTransformerTestUtils.count(changesetDb, "Bis.Element"),
    branchName: reportEntry.branch,
  }
  reporter.addEntry(reportEntry);

  Logger.logInfo(loggerCategory, "Done. Starting with-provenance transformation of same content");

  const targetPath = initOutputFile(`RawInserts-Target.bim`, outputDir);
  const targetDb = SnapshotDb.createEmpty(targetPath, { rootSubject: { name: "RawInsertsTarget" }});
  const transformerWithProv = new IModelTransformer(sourceDb, targetDb, { noProvenance: false });

  const [transformWithProvTimer] = await timed(async () => {
    await transformerWithProv.processAll();
  });

  reportEntry.testName = "populate by transform (adding provenance)";
  reportEntry.value = transformWithProvTimer?.elapsedSeconds ?? -1;
  reportEntry.info = {
    elementCount: IModelTransformerTestUtils.count(targetDb, "Bis.ElementGroupsMembers"),
    relationshipCount: IModelTransformerTestUtils.count(targetDb, "Bis.Element"),
    branchName: reportEntry.branch,
  }
  reporter.addEntry(reportEntry);

  Logger.logInfo(loggerCategory, "Done. Starting without-provenance transformation of same content");

  const targetNoProvPath = initOutputFile(`RawInserts-TargetNoProv.bim`, outputDir);
  const targetNoProvDb = SnapshotDb.createEmpty(targetNoProvPath, { rootSubject: { name: "RawInsertsTarget" }});
  const transformerNoProv = new IModelTransformer(sourceDb, targetNoProvDb, { noProvenance: true });

  const [transformNoProvTimer] = await timed(async () => {
    await transformerNoProv.processAll();
  });

  reportEntry.testName = "populate by transform";
  reportEntry.value = transformNoProvTimer?.elapsedSeconds ?? -1;
  reportEntry.info = {
    elementCount: IModelTransformerTestUtils.count(targetNoProvDb, "Bis.ElementGroupsMembers"),
    relationshipCount: IModelTransformerTestUtils.count(targetNoProvDb, "Bis.Element"),
    branchName: reportEntry.branch,
  }
  reporter.addEntry(reportEntry);

  sourceDb.close();
  changesetDb.close();
  targetDb.close();
  targetNoProvDb.close();
}

// stolen from itwinjs-core: core/backend/src/test/changesets/ChangeMerging.test.ts
function createChangeset(imodel: IModelDb): ChangesetFileProps {
  const changeset = imodel.nativeDb.startCreateChangeset();

  // completeCreateChangeset deletes the file that startCreateChangeSet created.
  // We make a copy of it now, before it does that.
  const csFileName = path.join(outputDir, `${changeset.id}.changeset`);
  fs.copyFileSync(changeset.pathname, csFileName);
  changeset.pathname = csFileName;

  imodel.nativeDb.completeCreateChangeset({ index: 0 });
  return changeset as any; // FIXME: bad peer deps
}

