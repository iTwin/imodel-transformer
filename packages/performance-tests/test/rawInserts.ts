import { Reporter } from "@itwin/perf-tools";
import { initOutputFile, timed } from "./TestUtils";
import { ElementGroupsMembers, IModelDb, IModelHost, PhysicalModel, PhysicalObject, SnapshotDb, SpatialCategory, StandaloneDb } from "@itwin/core-backend";
import { IModelTransformerTestUtils } from "@itwin/imodel-transformer/lib/cjs/test/IModelTransformerUtils";
import path from "path";
import fs from "fs";
import assert from "assert";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { BriefcaseIdValue, ChangesetFileProps, Code } from "@itwin/core-common";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { Guid, Logger, OpenMode } from "@itwin/core-bentley";

const loggerCategory = "Raw Inserts";
const outputDir = path.join(__dirname, ".output");

const ELEM_COUNT = 1000; // 50k
assert(ELEM_COUNT % 2 === 0, "elem count must be divisible by 2");

export default async function rawInserts(reporter: Reporter) {
  const sourcePath = initOutputFile(`RawInserts-source.bim`, outputDir);
  if (fs.existsSync(sourcePath))
    fs.unlinkSync(sourcePath);

  let sourceDb = StandaloneDb.createEmpty(sourcePath, { rootSubject: { name: "RawInsertsSource" }});
  const pathName = sourceDb.pathName;
  sourceDb.close();
  setToStandalone(pathName);
  sourceDb = StandaloneDb.openFile(sourcePath, OpenMode.ReadWrite);

  Logger.logInfo(loggerCategory, "starting 150k entity inserts");

  const [insertsTimer] = timed(() => {
    const physModelId = PhysicalModel.insert(sourceDb, IModelDb.rootSubjectId, "physical model");
    const categoryId = SpatialCategory.insert(sourceDb, IModelDb.dictionaryId, "spatial category", {});

    // 100,000 elements, 50,000  relationships
    for (let i = 0; i < ELEM_COUNT / 2; ++i) {
      const [id1, id2] = [0, 1].map((n) => new PhysicalObject({
        classFullName: PhysicalObject.classFullName,
        category: categoryId,
        geom: IModelTransformerTestUtils.createBox(Point3d.create(i, i, i)),
        placement: {
          origin: Point3d.create(i, i, i),
          angles: YawPitchRollAngles.createDegrees(i, i, i),
        },
        model: physModelId,
        code: new Code({ spec: IModelDb.rootSubjectId, scope: IModelDb.rootSubjectId, value: `${2*i + n}`}),
        userLabel: `${2*i + n}`,
      }, sourceDb).insert());

      const rel = new ElementGroupsMembers({
        classFullName: ElementGroupsMembers.classFullName,
        sourceId: id1,
        targetId: id2,
        memberPriority: i,
      }, sourceDb);

      rel.insert();
    }
  });

  reporter.addEntry(
    "Transformer Regression Tests",
    "populate with raw insert calls",
    "time elapsed (seconds)",
    insertsTimer?.elapsedSeconds ?? -1,
    {
      elementCount: IModelTransformerTestUtils.count(sourceDb, "Bis.ElementGroupsMembers"),
      relationshipCount: IModelTransformerTestUtils.count(sourceDb, "Bis.Element"),
    }
  );

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

  reporter.addEntry(
    "Transformer Regression Tests",
    "apply changeset of insert calls",
    "time elapsed (seconds)",
    applyChangeSetTimer?.elapsedSeconds ?? -1,
    {
      elementCount: IModelTransformerTestUtils.count(changesetDb, "Bis.ElementGroupsMembers"),
      relationshipCount: IModelTransformerTestUtils.count(changesetDb, "Bis.Element"),
    }
  );

  Logger.logInfo(loggerCategory, "Done. Starting with-provenance transformation of same content");

  const targetPath = initOutputFile(`RawInserts-Target.bim`, outputDir);
  const targetDb = SnapshotDb.createEmpty(targetPath, { rootSubject: { name: "RawInsertsTarget" }});
  const transformerWithProv = new IModelTransformer(sourceDb, targetDb, { noProvenance: false });

  const [transformWithProvTimer] = await timed(async () => {
    await transformerWithProv.processAll();
  });

  reporter.addEntry(
    "Transformer Regression Tests",
    "transform raw insert populated model (with provenance)",
    "time elapsed (seconds)",
    transformWithProvTimer?.elapsedSeconds ?? -1,
    {
      elementCount: IModelTransformerTestUtils.count(targetDb, "Bis.ElementGroupsMembers"),
      relationshipCount: IModelTransformerTestUtils.count(targetDb, "Bis.Element"),
    }
  );

  Logger.logInfo(loggerCategory, "Done. Starting without-provenance transformation of same content");

  const targetNoProvPath = initOutputFile(`RawInserts-TargetNoProv.bim`, outputDir);
  const targetNoProvDb = SnapshotDb.createEmpty(targetNoProvPath, { rootSubject: { name: "RawInsertsTarget" }});
  const transformerNoProv = new IModelTransformer(sourceDb, targetNoProvDb, { noProvenance: true });

  const [transformNoProvTimer] = await timed(async () => {
    await transformerNoProv.processAll();
  });

  reporter.addEntry(
    "Transformer Regression Tests",
    "transform raw insert populated model (no provenance)",
    "time elapsed (seconds)",
    transformNoProvTimer?.elapsedSeconds ?? -1,
    {
      elementCount: IModelTransformerTestUtils.count(targetNoProvDb, "Bis.ElementGroupsMembers"),
      relationshipCount: IModelTransformerTestUtils.count(targetNoProvDb, "Bis.Element"),
    }
  );

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

// TODO: dedup with other packages
// for testing purposes only, based on SetToStandalone.ts, force a snapshot to mimic a standalone iModel
function setToStandalone(iModelName: string) {
  const nativeDb = new IModelHost.platform.DgnDb();
  nativeDb.openIModel(iModelName, OpenMode.ReadWrite);
  nativeDb.setITwinId(Guid.empty); // empty iTwinId means "standalone"
  nativeDb.saveChanges(); // save change to iTwinId
  nativeDb.deleteAllTxns(); // necessary before resetting briefcaseId
  nativeDb.resetBriefcaseId(BriefcaseIdValue.Unassigned); // standalone iModels should always have BriefcaseId unassigned
  nativeDb.saveLocalValue("StandaloneEdit", JSON.stringify({ txns: true }));
  nativeDb.saveChanges(); // save change to briefcaseId
  nativeDb.closeIModel();
}

