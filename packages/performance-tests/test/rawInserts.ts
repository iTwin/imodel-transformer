import { Reporter } from "@itwin/perf-tools";
import { initOutputFile, timed } from "./TestUtils";
import { ElementGroupsMembers, IModelDb, PhysicalModel, PhysicalObject, SnapshotDb, SpatialCategory } from "@itwin/core-backend";
import { IModelTransformerTestUtils } from "@itwin/imodel-transformer/lib/cjs/test/IModelTransformerUtils";
import path from "path";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { Code } from "@itwin/core-common";
import { IModelTransformer } from "@itwin/imodel-transformer";

const outputDir = path.join(__dirname, ".output");

export default async function rawInserts(reporter: Reporter) {
  const sourcePath = initOutputFile(`RawInserts-source.bim`, outputDir);
  const sourceDb = SnapshotDb.createEmpty(sourcePath, { rootSubject: { name: "RawInsertsSource" }});

  const [insertsTimer] = timed(() => {
    const physModelId = PhysicalModel.insert(sourceDb, IModelDb.rootSubjectId, "physical model");
    const categoryId = SpatialCategory.insert(sourceDb, IModelDb.dictionaryId, "spatial category", {});

    // 100,000 elements, 50,000  relationships
    for (let i = 0; i < 50_000; ++i) {
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

  const targetPath = initOutputFile(`RawInserts-Target.bim`, outputDir);
  const targetDb = SnapshotDb.createEmpty(targetPath, { rootSubject: { name: "RawInsertsTarget" }});
  const transformerWithProv = new IModelTransformer(sourceDb, targetDb, { noProvenance: false });

  const [transformWithProvTimer] = await timed(async () => {
    await transformerWithProv.processAll();
  });

  reporter.addEntry(
    "Transformer Regression Tests",
    "transform raw insert populated model, with provenance",
    "time elapsed (seconds)",
    transformWithProvTimer?.elapsedSeconds ?? -1,
    {
      elementCount: IModelTransformerTestUtils.count(targetDb, "Bis.ElementGroupsMembers"),
      relationshipCount: IModelTransformerTestUtils.count(targetDb, "Bis.Element"),
    }
  );

  const targetNoProvPath = initOutputFile(`RawInserts-TargetNoProv.bim`, outputDir);
  const targetNoProvDb = SnapshotDb.createEmpty(targetNoProvPath, { rootSubject: { name: "RawInsertsTarget" }});
  const transformerNoProv = new IModelTransformer(sourceDb, targetNoProvDb, { noProvenance: true });

  const [transformNoProvTimer] = await timed(async () => {
    await transformerNoProv.processAll();
  });

  reporter.addEntry(
    "Transformer Regression Tests",
    "transform raw insert populated model, no provenance",
    "time elapsed (seconds)",
    transformNoProvTimer?.elapsedSeconds ?? -1,
    {
      elementCount: IModelTransformerTestUtils.count(targetDb, "Bis.ElementGroupsMembers"),
      relationshipCount: IModelTransformerTestUtils.count(targetDb, "Bis.Element"),
    }
  );
}

