import * as fs from "fs";
import * as path from "path";
import { ElementGroupsMembers, IModelDb, IModelHost, PhysicalModel, PhysicalObject, SpatialCategory, StandaloneDb } from "@itwin/core-backend";
import { Guid, OpenMode } from "@itwin/core-bentley";
import { BriefcaseIdValue, Code } from "@itwin/core-common";
import { initOutputFile } from "./TestUtils";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { IModelTransformerTestUtils } from "@itwin/imodel-transformer/lib/cjs/test/IModelTransformerUtils";
import { getTShirtSizeFromName, TestIModel  } from "./TestContext";

const outputDir = path.join(__dirname, ".output");

export interface IModelParams {
  numElements: number;
  fedGuids: boolean;
}

// TODO: dedup with other packages
// for testing purposes only, based on SetToStandalone.ts, force a snapshot to mimic a standalone iModel
export function setToStandalone(iModelPath: string) {
  const nativeDb = new IModelHost.platform.DgnDb();
  nativeDb.openIModel(iModelPath, OpenMode.ReadWrite);
  nativeDb.setITwinId(Guid.empty); // empty iTwinId means "standalone"
  nativeDb.saveChanges(); // save change to iTwinId
  nativeDb.deleteAllTxns(); // necessary before resetting briefcaseId
  nativeDb.resetBriefcaseId(BriefcaseIdValue.Unassigned); // standalone iModels should always have BriefcaseId unassigned
  nativeDb.saveLocalValue("StandaloneEdit", JSON.stringify({ txns: true }));
  nativeDb.saveChanges(); // save change to briefcaseId
  nativeDb.closeIModel();
}

export function generateTestIModel(iModelParam: IModelParams): TestIModel {
  const sourcePath = initOutputFile(`testIModel-fedguids-${iModelParam.fedGuids}.bim`, outputDir);
  if (fs.existsSync(sourcePath))
    fs.unlinkSync(sourcePath);

  let sourceDb = StandaloneDb.createEmpty(sourcePath, { rootSubject: { name: `testIModel-fedguids-${iModelParam.fedGuids}.bim` }});
  const pathName = sourceDb.pathName;
  sourceDb.close();
  setToStandalone(pathName);
  sourceDb = StandaloneDb.openFile(sourcePath, OpenMode.ReadWrite);

  const physModelId = PhysicalModel.insert(sourceDb, IModelDb.rootSubjectId, "physical model");
  const categoryId = SpatialCategory.insert(sourceDb, IModelDb.dictionaryId, "spatial category", {});

  // 100,000 elements, 50,000  relationships
  for (let i = 0; i < iModelParam.numElements / 2; ++i) {
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
      federationGuid: iModelParam.fedGuids ? Guid.createValue() : Guid.empty, // Guid.empty = 00000000-0000-0000-0000-000000000000
    }, sourceDb).insert());

    const rel = new ElementGroupsMembers({
      classFullName: ElementGroupsMembers.classFullName,
      sourceId: id1,
      targetId: id2,
      memberPriority: i,
    }, sourceDb);

    rel.insert();
  }
  const iModelId = sourceDb.iModelId;
  const iTwinId = sourceDb.iTwinId;
  sourceDb.close();
  const iModelToTest: TestIModel = {
    name: `testIModel-fedguids-${iModelParam.fedGuids}`,
    iModelId,
    iTwinId,
    tShirtSize: getTShirtSizeFromName(sourceDb.name),
    load: async () => StandaloneDb.openFile(sourcePath, OpenMode.ReadWrite),
  };
  return iModelToTest;
}
