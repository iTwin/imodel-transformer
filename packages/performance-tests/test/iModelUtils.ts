/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import * as fs from "fs";
import * as path from "path";
import {
  ElementGroupsMembers,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  StandaloneDb,
  withEditTxn,
} from "@itwin/core-backend";
import { Guid, OpenMode } from "@itwin/core-bentley";
import { Code } from "@itwin/core-common";
import { initOutputFile } from "./TestUtils";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { IModelTransformerTestUtils } from "@itwin/imodel-transformer/lib/cjs/test/IModelTransformerUtils";
import { getTShirtSizeFromName, TestIModel } from "./TestContext";

const outputDir = path.join(__dirname, ".output");

export interface IModelParams {
  fileName: string;
  numElements: number;
  fedGuids: boolean;
}

// for testing purposes only, based on SetToStandalone.ts, force a snapshot to mimic a standalone iModel
export function setToStandalone(iModelPath: string) {
  StandaloneDb.convertToStandalone(iModelPath);
}

export function generateTestIModel(iModelParam: IModelParams): TestIModel {
  const sourcePath = initOutputFile(iModelParam.fileName, outputDir);
  if (fs.existsSync(sourcePath)) fs.unlinkSync(sourcePath);

  let sourceDb = StandaloneDb.createEmpty(sourcePath, {
    rootSubject: { name: iModelParam.fileName },
  });
  const pathName = sourceDb.pathName;
  sourceDb.close();
  setToStandalone(pathName);
  sourceDb = StandaloneDb.openFile(sourcePath, OpenMode.ReadWrite);

  withEditTxn(sourceDb, "generate test iModel", (txn) => {
    const physModelId = PhysicalModel.insert(
      txn,
      IModelDb.rootSubjectId,
      "physical model"
    );
    const categoryId = SpatialCategory.insert(
      txn,
      IModelDb.dictionaryId,
      "spatial category",
      {}
    );

    for (let i = 0; i < iModelParam.numElements / 2; ++i) {
      const [id1, id2] = [0, 1].map((n) =>
        new PhysicalObject(
          {
            classFullName: PhysicalObject.classFullName,
            category: categoryId,
            geom: IModelTransformerTestUtils.createBox(Point3d.create(i, i, i)),
            placement: {
              origin: Point3d.create(i, i, i),
              angles: YawPitchRollAngles.createDegrees(i, i, i),
            },
            model: physModelId,
            code: new Code({
              spec: IModelDb.rootSubjectId,
              scope: IModelDb.rootSubjectId,
              value: `${2 * i + n}`,
            }),
            userLabel: `${2 * i + n}`,
            federationGuid: iModelParam.fedGuids ? undefined : Guid.empty, // Guid.empty = 00000000-0000-0000-0000-000000000000
          },
          sourceDb
        ).insert(txn)
      );

      const rel = new ElementGroupsMembers(
        {
          classFullName: ElementGroupsMembers.classFullName,
          sourceId: id1,
          targetId: id2,
          memberPriority: i,
        },
        sourceDb
      );

      rel.insert(txn);
    }
  });

  const iModelId = sourceDb.iModelId;
  const iTwinId = sourceDb.iTwinId;
  const filePath = sourceDb.pathName;
  sourceDb.close();
  const iModelToTest: TestIModel = {
    name: `testIModel-fedguids-${iModelParam.fedGuids}`,
    iModelId,
    iTwinId,
    tShirtSize: getTShirtSizeFromName(sourceDb.name),
    async getFileName(): Promise<string> {
      return filePath;
    },
  };
  return iModelToTest;
}
