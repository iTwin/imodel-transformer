/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "vitest";

import {
  BriefcaseDb,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";

import { AccessToken, GuidString } from "@itwin/core-bentley";
import {
  Code,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { IModelTransformer } from "../../imodel-transformer";

import {
  createStartedEditTxn,
  HubWrappers,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import { registerHubTestContext } from "../TestUtils/HubTestContext";

describe("IModelTransformerHub - branch provenance", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  registerHubTestContext("IModelTransformerHubBranchProvenance", (context) => {
    iTwinId = context.iTwinId;
    accessToken = context.accessToken;
  });

  it("should not include 'initialized branch provenance' changeset in a reverse sync", async () => {
    const validateCsFileProps = (transformer: IModelTransformer) => {
      const csFileProps = transformer["_csFileProps"];
      expect(
        csFileProps?.some((csFileProp) =>
          csFileProp.description.includes("initialized branch provenance")
        )
      ).to.be.false;
    };
    const masterIModelName = IModelTransformerTestUtils.generateUniqueName(
      "InitializedBranchProvenanceMaster"
    );
    const branchIModelName = IModelTransformerTestUtils.generateUniqueName(
      "InitializedBranchProvenanceBranch"
    );
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: masterIModelName,
      noLocks: true,
    });
    let masterDb: BriefcaseDb | undefined;
    let branchDb: BriefcaseDb | undefined;
    let branchIModelId: GuidString | undefined;

    try {
      masterDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: masterIModelId,
      });
      const { modelId, categoryId } = withEditTxn(
        masterDb,
        "populate master seed",
        (txn) => ({
          modelId: PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PhysicalModel"
          ),
          categoryId: SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "SpatialCategory",
            new SubCategoryAppearance()
          ),
        })
      );
      withEditTxn(masterDb, "maintain master objects", (txn) => {
        for (const [name, updateState] of Object.entries({
          1: 1,
          2: 2,
          3: 1,
        })) {
          txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: new Code({
              spec: IModelDb.rootSubjectId,
              scope: IModelDb.rootSubjectId,
              value: name,
            }),
            userLabel: name,
            geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
            placement: {
              origin: Point3d.create(0, 0, 0),
              angles: YawPitchRollAngles.createDegrees(0, 0, 0),
            },
            jsonProperties: { updateState },
          } as PhysicalElementProps);
        }
      });
      await masterDb.pushChanges({
        accessToken,
        description: "seeded master",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: branchIModelName,
        noLocks: true,
        version0: masterDb.pathName,
      });
      branchDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: branchIModelId,
      });
      const branchInitEditTxn = createStartedEditTxn(branchDb);
      const provenanceInitializer = new IModelTransformer(
        { source: masterDb, target: branchInitEditTxn },
        { wasSourceIModelCopiedToTarget: true }
      );
      let branchInitSucceeded = false;
      try {
        await provenanceInitializer.process();
        branchInitSucceeded = true;
      } finally {
        provenanceInitializer.dispose();
        branchInitEditTxn.end(branchInitSucceeded ? "save" : "abandon");
      }
      await branchDb.pushChanges({
        accessToken,
        description: "initialized branch provenance",
      });

      withEditTxn(branchDb, "update branch objects", (txn) => {
        const element1 = branchDb!.elements.getElement(
          IModelTestUtils.queryByUserLabel(branchDb!, "1")
        );
        txn.updateElement({
          ...element1.toJSON(),
          jsonProperties: { ...element1.jsonProperties, updateState: 2 },
        });
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({
            spec: IModelDb.rootSubjectId,
            scope: IModelDb.rootSubjectId,
            value: "4",
          }),
          userLabel: "4",
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: {
            origin: Point3d.create(0, 0, 0),
            angles: YawPitchRollAngles.createDegrees(0, 0, 0),
          },
          jsonProperties: { updateState: 1 },
        } as PhysicalElementProps);
      });
      await branchDb.pushChanges({
        accessToken,
        description: "updated branch objects",
      });

      const reverseSyncEditTxn = createStartedEditTxn(masterDb);
      const reverseSyncSourceEditTxn = createStartedEditTxn(branchDb);
      const reverseSyncer = new IModelTransformer(
        { source: branchDb, target: reverseSyncEditTxn },
        {
          sourceEditTxn: reverseSyncSourceEditTxn,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
        }
      );
      let reverseSyncSucceeded = false;
      try {
        await reverseSyncer.process();
        validateCsFileProps(reverseSyncer);
        reverseSyncSucceeded = true;
      } finally {
        reverseSyncer.dispose();
        reverseSyncEditTxn.end(reverseSyncSucceeded ? "save" : "abandon");
        reverseSyncSourceEditTxn.end(reverseSyncSucceeded ? "save" : "abandon");
      }
      await branchDb.pushChanges({
        accessToken,
        description: "reverse sync",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "reverse sync",
      });
    } finally {
      if (masterDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, masterDb);
      if (branchDb)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchDb);
      await transformerTestHub.deleteIModel({
        iTwinId,
        iModelId: masterIModelId,
      });
      if (branchIModelId)
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: branchIModelId,
        });
    }
  });
});
