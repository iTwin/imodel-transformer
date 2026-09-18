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
  ExternalSourceAspect,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";

import { AccessToken, GuidString, Id64 } from "@itwin/core-bentley";
import {
  Code,
  ExternalSourceAspectProps,
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
  withTransformerLifecycle,
} from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import { registerHubTestContext } from "../TestUtils/HubTestContext";
import { assertHubTestIModelState } from "../TestUtils/HubTestState";

const { count } = IModelTestUtils;

describe("IModelTransformerHub - provenance", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  registerHubTestContext("IModelTransformerHubProvenance", (context) => {
    iTwinId = context.iTwinId;
    accessToken = context.accessToken;
  });

  it("should skip provenance changesets made to branch during reverse sync", async () => {
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "SkipProvenanceMaster"
      ),
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
        "populate master objects",
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
      withEditTxn(masterDb, "insert master object 1", (txn) => {
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({
            spec: IModelDb.rootSubjectId,
            scope: IModelDb.rootSubjectId,
            value: "1",
          }),
          userLabel: "1",
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: {
            origin: Point3d.create(0, 0, 0),
            angles: YawPitchRollAngles.createDegrees(0, 0, 0),
          },
          jsonProperties: { updateState: 1 },
        } as PhysicalElementProps);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "insert master object 1",
      });
      withEditTxn(masterDb, "insert master object 2", (txn) => {
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({
            spec: IModelDb.rootSubjectId,
            scope: IModelDb.rootSubjectId,
            value: "2",
          }),
          userLabel: "2",
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: {
            origin: Point3d.create(0, 0, 0),
            angles: YawPitchRollAngles.createDegrees(0, 0, 0),
          },
          jsonProperties: { updateState: 2 },
        } as PhysicalElementProps);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "insert master object 2",
      });
      withEditTxn(masterDb, "insert master object 3", (txn) => {
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({
            spec: IModelDb.rootSubjectId,
            scope: IModelDb.rootSubjectId,
            value: "3",
          }),
          userLabel: "3",
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: {
            origin: Point3d.create(0, 0, 0),
            angles: YawPitchRollAngles.createDegrees(0, 0, 0),
          },
          jsonProperties: { updateState: 1 },
        } as PhysicalElementProps);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "insert master object 3",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: IModelTransformerTestUtils.generateUniqueName(
          "SkipProvenanceBranch"
        ),
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
        {
          forceExternalSourceAspectProvenance: true,
          wasSourceIModelCopiedToTarget: true,
        }
      );
      await withTransformerLifecycle(provenanceInitializer, [
        branchInitEditTxn,
      ]);
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
        description: "update branch objects",
      });

      expect(masterDb.changeset.index).to.equal(3);
      expect(branchDb.changeset.index).to.equal(2);
      expect(count(masterDb, ExternalSourceAspect.classFullName)).to.equal(0);
      expect(count(branchDb, ExternalSourceAspect.classFullName)).to.equal(9);
      const firstScopeCandidates = branchDb.elements
        .getAspects(IModelDb.rootSubjectId, ExternalSourceAspect.classFullName)
        .filter(
          (a) => (a as ExternalSourceAspect).identifier === masterDb!.iModelId
        );
      expect(firstScopeCandidates).to.have.length(1);
      const firstScope =
        firstScopeCandidates[0].toJSON() as ExternalSourceAspectProps;
      expect(firstScope).to.deep.subsetEqual({
        identifier: masterDb.iModelId,
        version: `${masterDb.changeset.id};${masterDb.changeset.index}`,
        jsonProperties: JSON.stringify({
          pendingReverseSyncChangesetIndices: [1],
          pendingSyncChangesetIndices: [],
          reverseSyncVersion: ";0",
        }),
      });

      const reverseTargetEditTxn = createStartedEditTxn(masterDb);
      const reverseSourceEditTxn = createStartedEditTxn(branchDb);
      const reverseSyncer = new IModelTransformer(
        { source: branchDb, target: reverseTargetEditTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn: reverseSourceEditTxn,
        }
      );
      await withTransformerLifecycle(reverseSyncer, [
        reverseTargetEditTxn,
        reverseSourceEditTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "reverse sync",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "reverse sync",
      });

      expect(masterDb.changeset.index).to.equal(4);
      expect(branchDb.changeset.index).to.equal(3);
      expect(count(masterDb, ExternalSourceAspect.classFullName)).to.equal(0);
      expect(count(branchDb, ExternalSourceAspect.classFullName)).to.equal(10);
      const secondScopeCandidates = branchDb.elements
        .getAspects(IModelDb.rootSubjectId, ExternalSourceAspect.classFullName)
        .filter(
          (a) => (a as ExternalSourceAspect).identifier === masterDb!.iModelId
        );
      expect(secondScopeCandidates).to.have.length(1);
      const secondScope =
        secondScopeCandidates[0].toJSON() as ExternalSourceAspectProps;
      expect(secondScope.version).to.match(/;3$/);
      const secondScopeJson = JSON.parse(secondScope.jsonProperties);
      expect(secondScopeJson).to.deep.subsetEqual({
        pendingReverseSyncChangesetIndices: [3],
        pendingSyncChangesetIndices: [4],
      });
      expect(secondScopeJson.reverseSyncVersion).to.match(/;2$/);

      const forwardTargetEditTxn = createStartedEditTxn(branchDb);
      const forwardSyncer = new IModelTransformer(
        { source: masterDb, target: forwardTargetEditTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
        }
      );
      await withTransformerLifecycle(forwardSyncer, [forwardTargetEditTxn]);
      await branchDb.pushChanges({
        accessToken,
        description: "forward sync",
      });
      withEditTxn(branchDb, "insert final branch object", (txn) => {
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({
            spec: IModelDb.rootSubjectId,
            scope: IModelDb.rootSubjectId,
            value: "5",
          }),
          userLabel: "5",
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
        description: "insert final branch object",
      });
      const finalReverseTargetTxn = createStartedEditTxn(masterDb);
      const finalReverseSourceTxn = createStartedEditTxn(branchDb);
      const finalReverse = new IModelTransformer(
        { source: branchDb, target: finalReverseTargetTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn: finalReverseSourceTxn,
        }
      );
      await withTransformerLifecycle(finalReverse, [
        finalReverseTargetTxn,
        finalReverseSourceTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "final reverse sync",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "final reverse sync",
      });
      const expectedState = { 1: 2, 2: 2, 3: 1, 4: 1, 5: 1 };
      await assertHubTestIModelState(masterDb, expectedState);
      await assertHubTestIModelState(branchDb, expectedState);
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

  it("should successfully remove element in master iModel after reverse synchronization when elements have random ExternalSourceAspects", async () => {
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName:
        IModelTransformerTestUtils.generateUniqueName("RandomAspectMaster"),
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
        "populate master object",
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
      withEditTxn(masterDb, "insert master object", (txn) => {
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({
            spec: IModelDb.rootSubjectId,
            scope: IModelDb.rootSubjectId,
            value: "1",
          }),
          userLabel: "1",
          geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
          placement: {
            origin: Point3d.create(0, 0, 0),
            angles: YawPitchRollAngles.createDegrees(0, 0, 0),
          },
          jsonProperties: { updateState: 1 },
        } as PhysicalElementProps);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "populate master object",
      });
      withEditTxn(masterDb, "insert random external source aspect", (txn) => {
        const elemId = IModelTestUtils.queryByUserLabel(masterDb!, "1");
        txn.insertAspect({
          classFullName: ExternalSourceAspect.classFullName,
          element: { id: elemId },
          scope: { id: IModel.dictionaryId },
          kind: "Element",
          identifier: "bar code",
        } as ExternalSourceAspectProps);
      });
      await masterDb.pushChanges({
        accessToken,
        description: "insert random external source aspect",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName:
          IModelTransformerTestUtils.generateUniqueName("RandomAspectBranch"),
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
      await withTransformerLifecycle(provenanceInitializer, [
        branchInitEditTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "initialized branch provenance",
      });

      withEditTxn(branchDb, "delete branch object", (txn) => {
        txn.deleteElement(IModelTestUtils.queryByUserLabel(branchDb!, "1"));
      });
      await branchDb.pushChanges({
        accessToken,
        description: "delete branch object",
      });

      const reverseSyncEditTxn = createStartedEditTxn(masterDb);
      const reverseSyncSourceEditTxn = createStartedEditTxn(branchDb);
      const reverseSyncer = new IModelTransformer(
        { source: branchDb, target: reverseSyncEditTxn },
        {
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn: reverseSyncSourceEditTxn,
        }
      );
      await withTransformerLifecycle(reverseSyncer, [
        reverseSyncEditTxn,
        reverseSyncSourceEditTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "reverse sync deleted object",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "reverse sync deleted object",
      });

      for (const [db, name] of [
        [branchDb, "branch"],
        [masterDb, "master"],
      ] as const) {
        const elemId = IModelTestUtils.queryByUserLabel(db, "1");
        expect(elemId, `db ${name} did not delete ${elemId}`).to.equal(
          Id64.invalid
        );
      }
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
