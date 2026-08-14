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
import * as TestUtils from "../TestUtils";
import { AccessToken, GuidString } from "@itwin/core-bentley";
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

describe("IModelTransformerHub - unsafe migration", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;
  let saveAndPushChanges: (db: BriefcaseDb, desc: string) => Promise<void>;

  registerHubTestContext("IModelTransformerHubUnsafeMigration", (context) => {
    iTwinId = context.iTwinId;
    accessToken = context.accessToken;
    saveAndPushChanges = context.saveAndPushChanges;
  });

  it("should throw when pendingSyncChangesetIndices and pendingReverseSyncChangesetIndices are undefined and then not throw when they're undefined, but 'unsafe-migrate' is set.", async () => {
    let targetScopeProvenanceProps: ExternalSourceAspectProps | undefined;
    const setBranchRelationshipDataBehaviorToUnsafeMigrate = (
      transformer: IModelTransformer
    ) =>
      (transformer["_options"]["branchRelationshipDataBehavior"] =
        "unsafe-migrate");
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "PendingIndicesMaster"
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
      withEditTxn(masterDb, "insert master objects", (txn) => {
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
        description: "populate master objects",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: IModelTransformerTestUtils.generateUniqueName(
          "PendingIndicesBranch"
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
        { wasSourceIModelCopiedToTarget: true }
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

      const scopeProvenanceCandidates = branchDb.elements
        .getAspects(IModelDb.rootSubjectId, ExternalSourceAspect.classFullName)
        .filter(
          (a) => (a as ExternalSourceAspect).identifier === masterDb!.iModelId
        );
      expect(scopeProvenanceCandidates).to.have.length(1);
      const targetScopeProvenance =
        scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;
      expect(targetScopeProvenance).to.deep.subsetEqual({
        identifier: masterDb.iModelId,
        version: `${masterDb.changeset.id};${masterDb.changeset.index}`,
        jsonProperties: JSON.stringify({
          pendingReverseSyncChangesetIndices: [1],
          pendingSyncChangesetIndices: [],
          reverseSyncVersion: ";0",
        }),
      });
      targetScopeProvenanceProps = targetScopeProvenance;

      const missingPendings = JSON.stringify({
        pendingReverseSyncChangesetIndices: undefined,
        pendingSyncChangesetIndices: undefined,
        reverseSyncVersion: ";0",
      });
      withEditTxn(branchDb, "update target scope provenance", (txn) => {
        txn.updateAspect({
          ...targetScopeProvenanceProps!,
          jsonProperties: missingPendings as any,
        });
      });
      await branchDb.pushChanges({
        accessToken,
        description: "remove pending synchronization indices",
      });

      const failingTargetEditTxn = createStartedEditTxn(masterDb);
      const failingSourceEditTxn = createStartedEditTxn(branchDb);
      const failingSyncer = new IModelTransformer(
        { source: branchDb, target: failingTargetEditTxn },
        {
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn: failingSourceEditTxn,
        }
      );
      let syncError: unknown;
      try {
        await failingSyncer.process();
      } catch (error) {
        syncError = error;
      } finally {
        failingSyncer.dispose();
        failingTargetEditTxn.end(syncError ? "abandon" : "save");
        failingSourceEditTxn.end(syncError ? "abandon" : "save");
      }
      expect(syncError).to.not.be.undefined;

      const missingAspect = branchDb.elements.getAspect(
        targetScopeProvenanceProps.id!
      );
      expect(missingAspect).to.not.be.undefined;
      expect((missingAspect as ExternalSourceAspect).jsonProperties).to.equal(
        missingPendings
      );

      const targetEditTxn = createStartedEditTxn(masterDb);
      const sourceEditTxn = createStartedEditTxn(branchDb);
      const syncer = new IModelTransformer(
        { source: branchDb, target: targetEditTxn },
        {
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn,
        }
      );
      let syncSucceeded = false;
      try {
        setBranchRelationshipDataBehaviorToUnsafeMigrate(syncer);
        await syncer.process();
        syncSucceeded = true;
      } finally {
        syncer.dispose();
        targetEditTxn.end(syncSucceeded ? "save" : "abandon");
        sourceEditTxn.end(syncSucceeded ? "save" : "abandon");
      }
      await branchDb.pushChanges({
        accessToken,
        description: "migrate target scope provenance",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "migrate target scope provenance",
      });

      const migratedAspect = branchDb.elements.getAspect(
        targetScopeProvenanceProps.id!
      );
      expect(migratedAspect).to.not.be.undefined;
      const jsonProps = JSON.parse(
        (migratedAspect as ExternalSourceAspect).jsonProperties!
      );
      expect((migratedAspect as any).version).to.match(/;1$/);
      expect(jsonProps.reverseSyncVersion).to.match(/;3$/);
      expect(jsonProps).to.deep.subsetEqual({
        pendingReverseSyncChangesetIndices: [4],
        pendingSyncChangesetIndices: [2],
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

  it("should set unsafeVersions correctly when branchRelationshipDataBehavior is 'unsafe-migrate'", async () => {
    let targetScopeProvenanceProps: ExternalSourceAspectProps | undefined;
    const setBranchRelationshipDataBehaviorToUnsafeMigrate = (
      transformer: IModelTransformer
    ) => {
      transformer["_options"]["branchRelationshipDataBehavior"] =
        "unsafe-migrate";
      transformer["_options"]["argsForProcessChanges"]![
        "unsafeFallbackReverseSyncVersion"
      ] = ";2";
      transformer["_options"]["argsForProcessChanges"]![
        "unsafeFallbackSyncVersion"
      ] = ";3";
    };

    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "UnsafeVersionsMaster"
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
      withEditTxn(masterDb, "insert master objects", (txn) => {
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
        description: "populate master objects",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: IModelTransformerTestUtils.generateUniqueName(
          "UnsafeVersionsBranch"
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
        { wasSourceIModelCopiedToTarget: true }
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

      const initialScopeProvenance = branchDb.elements
        .getAspects(IModelDb.rootSubjectId, ExternalSourceAspect.classFullName)
        .filter(
          (a) => (a as ExternalSourceAspect).identifier === masterDb!.iModelId
        );
      expect(initialScopeProvenance).to.have.length(1);
      const initialScope =
        initialScopeProvenance[0].toJSON() as ExternalSourceAspectProps;
      expect(initialScope).to.deep.subsetEqual({
        identifier: masterDb.iModelId,
        version: `${masterDb.changeset.id};${masterDb.changeset.index}`,
        jsonProperties: JSON.stringify({
          pendingReverseSyncChangesetIndices: [1],
          pendingSyncChangesetIndices: [],
          reverseSyncVersion: ";0",
        }),
      });
      targetScopeProvenanceProps = initialScope;

      withEditTxn(branchDb, "clear target scope provenance json", (txn) => {
        txn.updateAspect({
          ...targetScopeProvenanceProps!,
          jsonProperties: undefined,
        });
      });
      await branchDb.pushChanges({
        accessToken,
        description: "clear target scope provenance json",
      });

      const reverseTargetEditTxn = createStartedEditTxn(masterDb);
      const reverseSourceEditTxn = createStartedEditTxn(branchDb);
      const reverseSyncer = new IModelTransformer(
        { source: branchDb, target: reverseTargetEditTxn },
        {
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn: reverseSourceEditTxn,
        }
      );
      let reverseSyncSucceeded = false;
      try {
        setBranchRelationshipDataBehaviorToUnsafeMigrate(reverseSyncer);
        await reverseSyncer.process();
        reverseSyncSucceeded = true;
      } finally {
        reverseSyncer.dispose();
        reverseTargetEditTxn.end(reverseSyncSucceeded ? "save" : "abandon");
        reverseSourceEditTxn.end(reverseSyncSucceeded ? "save" : "abandon");
      }
      await branchDb.pushChanges({
        accessToken,
        description: "unsafe reverse sync",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "unsafe reverse sync",
      });

      const expectedState = { 1: 1, 2: 2, 3: 1, 5: 1 };
      await assertHubTestIModelState(masterDb, expectedState);
      await assertHubTestIModelState(branchDb, {
        ...expectedState,
        1: 2,
        4: 1,
      });

      withEditTxn(masterDb, "update master objects", (txn) => {
        const element2 = masterDb!.elements.getElement(
          IModelTestUtils.queryByUserLabel(masterDb!, "2")
        );
        txn.updateElement({
          ...element2.toJSON(),
          jsonProperties: { ...element2.jsonProperties, updateState: 4 },
        });
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({
            spec: IModelDb.rootSubjectId,
            scope: IModelDb.rootSubjectId,
            value: "6",
          }),
          userLabel: "6",
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
        description: "update master objects",
      });
      withEditTxn(masterDb, "insert final master object", (txn) => {
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: new Code({
            spec: IModelDb.rootSubjectId,
            scope: IModelDb.rootSubjectId,
            value: "7",
          }),
          userLabel: "7",
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
        description: "insert final master object",
      });

      const updatedScopeProvenance = branchDb.elements
        .getAspects(IModelDb.rootSubjectId, ExternalSourceAspect.classFullName)
        .filter(
          (a) => (a as ExternalSourceAspect).identifier === masterDb!.iModelId
        );
      expect(updatedScopeProvenance).to.have.length(1);
      targetScopeProvenanceProps =
        updatedScopeProvenance[0].toJSON() as ExternalSourceAspectProps;
      withEditTxn(branchDb, "clear target scope provenance version", (txn) => {
        txn.updateAspect({
          ...targetScopeProvenanceProps!,
          version: undefined,
        } as ExternalSourceAspectProps);
      });
      await branchDb.pushChanges({
        accessToken,
        description: "clear target scope provenance version",
      });

      const forwardTargetEditTxn = createStartedEditTxn(branchDb);
      const forwardSyncer = new IModelTransformer(
        { source: masterDb, target: forwardTargetEditTxn },
        {
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
        }
      );
      let forwardSyncSucceeded = false;
      try {
        setBranchRelationshipDataBehaviorToUnsafeMigrate(forwardSyncer);
        await forwardSyncer.process();
        forwardSyncSucceeded = true;
      } finally {
        forwardSyncer.dispose();
        forwardTargetEditTxn.end(forwardSyncSucceeded ? "save" : "abandon");
      }
      await branchDb.pushChanges({
        accessToken,
        description: "unsafe forward sync",
      });

      const expectedMasterState = { 1: 1, 2: 4, 3: 1, 5: 1, 6: 1, 7: 1 };
      const expectedBranchState = { 1: 2, 2: 2, 3: 1, 4: 1, 5: 1, 7: 1 };
      await assertHubTestIModelState(masterDb, expectedMasterState);
      await assertHubTestIModelState(branchDb, expectedBranchState);
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

  it("reverseSyncs should not push extra changesets if the only changeset to process is one found in the pendingReverseSyncIndices, even when handleUnsafeMigrate is true", async () => {
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "PendingReverseSyncMaster"
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
      withEditTxn(masterDb, "insert master objects", (txn) => {
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
        description: "populate master objects",
      });
      masterDb.performCheckpoint();

      branchIModelId = await HubWrappers.recreateIModel({
        accessToken,
        iTwinId,
        iModelName: IModelTransformerTestUtils.generateUniqueName(
          "PendingReverseSyncBranch"
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
        { wasSourceIModelCopiedToTarget: true }
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

      const assertProvenance = () => {
        expect(masterDb!.changeset.index).to.equal(2);
        expect(branchDb!.changeset.index).to.equal(3);
        expect(count(masterDb!, ExternalSourceAspect.classFullName)).to.equal(
          0
        );
        const expectedProps: TestUtils.ExpectedTargetScopeProvenanceProps = {
          pendingSyncChangesetIndices: [2],
          pendingReverseSyncChangesetIndices: [3],
          syncVersionIndex: "1",
          reverseSyncVersionIndex: "2",
        };
        IModelTestUtils.findAndAssertTargetScopeProvenance(
          masterDb!,
          branchDb!,
          expectedProps
        );
      };

      for (const unsafeMigrate of [false, false, true]) {
        const targetEditTxn = createStartedEditTxn(masterDb);
        const sourceEditTxn = createStartedEditTxn(branchDb);
        const syncer = new IModelTransformer(
          { source: branchDb, target: targetEditTxn },
          {
            argsForProcessChanges: {
              startChangeset: { index: undefined },
            },
            sourceEditTxn,
          }
        );
        await withTransformerLifecycle(
          syncer,
          [targetEditTxn, sourceEditTxn],
          async () => {
            if (unsafeMigrate)
              syncer["_options"]["branchRelationshipDataBehavior"] =
                "unsafe-migrate";
            await syncer.process();
          }
        );
        await saveAndPushChanges(branchDb, "reverse sync");
        await saveAndPushChanges(masterDb, "reverse sync");
        assertProvenance();
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
