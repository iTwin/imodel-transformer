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

import { AccessToken, GuidString } from "@itwin/core-bentley";
import {
  Code,
  ExternalSourceAspectProps,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import {
  IModelTransformer,
  IModelTransformerError,
} from "../../imodel-transformer";

import {
  assertTransformerError,
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

describe("IModelTransformerHub - version tracking", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  registerHubTestContext("IModelTransformerHubVersionTracking", (context) => {
    iTwinId = context.iTwinId;
    accessToken = context.accessToken;
  });

  it("should fail processingChanges on pre-version-tracking forks unless branchRelationshipDataBehavior is 'unsafe-migrate'", async () => {
    let synchronizationVersionErrorAsserted = false;
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
        "PreVersionTrackingMaster"
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
          "PreVersionTrackingBranch"
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

      const scopeProvenanceCandidates = branchDb.elements
        .getAspects(IModelDb.rootSubjectId, ExternalSourceAspect.classFullName)
        .filter(
          (a) => (a as ExternalSourceAspect).identifier === masterDb!.iModelId
        );
      expect(scopeProvenanceCandidates).to.have.length(1);
      targetScopeProvenanceProps =
        scopeProvenanceCandidates[0].toJSON() as ExternalSourceAspectProps;
      expect(targetScopeProvenanceProps).to.deep.subsetEqual({
        identifier: masterDb.iModelId,
        version: `${masterDb.changeset.id};${masterDb.changeset.index}`,
        jsonProperties: JSON.stringify({
          pendingReverseSyncChangesetIndices: [1],
          pendingSyncChangesetIndices: [],
          reverseSyncVersion: ";0",
        }),
      });

      withEditTxn(branchDb, "clear branch target scope json", (txn) => {
        txn.updateAspect({
          ...targetScopeProvenanceProps!,
          jsonProperties: undefined,
        });
      });
      await branchDb.pushChanges({
        accessToken,
        description: "clear branch target scope json",
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
      let reverseError: unknown;
      try {
        await withTransformerLifecycle(reverseSyncer, [
          reverseTargetEditTxn,
          reverseSourceEditTxn,
        ]);
      } catch (error) {
        reverseError = error;
      }
      expect(reverseError).to.not.be.undefined;

      const branchForwardInitTargetTxn = createStartedEditTxn(branchDb);
      const branchForwardInit = new IModelTransformer(
        { source: masterDb, target: branchForwardInitTargetTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
        }
      );
      await withTransformerLifecycle(branchForwardInit, [
        branchForwardInitTargetTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "forward sync with missing json properties",
      });

      withEditTxn(branchDb, "clear branch target scope version", (txn) => {
        txn.updateAspect({
          ...targetScopeProvenanceProps!,
          version: undefined,
        } as ExternalSourceAspectProps);
      });
      await branchDb.pushChanges({
        accessToken,
        description: "clear branch target scope version",
      });

      const branchForwardTargetTxn = createStartedEditTxn(branchDb);
      const branchForward = new IModelTransformer(
        { source: masterDb, target: branchForwardTargetTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
        }
      );
      let forwardError: unknown;
      try {
        await withTransformerLifecycle(
          branchForward,
          [branchForwardTargetTxn],
          async () => {
            try {
              await branchForward.process();
            } catch (error) {
              assertTransformerError(
                error,
                IModelTransformerError.SynchronizationVersionMissing,
                "Could not find synchronization version in scope aspect. This may be due to the last successful run of the transformer being done with an older version.\n         Consider running the transformer with branchRelationshipDataBehavior set to 'unsafe-migrate'"
              );
              synchronizationVersionErrorAsserted = true;
              throw error;
            }
          }
        );
      } catch (error) {
        forwardError = error;
      }
      expect(forwardError).to.not.be.undefined;

      const masterReverseTargetTxn = createStartedEditTxn(masterDb);
      const masterReverseSourceTxn = createStartedEditTxn(branchDb);
      const masterReverse = new IModelTransformer(
        { source: branchDb, target: masterReverseTargetTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn: masterReverseSourceTxn,
        }
      );
      await withTransformerLifecycle(masterReverse, [
        masterReverseTargetTxn,
        masterReverseSourceTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "reverse sync after missing version",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "reverse sync after missing version",
      });

      withEditTxn(
        branchDb,
        "clear branch target scope relationship data",
        (txn) => {
          txn.updateAspect({
            ...targetScopeProvenanceProps!,
            jsonProperties: undefined,
            version: undefined,
          } as ExternalSourceAspectProps);
        }
      );
      await branchDb.pushChanges({
        accessToken,
        description: "clear branch target scope relationship data",
      });

      const unsafeForwardTargetTxn = createStartedEditTxn(branchDb);
      const unsafeForward = new IModelTransformer(
        { source: masterDb, target: unsafeForwardTargetTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
        }
      );
      await withTransformerLifecycle(
        unsafeForward,
        [unsafeForwardTargetTxn],
        async () => {
          setBranchRelationshipDataBehaviorToUnsafeMigrate(unsafeForward);
          await unsafeForward.process();
        }
      );
      await branchDb.pushChanges({
        accessToken,
        description: "unsafe forward sync",
      });

      const unsafeReverseTargetTxn = createStartedEditTxn(masterDb);
      const unsafeReverseSourceTxn = createStartedEditTxn(branchDb);
      const unsafeReverse = new IModelTransformer(
        { source: branchDb, target: unsafeReverseTargetTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn: unsafeReverseSourceTxn,
        }
      );
      await withTransformerLifecycle(
        unsafeReverse,
        [unsafeReverseTargetTxn, unsafeReverseSourceTxn],
        async () => {
          setBranchRelationshipDataBehaviorToUnsafeMigrate(unsafeReverse);
          await unsafeReverse.process();
        }
      );
      await branchDb.pushChanges({
        accessToken,
        description: "unsafe reverse sync",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "unsafe reverse sync",
      });

      expect(masterDb.changeset.index).to.equal(3);
      expect(branchDb.changeset.index).to.equal(8);
      expect(count(masterDb, ExternalSourceAspect.classFullName)).to.equal(0);
      const sql = `
          SELECT e.ECInstanceId as elementId, COUNT(*) as aspectCount FROM bis.ExternalSourceAspect esa
          JOIN bis.Element e ON e.ECInstanceId=esa.Element.Id
          GROUP BY e.ECInstanceId
          `;
      const externalAspectCounts = async (db: IModelDb) => {
        const results = [];
        for await (const row of db.createQueryReader(sql))
          results.push(row.toRow());
        return results;
      };
      expect(count(branchDb, "bis.ExternalSourceAspect")).to.be.equal(
        count(masterDb, "bis.Element")
      );
      expect(count(branchDb, "bis.Element")).to.be.equal(
        count(masterDb, "bis.Element")
      );
      (await externalAspectCounts(branchDb)).forEach((value) => {
        expect(value.aspectCount).to.equal(1);
      });
      const finalScopeCandidates = branchDb.elements
        .getAspects(IModelDb.rootSubjectId, ExternalSourceAspect.classFullName)
        .filter(
          (a) => (a as ExternalSourceAspect).identifier === masterDb!.iModelId
        );
      expect(finalScopeCandidates).to.have.length(1);
      const finalScope =
        finalScopeCandidates[0].toJSON() as ExternalSourceAspectProps;
      expect(finalScope.version).to.match(/;2$/);
      const finalScopeJsonProps = JSON.parse(finalScope.jsonProperties);
      expect(finalScopeJsonProps).to.deep.subsetEqual({
        pendingReverseSyncChangesetIndices: [8],
        pendingSyncChangesetIndices: [3],
      });
      expect(finalScopeJsonProps.reverseSyncVersion).to.match(/;7$/);

      const postCheckForwardTargetTxn = createStartedEditTxn(branchDb);
      const postCheckForward = new IModelTransformer(
        { source: masterDb, target: postCheckForwardTargetTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
        }
      );
      await withTransformerLifecycle(postCheckForward, [
        postCheckForwardTargetTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "post-check forward sync",
      });
      const postCheckReverseTargetTxn = createStartedEditTxn(masterDb);
      const postCheckReverseSourceTxn = createStartedEditTxn(branchDb);
      const postCheckReverse = new IModelTransformer(
        { source: branchDb, target: postCheckReverseTargetTxn },
        {
          forceExternalSourceAspectProvenance: true,
          argsForProcessChanges: {
            startChangeset: { index: undefined },
          },
          sourceEditTxn: postCheckReverseSourceTxn,
        }
      );
      await withTransformerLifecycle(postCheckReverse, [
        postCheckReverseTargetTxn,
        postCheckReverseSourceTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "post-check reverse sync",
      });
      await masterDb.pushChanges({
        accessToken,
        description: "post-check reverse sync",
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

    expect(synchronizationVersionErrorAsserted).to.be.true;
  });
});
