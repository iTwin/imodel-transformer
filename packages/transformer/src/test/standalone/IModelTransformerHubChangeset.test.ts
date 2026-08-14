/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect, vi } from "vitest";

import {
  BriefcaseDb,
  BriefcaseManager,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  SpatialCategory,
  Subject,
  SubjectOwnsPartitionElements,
  SubjectOwnsSubjects,
  withEditTxn,
} from "@itwin/core-backend";

import { AccessToken, Guid, GuidString, Id64String } from "@itwin/core-bentley";
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
  withTransformerLifecycle,
} from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { transformerTestHub } from "../TestUtils/TransformerTestHub";
import { registerHubTestContext } from "../TestUtils/HubTestContext";

const { count } = IModelTestUtils;

describe("IModelTransformerHub - changesets", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  registerHubTestContext("IModelTransformerHubChangeset", (context) => {
    iTwinId = context.iTwinId;
    accessToken = context.accessToken;
  });

  it("should not download more changesets than necessary", async () => {
    const masterIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: IModelTransformerTestUtils.generateUniqueName(
        "DownloadChangesetsMaster"
      ),
      noLocks: true,
    });
    const branchIModelName = IModelTransformerTestUtils.generateUniqueName(
      "DownloadChangesetsBranch"
    );
    let masterDb: BriefcaseDb | undefined;
    let branchDb: BriefcaseDb | undefined;
    let branchAt2: BriefcaseDb | undefined;
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
      await masterDb.pushChanges({ accessToken, description: "seeded master" });
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
      await withTransformerLifecycle(provenanceInitializer, [
        branchInitEditTxn,
      ]);
      await branchDb.pushChanges({
        accessToken,
        description: "initialized branch provenance",
      });
      const branchAt2Changeset = branchDb.changeset;
      assert(branchAt2Changeset.index);

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
            value: "2",
          }),
          userLabel: "2",
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
      withEditTxn(branchDb, "insert final branch object", (txn) => {
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
          jsonProperties: { updateState: 3 },
        } as PhysicalElementProps);
      });
      await branchDb.pushChanges({
        accessToken,
        description: "insert final branch object",
      });

      branchAt2 = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: branchIModelId,
        asOf: { first: true },
      });
      await branchAt2.pullChanges({
        toIndex: branchAt2Changeset.index,
        accessToken,
      });

      const syncEditTxn = createStartedEditTxn(masterDb);
      const reverseSyncSourceEditTxn = createStartedEditTxn(branchAt2);
      const syncer = new IModelTransformer(
        { source: branchAt2, target: syncEditTxn },
        {
          argsForProcessChanges: {
            startChangeset: branchAt2Changeset,
          },
          sourceEditTxn: reverseSyncSourceEditTxn,
        }
      );
      const queryChangeset = vi.spyOn(BriefcaseManager, "queryChangeset");
      let syncSucceeded = false;
      try {
        await syncer.process();
        syncSucceeded = true;
        expect(queryChangeset.mock.calls).to.have.length.greaterThan(0);
        for (const [args] of queryChangeset.mock.calls) {
          expect(args).to.deep.equal({
            iModelId: branchIModelId,
            changeset: {
              id: branchAt2Changeset.id,
            },
          });
        }
      } finally {
        syncer.dispose();
        syncEditTxn.end(syncSucceeded ? "save" : "abandon");
        reverseSyncSourceEditTxn.end(syncSucceeded ? "save" : "abandon");
      }
    } finally {
      if (branchAt2)
        await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, branchAt2);
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
      vi.restoreAllMocks();
    }
  });

  it("should reverse synchronize forked iModel when an element was updated", async () => {
    const sourceIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Master");
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Fork");
    const targetIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: targetIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });

      const originalElementId = withEditTxn(
        sourceDb,
        "insert physical element",
        (txn) => {
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "C1",
            {}
          );
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PM1"
          );
          const physicalElement: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: "Element1",
          };
          return txn.insertElement(physicalElement);
        }
      );
      await sourceDb.pushChanges({ description: "insert physical element" });

      const initialForkEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: initialForkEditTxn,
      });
      let forkedElementId!: Id64String;
      await withTransformerLifecycle(
        transformer,
        [initialForkEditTxn],
        async () => {
          await transformer.process();
          forkedElementId =
            transformer.context.findTargetElementId(originalElementId);
          expect(forkedElementId).not.to.be.undefined;
        }
      );
      await targetDb.pushChanges({ description: "initial transformation" });

      withEditTxn(targetDb, "update forked element", (txn) => {
        const forkedElement = targetDb.elements.getElement(forkedElementId);
        forkedElement.userLabel = "Element1_updated";
        forkedElement.update(txn);
      });
      await targetDb.pushChanges({
        description: "update forked element's userLabel",
      });

      const reverseForkEditTxn = createStartedEditTxn(sourceDb);
      const reverseForkSourceEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: targetDb, target: reverseForkEditTxn },
        {
          argsForProcessChanges: { startChangeset: targetDb.changeset },
          sourceEditTxn: reverseForkSourceEditTxn,
        }
      );
      await withTransformerLifecycle(transformer, [
        reverseForkEditTxn,
        reverseForkSourceEditTxn,
      ]);
      await sourceDb.pushChanges({
        description: "change processing transformation",
      });

      const masterElement = sourceDb.elements.getElement(originalElementId);
      expect(masterElement).to.not.be.undefined;
      expect(masterElement.userLabel).to.be.equal("Element1_updated");
    } finally {
      try {
        // delete iModel briefcases
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });

  it("should preserve FederationGuid when element is recreated within the same changeset and across changesets", async () => {
    const sourceIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Source");
    const sourceIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: sourceIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(sourceIModelId));
    const targetIModelName: string =
      IModelTransformerTestUtils.generateUniqueName("Fork");
    const targetIModelId = await HubWrappers.recreateIModel({
      accessToken,
      iTwinId,
      iModelName: targetIModelName,
      noLocks: true,
    });
    assert.isTrue(Guid.isGuid(targetIModelId));

    try {
      const sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: sourceIModelId,
      });
      const targetDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId,
        iModelId: targetIModelId,
      });

      const constSubjectFedGuid = Guid.createValue();
      const constPartitionFedGuid = Guid.createValue();
      const { originalSubjectId, originalPartitionId, originalModelId } =
        withEditTxn(sourceDb, "insert elements & models", (txn) => {
          const subjId = txn.insertElement({
            classFullName: Subject.classFullName,
            code: Code.createEmpty(),
            model: IModel.repositoryModelId,
            parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
            federationGuid: constSubjectFedGuid,
            userLabel: "A",
          });

          const partId = txn.insertElement({
            model: IModel.repositoryModelId,
            code: PhysicalPartition.createCode(
              sourceDb,
              IModel.rootSubjectId,
              "original partition"
            ),
            classFullName: PhysicalPartition.classFullName,
            federationGuid: constPartitionFedGuid,
            parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
          });
          const modId = txn.insertModel({
            classFullName: PhysicalModel.classFullName,
            modeledElement: { id: partId },
            isPrivate: true,
          });
          return {
            originalSubjectId: subjId,
            originalPartitionId: partId,
            originalModelId: modId,
          };
        });
      await sourceDb.pushChanges({ description: "inserted elements & models" });

      const initialTargetEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: initialTargetEditTxn,
      });
      await withTransformerLifecycle(transformer, [initialTargetEditTxn]);
      await targetDb.pushChanges({ description: "initial transformation" });

      const originalTargetElement = targetDb.elements.getElement<Subject>(
        { federationGuid: constSubjectFedGuid },
        Subject
      );
      expect(originalTargetElement?.userLabel).to.equal("A");
      const originalTargetPartition =
        targetDb.elements.getElement<PhysicalPartition>(
          { federationGuid: constPartitionFedGuid },
          PhysicalPartition
        );
      expect(originalTargetPartition.code.value).to.be.equal(
        "original partition"
      );
      const originalTargetModel = targetDb.models.getModel<PhysicalModel>(
        originalTargetPartition.id,
        PhysicalModel
      );
      expect(originalTargetModel.isPrivate).to.be.true;

      const {
        secondCopyOfSubjectId,
        recreatedPartitionId: _recreatedPartitionId,
      } = withEditTxn(sourceDb, "recreate elements & models", (txn) => {
        txn.deleteElement(originalSubjectId);
        const secondSubjId = txn.insertElement({
          classFullName: Subject.classFullName,
          code: Code.createEmpty(),
          model: IModel.repositoryModelId,
          parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
          federationGuid: constSubjectFedGuid,
          userLabel: "B",
        });

        txn.deleteModel(originalModelId);
        txn.deleteElement(originalPartitionId);
        const recPartId = txn.insertElement({
          model: IModel.repositoryModelId,
          code: PhysicalPartition.createCode(
            sourceDb,
            IModel.rootSubjectId,
            "recreated partition"
          ),
          classFullName: PhysicalPartition.classFullName,
          federationGuid: constPartitionFedGuid,
          parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
        });
        txn.insertModel({
          classFullName: PhysicalModel.classFullName,
          modeledElement: { id: recPartId },
          isPrivate: false,
        });
        return {
          secondCopyOfSubjectId: secondSubjId,
          recreatedPartitionId: recPartId,
        };
      });
      await sourceDb.pushChanges({
        description: "recreated elements & models",
      });

      const changeTargetEditTxn1 = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: changeTargetEditTxn1 },
        { argsForProcessChanges: { startChangeset: sourceDb.changeset } }
      );
      await transformer.process();
      changeTargetEditTxn1.end();
      await targetDb.pushChanges({
        description: "change processing transformation",
      });

      const targetElement = targetDb.elements.getElement<Subject>(
        { federationGuid: constSubjectFedGuid },
        Subject
      );
      expect(targetElement?.userLabel).to.equal("B");
      const targetPartition = targetDb.elements.getElement<PhysicalPartition>(
        { federationGuid: constPartitionFedGuid },
        PhysicalPartition
      );
      expect(targetPartition.code.value).to.be.equal("recreated partition");
      const targetModel = targetDb.models.getModel<PhysicalModel>(
        targetPartition.id,
        PhysicalModel
      );
      expect(targetModel.isPrivate).to.be.false;

      expect(
        count(
          sourceDb,
          Subject.classFullName,
          `Parent.Id = ${IModel.rootSubjectId}`
        )
      ).to.equal(1);
      expect(
        count(
          targetDb,
          Subject.classFullName,
          `Parent.Id = ${IModel.rootSubjectId}`
        )
      ).to.equal(1);
      expect(count(sourceDb, PhysicalPartition.classFullName)).to.equal(1);
      expect(count(targetDb, PhysicalPartition.classFullName)).to.equal(1);
      expect(count(sourceDb, PhysicalModel.classFullName)).to.equal(1);
      expect(count(targetDb, PhysicalModel.classFullName)).to.equal(1);

      withEditTxn(sourceDb, "delete second copy of subject", (txn) => {
        txn.deleteElement(secondCopyOfSubjectId);
      });
      await sourceDb.pushChanges({
        description: "deleted the second copy of the subject",
      });
      const startChangeset = sourceDb.changeset;
      // readd the subject in a separate changeset
      withEditTxn(sourceDb, "insert third copy of subject", (txn) => {
        txn.insertElement({
          classFullName: Subject.classFullName,
          code: Code.createEmpty(),
          model: IModel.repositoryModelId,
          parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
          federationGuid: constSubjectFedGuid,
          userLabel: "C",
        });
      });
      await sourceDb.pushChanges({
        description: "inserted a third copy of the subject with userLabel C",
      });

      const changeTargetEditTxn2 = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: changeTargetEditTxn2 },
        { argsForProcessChanges: { startChangeset } }
      );
      await transformer.process();
      changeTargetEditTxn2.end();
      await targetDb.pushChanges({ description: "transformation" });

      const thirdCopySubject = targetDb.elements.getElement<Subject>(
        { federationGuid: constSubjectFedGuid },
        Subject
      );
      expect(thirdCopySubject?.userLabel).to.equal("C");
    } finally {
      try {
        // delete iModel briefcases
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: sourceIModelId,
        });
        await transformerTestHub.deleteIModel({
          iTwinId,
          iModelId: targetIModelId,
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log("can't destroy", err);
      }
    }
  });
});
