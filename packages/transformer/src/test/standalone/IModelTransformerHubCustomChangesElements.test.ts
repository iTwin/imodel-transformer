/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect, vi } from "vitest";

import {
  BriefcaseDb,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  PhysicalModel,
  PhysicalPartition,
  SpatialCategory,
  Subject,
  SubjectOwnsPartitionElements,
  SubjectOwnsSubjects,
  withEditTxn,
} from "@itwin/core-backend";

import { AccessToken, Guid, GuidString, Id64 } from "@itwin/core-bentley";
import { Code, IModel } from "@itwin/core-common";

import { withTransformerLifecycle } from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import {
  assertModelExistsByName,
  CustomChangesTransformer,
  insertPhysicalElement,
} from "../TestUtils/HubCustomChangesTestUtils";

import {
  closeAndDeleteHubBriefcase,
  prepareHubBriefcase,
  registerHubTestContext,
} from "../TestUtils/HubTestContext";

describe("IModelTransformerHub - custom changes", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  registerHubTestContext(
    "IModelTransformerHubCustomChangesElements",
    (context) => {
      iTwinId = context.iTwinId;
      accessToken = context.accessToken;
    }
  );

  describe("addCustomChanges", () => {
    let sourceDb: BriefcaseDb;
    let targetDb: BriefcaseDb;

    beforeEach(async () => {
      sourceDb = await prepareHubBriefcase(accessToken, iTwinId, "source");
      targetDb = await prepareHubBriefcase(accessToken, iTwinId, "target");
    });

    afterEach(async () => {
      await closeAndDeleteHubBriefcase(accessToken, iTwinId, sourceDb);
      await closeAndDeleteHubBriefcase(accessToken, iTwinId, targetDb);
    });
    it("should reset element values when custom changes to update element are added", async function () {
      // Arrange
      const { categoryId1, physicalModel1Id, physicalModel2Id } = withEditTxn(
        sourceDb,
        "insert reset-test source data",
        (txn) => {
          const subjectId = Subject.insert(txn, IModel.rootSubjectId, "S1");
          return {
            categoryId1: SpatialCategory.insert(
              txn,
              IModel.dictionaryId,
              "C1",
              {}
            ),
            physicalModel1Id: PhysicalModel.insert(txn, subjectId, "PM1"),
            physicalModel2Id: PhysicalModel.insert(txn, subjectId, "PM2"),
          };
        }
      );
      const physicalElem1 = insertPhysicalElement(
        sourceDb,
        physicalModel1Id,
        categoryId1,
        "PhysicalOne"
      );
      const physicalElem2 = insertPhysicalElement(
        sourceDb,
        physicalModel2Id,
        categoryId1,
        "PhysicalTwo"
      );
      await sourceDb.pushChanges({
        description: "Initial changes",
        retainLocks: true,
      });

      // === Transformation 1: Run `process all` transformation ===
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      await withTransformerLifecycle(
        transformer,
        [transformer.editTxn],
        async () => {
          await transformer.process();
          await transformer.updateSynchronizationVersion({
            initializeReverseSyncVersion: true,
          });
        }
      );
      await targetDb.pushChanges({
        description: "Transformation 1: Process All",
        retainLocks: true,
      });

      // === Transformation 2: `process changes` transformation to update other element  ===
      // Update element in target
      const physicalElem1InTargetProps = targetDb.elements.getElementProps(
        physicalElem1.federationGuid!
      );
      physicalElem1InTargetProps.userLabel = "Updated";
      withEditTxn(targetDb, "update target element", (txn) => {
        txn.updateElement(physicalElem1InTargetProps);
      });

      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomElementChange(
            "Updated",
            physicalElem2.id!
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 2: update other element",
        retainLocks: true,
      });

      let physicalElem1InTarget = targetDb.elements.tryGetElement(
        physicalElem1.federationGuid!
      );
      expect(physicalElem1InTarget).to.not.be.undefined;
      expect(physicalElem1InTarget!.userLabel).to.be.equal("Updated");

      // === Transformation 3: `process changes` transformation to update changed element  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomElementChange(
            "Updated",
            physicalElem1.id!
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 2: update changed element",
        retainLocks: true,
      });

      physicalElem1InTarget = targetDb.elements.tryGetElement(
        physicalElem1.federationGuid!
      );
      expect(physicalElem1InTarget).to.not.be.undefined;
      expect(
        physicalElem1InTarget!.userLabel,
        "updated value should be reverted"
      ).to.be.equal("PhysicalOne");
    });

    it("should delete recreated model when custom delete change is registered for it", async () => {
      const constSubjectFedGuid = Guid.createValue();
      const constPartitionFedGuid = Guid.createValue();
      const { originalSubjectId, originalPartitionId, originalModelId } =
        withEditTxn(sourceDb, "insert original elements and model", (txn) => {
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
          return {
            originalSubjectId: subjId,
            originalPartitionId: partId,
            originalModelId: txn.insertModel({
              classFullName: PhysicalModel.classFullName,
              modeledElement: { id: partId },
              isPrivate: true,
            }),
          };
        });

      await sourceDb.pushChanges({
        description: "Initial changes",
        retainLocks: true,
      });

      // === Transformation 1: Run `process all` transformation ===
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      await withTransformerLifecycle(
        transformer,
        [transformer.editTxn],
        async () => {
          await transformer.process();
          await transformer.updateSynchronizationVersion({
            initializeReverseSyncVersion: true,
          });
        }
      );
      await targetDb.pushChanges({
        description: "Transformation 1: Process All",
        retainLocks: true,
      });

      // Assert
      expect(targetDb.elements.tryGetElement(constSubjectFedGuid)).to.not.be
        .undefined;
      expect(targetDb.elements.tryGetElement(constPartitionFedGuid)).to.not.be
        .undefined;
      expect(
        IModelTestUtils.count(targetDb, PhysicalModel.classFullName)
      ).to.be.equal(1);
      assertModelExistsByName(targetDb, ["original partition"]);

      // === Transformation 1: Run `process all` transformation ===
      const { secondCopyOfSubjectId, recreatedPartitionId } = withEditTxn(
        sourceDb,
        "recreate elements and model",
        (txn) => {
          txn.deleteElement(originalSubjectId);
          const newSubjectId = txn.insertElement({
            classFullName: Subject.classFullName,
            code: Code.createEmpty(),
            model: IModel.repositoryModelId,
            parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
            federationGuid: constSubjectFedGuid,
            userLabel: "B",
          });

          txn.deleteModel(originalModelId);
          txn.deleteElement(originalPartitionId);
          const newPartitionId = txn.insertElement({
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
            modeledElement: { id: newPartitionId },
            isPrivate: false,
          });
          return {
            secondCopyOfSubjectId: newSubjectId,
            recreatedPartitionId: newPartitionId,
          };
        }
      );

      await sourceDb.pushChanges({
        description: "Recreated elements",
        retainLocks: true,
      });

      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          await sourceDbChanges.addCustomModelChange(
            "Deleted",
            recreatedPartitionId
          );
          await sourceDbChanges.addCustomElementChange(
            "Deleted",
            secondCopyOfSubjectId
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 2: inserted previously excluded model",
        retainLocks: true,
      });
      expect(targetDb.elements.tryGetElement(constSubjectFedGuid)).to.be
        .undefined;
      expect(targetDb.elements.tryGetElement(constPartitionFedGuid)).to.be
        .undefined;
      expect(
        IModelTestUtils.count(targetDb, PhysicalModel.classFullName)
      ).to.be.equal(0);
    });

    it("should handle custom changes when source iModel has no changesets", async () => {
      // set up source
      const subjectFedGuid1 = Guid.createValue();
      const subjectFedGuid2 = Guid.createValue();
      const { originalSubjectId1, originalSubjectId2 } = withEditTxn(
        sourceDb,
        "insert initial subjects",
        (txn) => ({
          originalSubjectId1: txn.insertElement({
            classFullName: Subject.classFullName,
            code: Code.createEmpty(),
            model: IModel.repositoryModelId,
            parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
            federationGuid: subjectFedGuid1,
            userLabel: "A",
          }),
          originalSubjectId2: txn.insertElement({
            classFullName: Subject.classFullName,
            code: Code.createEmpty(),
            model: IModel.repositoryModelId,
            parent: new SubjectOwnsSubjects(IModel.rootSubjectId),
            federationGuid: subjectFedGuid2,
            userLabel: "B",
          }),
        })
      );

      // process all
      const transformer1 = new CustomChangesTransformer(
        sourceDb,
        targetDb,
        false
      );
      transformer1.exporter.excludeElement(originalSubjectId2);
      await withTransformerLifecycle(transformer1, [transformer1.editTxn]);
      await targetDb.pushChanges({
        description: "target changes for process all transformation.",
        retainLocks: true,
      });
      expect(targetDb.elements.tryGetElement(subjectFedGuid1)).to.not.be
        .undefined;
      expect(targetDb.elements.tryGetElement(subjectFedGuid2)).to.be.undefined;

      // process changes
      const transformer2 = new CustomChangesTransformer(
        sourceDb,
        targetDb,
        true
      );
      const addChangesStub = vi
        .spyOn(transformer2, "addCustomChanges")
        .mockImplementation(async (sourceDbChanges) => {
          // Assert that element mapping is set
          const targetId =
            transformer2.context.findTargetElementId(originalSubjectId1);
          expect(
            targetId,
            "addCustomChanges should be called only after elements are mapped in clone context"
          ).to.not.be.equal(Id64.invalid);
          await sourceDbChanges.addCustomElementChange(
            "Deleted",
            originalSubjectId1
          );
          await sourceDbChanges.addCustomElementChange(
            "Inserted",
            originalSubjectId2
          );
        });
      await withTransformerLifecycle(transformer2, [transformer2.editTxn]);
      await targetDb.pushChanges({
        description: "target changes for process changes transformation.",
        retainLocks: true,
      });
      expect(addChangesStub.mock.calls).to.have.lengthOf(1);
      expect(targetDb.elements.tryGetElement(subjectFedGuid1)).to.be.undefined;
      expect(targetDb.elements.tryGetElement(subjectFedGuid2)).to.not.be
        .undefined;
    });
  });
});
