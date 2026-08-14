/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect, vi } from "vitest";

import {
  BriefcaseDb,
  DocumentListModel,
  DrawingModel,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementGroupsMembers,
  GeometricModel,
  PhysicalModel,
  SpatialCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";

import { AccessToken, GuidString, Id64 } from "@itwin/core-bentley";
import { IModel } from "@itwin/core-common";

import { withTransformerLifecycle } from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import {
  assertElementHasExpectedAspectCount,
  assertElementsDoNotExistByCode,
  assertElementsExistByCode,
  assertModelDoesNotExistByName,
  assertModelExistsByName,
  CustomChangesTransformer,
  insertDrawingElement,
  insertElementAspect,
  insertElementGroupsElementsRelationship,
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
    "IModelTransformerHubCustomChangesModels",
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
    it("should call addCustomChanges when processing changes after source and target id map is populated", async () => {
      // set up source
      const sourceModelId0 = withEditTxn(
        sourceDb,
        "insert source model",
        (txn) => PhysicalModel.insert(txn, IModel.rootSubjectId, "M0")
      );
      await sourceDb.pushChanges({
        description: "Initial source data",
        retainLocks: true,
      });

      // process all
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      let addChangesStub = vi.spyOn(transformer, "addCustomChanges");
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "target changes for transformation 1",
        retainLocks: true,
      });
      expect(addChangesStub.mock.calls).to.have.lengthOf(0);

      // process changes
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      addChangesStub = vi
        .spyOn(transformer, "addCustomChanges")
        .mockImplementation(async (_sourceDbChanges) => {
          const targetId =
            transformer.context.findTargetElementId(sourceModelId0);
          expect(
            targetId,
            "addCustomChanges should be called only after elements are mapped in clone context"
          ).to.not.be.equal(Id64.invalid);
        });
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "target changes for transformation 2",
        retainLocks: true,
      });
      expect(addChangesStub.mock.calls).to.have.lengthOf(1);
    });

    it("should update data in target correctly when custom changes are registered for models", async () => {
      // Arrange
      const {
        sourceSubjectId,
        physicalModel1Id,
        categoryId1,
        documentListModel,
      } = withEditTxn(
        sourceDb,
        "insert source subject model and category",
        (txn) => {
          const subjectId = Subject.insert(txn, IModel.rootSubjectId, "S1");
          return {
            sourceSubjectId: subjectId,
            physicalModel1Id: PhysicalModel.insert(txn, subjectId, "PM1"),
            categoryId1: SpatialCategory.insert(
              txn,
              IModel.dictionaryId,
              "C1",
              {}
            ),
            documentListModel: DocumentListModel.insert(txn, subjectId, "DL"),
          };
        }
      );
      // Create Drawing model hierarchy
      const parentDrawing = insertDrawingElement(
        sourceDb,
        documentListModel,
        "DrawingParent"
      );
      const childDrawing = insertDrawingElement(
        sourceDb,
        parentDrawing.id!,
        "DrawingChild"
      );
      const physicalElem1 = insertPhysicalElement(
        sourceDb,
        physicalModel1Id,
        categoryId1,
        "PhysicalOne"
      );
      await sourceDb.pushChanges({
        description: "Initial changes",
        retainLocks: true,
      });

      // === Transformation 1: Run `process all` transformation ===
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      transformer.exporter.excludeElement(documentListModel);
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
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(1);
      expect(
        IModelTestUtils.count(targetDb, DrawingModel.classFullName)
      ).to.be.equal(0);
      expect(IModelTestUtils.queryByCodeValue(targetDb, "PM1")).to.not.be.equal(
        Id64.invalid
      );
      assertElementsExistByCode(targetDb, [physicalElem1]);
      assertElementsDoNotExistByCode(targetDb, [parentDrawing, childDrawing]);

      // === Transformation 2: `process changes` transformation to insert excluded parent model ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomModelChange(
            "Inserted",
            parentDrawing.id!
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 2: inserted previously excluded model",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(2);
      assertModelExistsByName(targetDb, ["PM1", "DL", "DrawingParent"]);
      expect(
        IModelTestUtils.count(targetDb, DrawingModel.classFullName)
      ).to.be.equal(1);
      assertElementsExistByCode(targetDb, [physicalElem1, parentDrawing]);
      assertElementsDoNotExistByCode(targetDb, [childDrawing]);

      // === Transformation 3: `process changes` transformation to include newly added model  ===
      // Act
      const physicalModel2Id = withEditTxn(
        sourceDb,
        "insert second physical model",
        (txn) => PhysicalModel.insert(txn, sourceSubjectId, "PM2")
      );
      const physicalElem2 = insertPhysicalElement(
        sourceDb,
        physicalModel2Id,
        categoryId1,
        "PhysicalTwo"
      );
      await sourceDb.pushChanges({
        description: "Added new physical model",
        retainLocks: true,
      });

      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          await sourceDbChanges.addCustomModelChange(
            "Inserted",
            physicalModel2Id
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 3: inserted newly created model",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(3);
      expect(
        IModelTestUtils.count(targetDb, DrawingModel.classFullName)
      ).to.be.equal(1);
      assertModelExistsByName(targetDb, ["PM1", "DL", "DrawingParent", "PM2"]);
      assertElementsExistByCode(targetDb, [
        physicalElem1,
        physicalElem2,
        parentDrawing,
      ]);
      assertElementsDoNotExistByCode(targetDb, [childDrawing]);

      // === Transformation 4: `process changes` transformation to delete existing model  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomModelChange(
            "Deleted",
            physicalModel1Id
          );
          await sourceDbChanges.addCustomModelChange(
            "Deleted",
            parentDrawing.id!
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 4: delete exported model",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(1);
      expect(
        IModelTestUtils.count(targetDb, DrawingModel.classFullName)
      ).to.be.equal(0);
      assertModelExistsByName(targetDb, ["DL", "PM2"]);
      assertModelDoesNotExistByName(targetDb, ["PM1", "DrawingParent"]);
      assertElementsExistByCode(targetDb, [physicalElem2]);
      assertElementsDoNotExistByCode(targetDb, [
        physicalElem1,
        parentDrawing,
        childDrawing,
      ]);
      // === Transformation 5: `process changes` transformation to delete existing model with newly added elements  ===
      const physicalElem3 = insertPhysicalElement(
        sourceDb,
        physicalModel2Id,
        categoryId1,
        "PhysicalThree"
      );
      await sourceDb.pushChanges({
        description: "Added new physical element into PM2",
        retainLocks: true,
      });
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          await sourceDbChanges.addCustomModelChange(
            "Deleted",
            physicalModel2Id
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 5: delete model with newly added elements",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(0);
      assertModelDoesNotExistByName(targetDb, ["PM2"]);
      assertElementsDoNotExistByCode(targetDb, [physicalElem2, physicalElem3]);
    });

    it("should update modeled element and its related data when custom changes are added for it's sub model", async function () {
      // === Transformation 1: Run `process all` transformation ===
      // Arrange
      const { sourceSubjectId, documentListModel } = withEditTxn(
        sourceDb,
        "insert source subject and document list",
        (txn) => {
          const subjectId = Subject.insert(txn, IModel.rootSubjectId, "S1");
          return {
            sourceSubjectId: subjectId,
            documentListModel: DocumentListModel.insert(txn, subjectId, "DL"),
          };
        }
      );
      const parentDrawing1 = insertDrawingElement(
        sourceDb,
        documentListModel,
        "ParentDrawing1"
      );
      const parentDrawing2 = insertDrawingElement(
        sourceDb,
        documentListModel,
        "ParentDrawing2"
      );
      const childDrawing1 = insertDrawingElement(
        sourceDb,
        parentDrawing1.id!,
        "ChildDrawing1"
      );
      const childDrawing2 = insertDrawingElement(
        sourceDb,
        parentDrawing1.id!,
        "ChildDrawing2"
      );
      insertElementAspect(
        sourceDb,
        sourceSubjectId,
        parentDrawing1.id!,
        "ParentAspect1"
      );
      insertElementAspect(
        sourceDb,
        sourceSubjectId,
        childDrawing1.id!,
        "TestAspect1"
      );
      insertElementAspect(
        sourceDb,
        sourceSubjectId,
        childDrawing2.id!,
        "TestAspect2"
      );
      insertElementGroupsElementsRelationship(
        sourceDb,
        parentDrawing1.id!,
        parentDrawing2.id!
      );

      insertElementGroupsElementsRelationship(
        sourceDb,
        childDrawing1.id!,
        childDrawing2.id!
      );
      await sourceDb.pushChanges({
        description: "Initial changes",
        retainLocks: true,
      });
      // Act
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      // Exclude all drawings
      transformer.exporter.excludeElement(parentDrawing1.id!);
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

      assertModelExistsByName(targetDb, ["DL", "ParentDrawing2"]);
      assertModelDoesNotExistByName(targetDb, [
        "ParentDrawing1",
        "ChildDrawing1",
        "ChildDrawing2",
      ]);
      assertElementsDoNotExistByCode(targetDb, [
        parentDrawing1,
        childDrawing1,
        childDrawing2,
      ]);

      // === Transformation 2: `process changes` transformation to include first child element's sub model  ===
      // Act
      // insert first child and keep excluding second child
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      transformer.exporter.excludeElement(childDrawing2.id!);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomModelChange(
            "Inserted",
            childDrawing1.id!
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description:
          "Transformation 2: add first previously excluded child element",
        retainLocks: true,
      });

      assertModelExistsByName(targetDb, [
        "DL",
        "ParentDrawing1",
        "ParentDrawing2",
        "ChildDrawing1",
      ]);
      assertModelDoesNotExistByName(targetDb, ["ChildDrawing2"]);
      assertElementsExistByCode(targetDb, [parentDrawing1, childDrawing1]);
      assertElementsDoNotExistByCode(targetDb, [childDrawing2]);
      assertElementHasExpectedAspectCount(
        targetDb,
        childDrawing1.federationGuid!,
        1
      );
      assertElementHasExpectedAspectCount(
        targetDb,
        parentDrawing1.federationGuid!,
        1
      );
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(1);

      // === Transformation 3: `process changes` transformation to include second child element's sub model  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomModelChange(
            "Inserted",
            childDrawing2.id!
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description:
          "Transformation 2: add second previously excluded child element",
        retainLocks: true,
      });
      // Assert
      assertModelExistsByName(targetDb, [
        "DL",
        "ParentDrawing1",
        "ParentDrawing2",
        "ChildDrawing1",
        "ChildDrawing2",
      ]);
      assertElementsExistByCode(targetDb, [
        parentDrawing1,
        childDrawing1,
        childDrawing2,
      ]);
      assertElementHasExpectedAspectCount(
        targetDb,
        childDrawing2.federationGuid!,
        1
      );
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(2);

      // === Transformation 4: `process changes` transformation to delete first child element's sub model  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomModelChange(
            "Deleted",
            childDrawing1.id!
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 3: delete first child element's submodel",
        retainLocks: true,
      });
      assertModelExistsByName(targetDb, [
        "DL",
        "ParentDrawing1",
        "ParentDrawing2",
        "ChildDrawing2",
      ]);
      assertElementsExistByCode(targetDb, [parentDrawing1, childDrawing2]);
      assertElementsDoNotExistByCode(targetDb, [childDrawing1]);
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(1);
    });

    it("should update exported data correctly when custom changes are registered for elements", async function () {
      // Prepare source
      const {
        sourceSubjectId,
        categoryId1,
        physicalModel1Id,
        physicalModel2Id,
      } = withEditTxn(sourceDb, "insert source models and category", (txn) => {
        const subjectId = Subject.insert(txn, IModel.rootSubjectId, "S1");
        return {
          sourceSubjectId: subjectId,
          categoryId1: SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "C1",
            {}
          ),
          physicalModel1Id: PhysicalModel.insert(txn, subjectId, "PM1"),
          physicalModel2Id: PhysicalModel.insert(txn, subjectId, "PM2"),
        };
      });
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
      insertElementAspect(
        sourceDb,
        sourceSubjectId,
        physicalElem1.id!,
        "TestAspect1"
      );
      insertElementAspect(
        sourceDb,
        sourceSubjectId,
        physicalElem2.id!,
        "TestAspect2"
      );
      insertElementGroupsElementsRelationship(
        sourceDb,
        physicalElem1.id!,
        physicalElem2.id!
      );
      await sourceDb.pushChanges({
        description: "Initial changes",
        retainLocks: true,
      });

      // === Transformation 1: Run `process all` transformation ===
      let transformer = new CustomChangesTransformer(sourceDb, targetDb, false);
      // will exclude 'PM2'
      transformer.exporter.excludeElement(physicalModel2Id);
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

      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(1);
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(0);
      assertModelExistsByName(targetDb, ["PM1"]);
      assertModelDoesNotExistByName(targetDb, ["PM2"]);
      assertElementsExistByCode(targetDb, [physicalElem1]);
      assertElementsDoNotExistByCode(targetDb, [physicalElem2]);
      assertElementHasExpectedAspectCount(
        targetDb,
        physicalElem1.federationGuid!,
        1
      );

      // === Transformation 2: `process changes` transformation to include excluded element  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomElementChange(
            "Inserted",
            physicalElem2.id!
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 2: include previously excluded element",
        retainLocks: true,
      });

      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(2);
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(1);
      assertModelExistsByName(targetDb, ["PM1", "PM2"]);
      assertElementsExistByCode(targetDb, [physicalElem1, physicalElem2]);
      assertElementHasExpectedAspectCount(
        targetDb,
        physicalElem2.federationGuid!,
        1
      );

      // === Transformation 3: `process changes` transformation to include newly added element  ===
      const physicalModel3Id = withEditTxn(
        sourceDb,
        "insert third physical model",
        (txn) => PhysicalModel.insert(txn, sourceSubjectId, "PM3")
      );
      const physicalElem3 = insertPhysicalElement(
        sourceDb,
        physicalModel3Id,
        categoryId1,
        "PhysicalThree"
      );
      await sourceDb.pushChanges({
        description: "Added new model and physical element",
        retainLocks: true,
      });

      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          await sourceDbChanges.addCustomElementChange(
            "Inserted",
            physicalElem3.id!
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 3: include newly added element",
        retainLocks: true,
      });

      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(3);
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(1);
      assertModelExistsByName(targetDb, ["PM1", "PM2", "PM3"]);
      assertElementsExistByCode(targetDb, [
        physicalElem1,
        physicalElem2,
        physicalElem3,
      ]);

      // === Transformation 4: `process changes` transformation to delete exported element  ===
      transformer = new CustomChangesTransformer(sourceDb, targetDb, true);
      vi.spyOn(transformer, "addCustomChanges").mockImplementation(
        async (sourceDbChanges) => {
          expect(
            sourceDbChanges.hasChanges,
            "there should be only custom changes"
          ).to.be.false;
          await sourceDbChanges.addCustomElementChange(
            "Deleted",
            physicalElem1.id!
          );
        }
      );
      await withTransformerLifecycle(transformer, [transformer.editTxn]);
      await targetDb.pushChanges({
        description: "Transformation 4: delete exported element",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(3);
      expect(
        IModelTestUtils.count(targetDb, ElementGroupsMembers.classFullName)
      ).to.be.equal(0);
      assertModelExistsByName(targetDb, ["PM1", "PM2", "PM3"]);
      assertElementsExistByCode(targetDb, [physicalElem2, physicalElem3]);
      assertElementsDoNotExistByCode(targetDb, [physicalElem1]);
    });
  });
});
