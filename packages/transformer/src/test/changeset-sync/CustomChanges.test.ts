/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect, vi } from "vitest";
import {
  BriefcaseDb,
  DocumentListModel,
  Drawing,
  DrawingModel,
  ElementGroupsMembers,
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  GeometricModel,
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
import { Guid, GuidString, Id64, Id64String } from "@itwin/core-bentley";
import {
  Code,
  ElementProps,
  ExternalSourceAspectProps,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";
import {
  ChangedInstanceIds,
  IModelExporter,
  IModelTransformer,
  IModelTransformOptions,
} from "../../imodel-transformer.js";
import { createStartedEditTxn } from "../IModelTransformerUtils.js";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils.js";

import { useTransformerHub } from "../TestUtils/TransformerHubFixture.js";

/** Changes registered through addCustomChanges. */
describe("Custom changes", () => {
  const hub = useTransformerHub("CustomChanges");
  const { prepareBriefcase, closeAndDeleteBriefcase } = hub;

  describe("addCustomChanges", () => {
    let sourceDb: BriefcaseDb;
    let targetDb: BriefcaseDb;

    beforeEach(async () => {
      sourceDb = await prepareBriefcase("source");
      targetDb = await prepareBriefcase("target");
    });

    afterEach(async () => {
      await closeAndDeleteBriefcase(sourceDb);
      await closeAndDeleteBriefcase(targetDb);
    });
    class CustomChangesTransformer extends IModelTransformer {
      public readonly editTxn: ReturnType<typeof createStartedEditTxn>;

      constructor(
        source: IModelDb,
        target: IModelDb,
        isChangeProcessing: boolean
      ) {
        const editTxn = createStartedEditTxn(target);
        const options: IModelTransformOptions = {
          includeSourceProvenance: true,
        };
        if (isChangeProcessing) {
          options.argsForProcessChanges = {};
        }
        const exporter = new IModelExporter(source);
        super({ source: exporter, target: editTxn }, options);
        this.editTxn = editTxn;
      }

      public override async addCustomChanges(
        _sourceDbChanges: ChangedInstanceIds
      ) {}
    }

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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      await transformer.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      transformer.editTxn.end();
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      transformer.editTxn.end();
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
      assertModelDoesNotExistsByName(targetDb, ["PM1", "DrawingParent"]);
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
      await transformer.process();
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 5: delete model with newly added elements",
        retainLocks: true,
      });
      // Assert
      expect(
        IModelTestUtils.count(targetDb, GeometricModel.classFullName)
      ).to.be.equal(0);
      assertModelDoesNotExistsByName(targetDb, ["PM2"]);
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
      await transformer.process();
      await transformer.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      transformer.editTxn.end();
      await targetDb.pushChanges({
        description: "Transformation 1: Process All",
        retainLocks: true,
      });

      assertModelExistsByName(targetDb, ["DL", "ParentDrawing2"]);
      assertModelDoesNotExistsByName(targetDb, [
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
      await transformer.process();
      transformer.editTxn.end();
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
      assertModelDoesNotExistsByName(targetDb, ["ChildDrawing2"]);
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      await transformer.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      transformer.editTxn.end();
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
      assertModelDoesNotExistsByName(targetDb, ["PM2"]);
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      await transformer.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      transformer.editTxn.end();
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer.process();
      await transformer.updateSynchronizationVersion({
        initializeReverseSyncVersion: true,
      });
      transformer.editTxn.end();
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
      await transformer.process();
      transformer.editTxn.end();
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
      await transformer1.process();
      transformer1.editTxn.end();
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
      await transformer2.process();
      transformer2.editTxn.end();
      await targetDb.pushChanges({
        description: "target changes for process changes transformation.",
        retainLocks: true,
      });
      expect(addChangesStub.mock.calls).to.have.lengthOf(1);
      expect(targetDb.elements.tryGetElement(subjectFedGuid1)).to.be.undefined;
      expect(targetDb.elements.tryGetElement(subjectFedGuid2)).to.not.be
        .undefined;
    });

    function insertDrawingElement(
      iModel: IModelDb,
      documentListModelId: Id64String,
      drawingName: string
    ): ElementProps {
      const id = withEditTxn(iModel, `insert drawing ${drawingName}`, (txn) =>
        Drawing.insert(txn, documentListModelId, drawingName)
      );
      return iModel.elements.getElementProps(id);
    }

    function insertPhysicalElement(
      iModel: IModelDb,
      modelId: Id64String,
      categoryId: Id64String,
      uniqueName: string
    ): ElementProps {
      const code = new Code({ scope: "0x1", spec: "0x1", value: uniqueName });
      const element: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code,
        userLabel: uniqueName,
      };

      const id = withEditTxn(
        iModel,
        `insert physical element ${uniqueName}`,
        (txn) => txn.insertElement(element)
      );
      // re-read element to populate federationGuid value
      return iModel.elements.getElementProps(id);
    }

    function insertElementAspect(
      iModel: IModelDb,
      scopeId: Id64String,
      elementId: Id64String,
      identifier: string
    ): Id64String {
      const aspectProps: ExternalSourceAspectProps = {
        classFullName: ExternalSourceAspect.classFullName,
        kind: "something",
        scope: { id: scopeId },
        element: {
          id: elementId,
          relClassName: ElementOwnsExternalSourceAspects.classFullName,
        },
        identifier,
      };

      return withEditTxn(iModel, `insert aspect ${identifier}`, (txn) =>
        txn.insertAspect(aspectProps)
      );
    }

    function insertElementGroupsElementsRelationship(
      iModel: IModelDb,
      sourceId: Id64String,
      targetId: Id64String
    ) {
      const rel = ElementGroupsMembers.create(iModel, sourceId, targetId, 0);
      const id = withEditTxn(
        iModel,
        "insert element groups relationship",
        (txn) => txn.insertRelationship(rel.toJSON())
      );
      return iModel.relationships.getInstance(
        ElementGroupsMembers.classFullName,
        id
      );
    }

    function assertElementsExistByCode(
      iModel: IModelDb,
      properties: ElementProps[]
    ) {
      properties.forEach((elemProp) => {
        expect(elemProp.code.value).to.not.be.undefined;
        expect(
          IModelTestUtils.queryByCodeValue(iModel, elemProp.code.value!),
          `Element '${elemProp.code.value}' should exist in iModel.`
        ).to.not.be.equal(Id64.invalid);
      });
    }

    function assertModelExistsByName(iModel: IModelDb, names: string[]) {
      names.forEach((name) => {
        expect(
          IModelTestUtils.queryModelIddByModeledElementCodeValue(iModel, name),
          `Model '${name}' should exist in iModel.`
        ).to.not.be.equal(Id64.invalid);
      });
    }

    function assertModelDoesNotExistsByName(iModel: IModelDb, names: string[]) {
      names.forEach((name) => {
        expect(
          IModelTestUtils.queryModelIddByModeledElementCodeValue(iModel, name),
          `Model '${name}' should not exist in iModel.`
        ).to.be.equal(Id64.invalid);
      });
    }

    function assertElementsDoNotExistByCode(
      iModel: IModelDb,
      properties: ElementProps[]
    ) {
      properties.forEach((elemProp) => {
        expect(elemProp.code.value).to.not.be.undefined;
        expect(
          IModelTestUtils.queryByCodeValue(iModel, elemProp.code.value!),
          `Element '${elemProp.code.value}' should not exist in iModel.`
        ).to.be.equal(Id64.invalid);
      });
    }

    function assertElementHasExpectedAspectCount(
      iModel: IModelDb,
      federationGuid: GuidString,
      expectedAspectCount: number
    ) {
      const element = iModel.elements.tryGetElement(federationGuid);
      expect(
        element,
        `Could not locate element with federationGuid: ${federationGuid}`
      ).to.not.be.undefined;
      expect(iModel.elements.getAspects(element!.id).length).to.be.equal(
        expectedAspectCount,
        "Aspect count is different than expected."
      );
    }
  });
});
