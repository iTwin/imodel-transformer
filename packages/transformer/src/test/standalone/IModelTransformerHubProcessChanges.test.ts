/* eslint-disable @typescript-eslint/dot-notation */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect, vi } from "vitest";

import {
  BriefcaseDb,
  ChangesetReader,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementOwnsChildElements,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  PhysicalType,
  PropertyFilter,
  SpatialCategory,
  Subject,
  SubjectOwnsPartitionElements,
  withEditTxn,
} from "@itwin/core-backend";

import { AccessToken, GuidString, Id64, Id64String } from "@itwin/core-bentley";
import {
  BisCodeSpec,
  Code,
  GeometricElementProps,
  IModel,
  InformationPartitionElementProps,
  PhysicalElementProps,
} from "@itwin/core-common";

import {
  IModelTransformer,
  IModelTransformerError,
} from "../../imodel-transformer";

import {
  createStartedEditTxn,
  expectTransformerError,
  IModelTransformerTestUtils,
  withTransformerLifecycle,
} from "../IModelTransformerUtils";

import { IModelTestUtils } from "../TestUtils/IModelTestUtils";

import {
  closeAndDeleteHubBriefcase,
  prepareHubBriefcase,
  registerHubTestContext,
} from "../TestUtils/HubTestContext";

describe("IModelTransformerHub - process changes", () => {
  let iTwinId: GuidString;
  let accessToken: AccessToken;

  registerHubTestContext("IModelTransformerHubProcessChanges", (context) => {
    iTwinId = context.iTwinId;
    accessToken = context.accessToken;
  });

  describe("processChanges", () => {
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

    it("identifies a relationship deletion missing an endpoint", async () => {
      const editTxn = createStartedEditTxn(targetDb);
      const transformer = new IModelTransformer({
        source: sourceDb,
        target: editTxn,
      });
      await withTransformerLifecycle(transformer, [editTxn], async () => {
        await expectTransformerError(
          transformer["processDeletedOp"](
            {
              ecInstanceId: "0x123",
              ecClassId: "0x456",
            },
            new Map(),
            true,
            new Set<Id64String>(),
            new Set<Id64String>()
          ),
          IModelTransformerError.ChangedInstanceMetadataMissing,
          "Relationship deletion 0x123 is missing an endpoint."
        );
      });
    });

    it("should skip unchanged parent elements but still export changed child elements during processChanges", async () => {
      // Create a model with a parent element and a child element
      const { parentElementId, childElementId } = withEditTxn(
        sourceDb,
        "create model with parent and child elements",
        (txn) => {
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "TestPhysicalModel"
          );
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "TestCategory",
            {}
          );
          const parentId = txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: "ParentElement",
          } as GeometricElementProps);
          const childId = txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: "ChildElement",
            parent: new ElementOwnsChildElements(parentId),
          } as GeometricElementProps);
          return {
            physicalModelId: modelId,
            parentElementId: parentId,
            childElementId: childId,
          };
        }
      );
      await sourceDb.pushChanges({
        description: "Initial model and elements",
        retainLocks: true,
      });

      // Run initial processAll transformation
      const firstEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstEditTxn,
      });
      await withTransformerLifecycle(transformer, [firstEditTxn]);
      await targetDb.pushChanges({
        description: "Initial transformation",
        retainLocks: true,
      });

      // Update only the child element (not the parent) to trigger a change
      withEditTxn(sourceDb, "update child element only", (txn) => {
        const childProps = sourceDb.elements.getElementProps(childElementId);
        txn.updateElement({
          ...childProps,
          userLabel: "ChildElement-Updated",
        });
      });
      await sourceDb.pushChanges({
        description: "Child element update",
        retainLocks: true,
      });

      // Run processChanges and spy on onExportElement
      const secondEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondEditTxn },
        { argsForProcessChanges: {} }
      );
      const onExportElementSpy = vi.spyOn(transformer, "onExportElement");
      await withTransformerLifecycle(transformer, [secondEditTxn], async () => {
        await transformer.process();

        // Verify: parent element was NOT exported (short-circuited)
        const parentWasExported = onExportElementSpy.mock.calls.some(
          ([element]) => element.id === parentElementId
        );
        expect(
          parentWasExported,
          "onExportElement should not have been called for unchanged parent element"
        ).to.be.false;

        // Verify: child element WAS exported (still traversed through unchanged parent)
        const childWasExported = onExportElementSpy.mock.calls.some(
          ([element]) => element.id === childElementId
        );
        expect(
          childWasExported,
          "onExportElement should have been called for changed child element"
        ).to.be.true;
      });
    });

    it("should still export updated aspects when the owning element is unchanged during processChanges", async () => {
      // Import a schema with a custom UniqueAspect so we can test aspect-only updates
      // without interference from the provenance system
      const testSchemaPath =
        IModelTransformerTestUtils.getPathToSchemaWithUniqueAspect();
      await sourceDb.importSchemas([testSchemaPath]);
      await targetDb.importSchemas([testSchemaPath]);
      await sourceDb.pushChanges({
        description: "Import test schema",
        retainLocks: true,
      });
      await targetDb.pushChanges({
        description: "Import test schema",
        retainLocks: true,
      });

      // Create an element with a unique aspect
      const elementId = withEditTxn(
        sourceDb,
        "create element with unique aspect",
        (txn) => {
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "TestPhysicalModelForAspect"
          );
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "TestCategoryForAspect",
            {}
          );
          const elemId = txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: "ElementWithUniqueAspect",
          } as GeometricElementProps);
          txn.insertAspect({
            classFullName: "TestSchema1:MyUniqueAspect",
            element: { id: elemId },
            myProp1: "original-value",
          } as any);
          return elemId;
        }
      );
      await sourceDb.pushChanges({
        description: "Initial element with unique aspect",
        retainLocks: true,
      });

      // Run initial processAll transformation
      const firstEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstEditTxn,
      });
      await withTransformerLifecycle(transformer, [firstEditTxn]);
      await targetDb.pushChanges({
        description: "Initial transformation",
        retainLocks: true,
      });

      // Verify initial aspect value on target
      const targetElementId = IModelTestUtils.queryByUserLabel(
        targetDb,
        "ElementWithUniqueAspect"
      );
      const targetAspectsBefore = targetDb.elements.getAspects(
        targetElementId,
        "TestSchema1:MyUniqueAspect"
      );
      expect(targetAspectsBefore).to.have.lengthOf(1);
      expect((targetAspectsBefore[0] as any).myProp1).to.equal(
        "original-value"
      );

      // Update only the aspect (not the element directly)
      withEditTxn(sourceDb, "update unique aspect only", (txn) => {
        const aspects = sourceDb.elements.getAspects(
          elementId,
          "TestSchema1:MyUniqueAspect"
        );
        txn.updateAspect({
          ...aspects[0].toJSON(),
          myProp1: "updated-value",
        } as any);
      });
      await sourceDb.pushChanges({
        description: "Aspect-only update",
        retainLocks: true,
      });

      // Run processChanges — the aspect change should propagate to the target
      const secondEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondEditTxn },
        { argsForProcessChanges: {} }
      );
      await withTransformerLifecycle(transformer, [secondEditTxn]);

      // Verify: the aspect on the target element was updated
      const targetAspectsAfter = targetDb.elements.getAspects(
        targetElementId,
        "TestSchema1:MyUniqueAspect"
      );
      expect(targetAspectsAfter).to.have.lengthOf(1);
      expect(
        (targetAspectsAfter[0] as any).myProp1,
        "target aspect should have been updated to 'updated-value' by processChanges"
      ).to.equal("updated-value");
    });

    it("should process changes successfully when element is deleted after existing elements were expanded into overflow table", async () => {
      // Import initial schema with property count that does not require overflow table
      const initialSchema = generateSchema(1, "SourceProperty", 5);
      await sourceDb.importSchemaStrings([initialSchema]);
      const elementId = createPhysicalElement(
        sourceDb,
        "DynamicTestSchema:DynamicPhysicalElement"
      );
      await sourceDb.pushChanges({
        description: "Initial schema and element creation",
        retainLocks: true,
      });

      // === Transformation 1: Run `process all` transformation ===
      const firstTransformEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstTransformEditTxn,
      });
      await withTransformerLifecycle(
        transformer,
        [firstTransformEditTxn],
        async () => {
          await transformer.processSchemas();
          await transformer.process();
        }
      );
      await targetDb.pushChanges({
        description: "Transformation 1: Process All",
        retainLocks: true,
      });

      // Assert that element was transformed
      const targetElement = IModelTestUtils.queryByUserLabel(
        targetDb,
        "TestClassElement"
      );
      expect(targetElement).to.not.equal(Id64.invalid);

      // Update schema: Add enough properties to spill into overflow table (more than 32)
      const expandedSchema = generateSchema(2, "SourceProperty", 100);
      await sourceDb.importSchemaStrings([expandedSchema]);
      await sourceDb.pushChanges({
        description: "Updated schema",
        retainLocks: true,
      });

      // Delete the element
      withEditTxn(sourceDb, "recreate elements & models", (txn) => {
        txn.deleteElement(elementId);
      });
      await sourceDb.pushChanges({
        description: "Deleted element",
        retainLocks: true,
      });

      // === Transformation 2: Run `process changes` transformation ===
      const secondTransformEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondTransformEditTxn },
        { argsForProcessChanges: {} }
      );
      await transformer.processSchemas();
      const openFileSpy = vi.spyOn(ChangesetReader, "openFile");
      try {
        await withTransformerLifecycle(
          transformer,
          [secondTransformEditTxn],
          async () => {
            await transformer.process();

            const selectedChangesetPaths = transformer["_csFileProps"]!.map(
              (csFile) => csFile.pathname
            );
            expect(openFileSpy).toHaveBeenCalledTimes(
              selectedChangesetPaths.length
            );
            expect(
              openFileSpy.mock.calls.map(([args]) => args.fileName)
            ).to.deep.equal(selectedChangesetPaths);
            expect(
              openFileSpy.mock.calls.map(([args]) => args.propFilter)
            ).to.deep.equal(
              selectedChangesetPaths.map(() => PropertyFilter.BisCoreElement)
            );
          }
        );
        await targetDb.pushChanges({
          description: "Transformation 2: Process Changes with deletion",
          retainLocks: true,
        });
      } finally {
        openFileSpy.mockRestore();
      }

      // Assert: Verify element is deleted in target
      const targetElement2 = IModelTestUtils.queryByUserLabel(
        targetDb,
        "TestClassElement"
      );
      expect(
        targetElement2,
        "Element should be deleted in target iModel"
      ).to.equal(Id64.invalid);
    });

    it("should leave model contents correct when model partition was recreated with different federation guid and the same code value", async () => {
      // Arrange
      const specId = sourceDb.codeSpecs.getByName(
        BisCodeSpec.physicalMaterial
      ).id;
      const { subjectId, physicalModelId, categoryId, physicalObjectId } =
        withEditTxn(sourceDb, "recreate elements & models", (txn) => {
          // prepare source - create initial subject, model, and element
          const subjId = Subject.insert(txn, IModel.rootSubjectId, "Subject1");
          const physModId = PhysicalModel.insert(txn, subjId, "PhysicalModel");
          const catId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "C1",
            {}
          );
          const physicalObjectProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: physModId,
            category: catId,
            code: new Code({
              value: "PO1",
              scope: IModel.rootSubjectId,
              spec: specId,
            }),
          };
          const physicalObjId = txn.insertElement(physicalObjectProps);
          return {
            subjectId: subjId,
            physicalModelId: physModId,
            categoryId: catId,
            physicalObjectId: physicalObjId,
          };
        });
      await sourceDb.pushChanges({
        accessToken,
        description: "First changes",
        retainLocks: true,
      });

      // Run first transform
      const firstTransformEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstTransformEditTxn,
      });
      await withTransformerLifecycle(transformer, [firstTransformEditTxn]);
      await targetDb.pushChanges({
        accessToken,
        description: "First transformation",
        retainLocks: true,
      });

      // Recreate source model partition with different federation guid
      withEditTxn(sourceDb, "delete and recreate model", (txn) => {
        txn.deleteElement(physicalObjectId);
        txn.deleteModel(physicalModelId);
        txn.deleteElement(physicalModelId);
        const physicalModel2Id = PhysicalModel.insert(
          txn,
          subjectId,
          "PhysicalModel"
        );
        const physicalObject2Props: PhysicalElementProps = {
          classFullName: PhysicalObject.classFullName,
          model: physicalModel2Id,
          category: categoryId,
          code: new Code({
            value: "PO2",
            scope: IModel.rootSubjectId,
            spec: specId,
          }),
        };
        txn.insertElement(physicalObject2Props);
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "Second changes",
      });

      // Act - run second transform with change processing
      const secondTransformEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondTransformEditTxn },
        {
          argsForProcessChanges: {},
        }
      );
      await withTransformerLifecycle(transformer, [secondTransformEditTxn]);
      await targetDb.pushChanges({
        accessToken,
        description: "Second transformation",
      });

      // Assert - verify that new elements and models exist with correct values
      expect(
        IModelTransformerTestUtils.queryByCodeValue(targetDb, "PO2")
      ).to.not.be.equal(Id64.invalid);
      expect(
        IModelTestUtils.queryModelIddByModeledElementCodeValue(
          targetDb,
          "PhysicalModel"
        )
      ).to.not.be.equal(Id64.invalid);
    });

    it("should delete model when model partition was recreated with different federation guid and the same code value but model was left deleted", async () => {
      // Arrange
      const specId = sourceDb.codeSpecs.getByName(
        BisCodeSpec.physicalMaterial
      ).id;
      const { subjectId, physicalModelId, physicalObjectId } = withEditTxn(
        sourceDb,
        "recreate elements & models",
        (txn) => {
          // prepare source - create initial subject, model, and element
          const subjId = Subject.insert(txn, IModel.rootSubjectId, "Subject1");
          const physModId = PhysicalModel.insert(txn, subjId, "PhysicalModel");
          const catId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "C1",
            {}
          );
          const physicalObjectProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: physModId,
            category: catId,
            code: new Code({
              value: "PO1",
              scope: IModel.rootSubjectId,
              spec: specId,
            }),
          };
          const physicalObjId = txn.insertElement(physicalObjectProps);
          return {
            subjectId: subjId,
            physicalModelId: physModId,
            physicalObjectId: physicalObjId,
          };
        }
      );
      await sourceDb.pushChanges({
        accessToken,
        description: "First changes",
        retainLocks: true,
      });

      // Run first transform
      const firstTransformEditTxn = createStartedEditTxn(targetDb);
      let transformer = new IModelTransformer({
        source: sourceDb,
        target: firstTransformEditTxn,
      });
      await withTransformerLifecycle(transformer, [firstTransformEditTxn]);
      await targetDb.pushChanges({
        accessToken,
        description: "First transformation",
        retainLocks: true,
      });

      // Recreate source model partition with different federation guid
      withEditTxn(sourceDb, "delete and recreate model", (txn) => {
        txn.deleteElement(physicalObjectId);
        txn.deleteModel(physicalModelId);
        txn.deleteElement(physicalModelId);
        const partitionProps: InformationPartitionElementProps = {
          classFullName: PhysicalPartition.classFullName,
          model: IModel.repositoryModelId,
          parent: new SubjectOwnsPartitionElements(subjectId),
          code: PhysicalPartition.createCode(
            txn.iModel,
            subjectId,
            "PhysicalModel"
          ),
        };
        txn.insertElement(partitionProps);
      });
      await sourceDb.pushChanges({
        accessToken,
        description: "Second changes",
      });

      // Act - run second transform with change processing
      const secondTransformEditTxn = createStartedEditTxn(targetDb);
      transformer = new IModelTransformer(
        { source: sourceDb, target: secondTransformEditTxn },
        {
          argsForProcessChanges: {},
        }
      );
      await withTransformerLifecycle(transformer, [secondTransformEditTxn]);
      await targetDb.pushChanges({
        accessToken,
        description: "Second transformation",
      });

      // Assert - verify that new elements and models exist with correct values
      expect(
        IModelTransformerTestUtils.queryByCodeValue(targetDb, "PhysicalModel")
      ).to.not.be.equal(Id64.invalid);
      expect(
        IModelTestUtils.queryModelIddByModeledElementCodeValue(
          targetDb,
          "PhysicalModel"
        )
      ).to.be.equal(Id64.invalid);
    });

    function generateSchema(
      schemaVersion: number,
      propertySuffix: string,
      propertyCount: number
    ): string {
      const schemaName = "DynamicTestSchema";
      const properties = Array.from(
        { length: propertyCount },
        (_, index) =>
          `                <ECProperty propertyName="${propertySuffix}${index + 1}" typeName="string"/>`
      ).join("\n");
      const sourceSchema = `<?xml version="1.0" encoding="UTF-8"?>
            <ECSchema schemaName="${schemaName}" alias="DTS" version="0${schemaVersion}.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
                <ECSchemaReference name="CoreCustomAttributes" version="01.00.03" alias="CoreCA"/>
                <ECSchemaReference name="BisCore" version="01.00.16" alias="bis"/>
                <ECCustomAttributes>
                    <DynamicSchema xmlns="CoreCustomAttributes.01.00.03"/>
                </ECCustomAttributes>
                <ECEntityClass typeName="DynamicPhysicalElement" modifier="Sealed">
                    <BaseClass>bis:PhysicalElement</BaseClass>
                    ${properties}
                </ECEntityClass>
            </ECSchema>`;
      return sourceSchema;
    }

    function createPhysicalElement(
      db: IModelDb,
      classFullName: string
    ): Id64String {
      return withEditTxn(db, "recreate elements & models", (txn) => {
        const sourcePhysicalModelId = PhysicalModel.insert(
          txn,
          IModelDb.rootSubjectId,
          "SourcePhysicalModel"
        );
        const sourceCategoryId = SpatialCategory.insert(
          txn,
          IModelDb.dictionaryId,
          "SourceCategory",
          {}
        );
        return txn.insertElement({
          classFullName,
          model: sourcePhysicalModelId,
          category: sourceCategoryId,
          code: PhysicalType.createCode(
            db,
            sourcePhysicalModelId,
            "TestClassElement"
          ),
          userLabel: "TestClassElement",
          SourceProperty1: "value1",
        } as GeometricElementProps);
      });
    }
  });
});
