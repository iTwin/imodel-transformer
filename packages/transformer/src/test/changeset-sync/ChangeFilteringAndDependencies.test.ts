/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect, vi } from "vitest";
import {
  BriefcaseDb,
  EditTxn,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementOwnsChildElements,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  SubCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import {
  Code,
  GeometricElementProps,
  IModel,
  SubCategoryAppearance,
} from "@itwin/core-common";
import {
  IModelTransformer,
  IModelTransformerError,
  IModelTransformOptions,
} from "../../imodel-transformer.js";
import {
  createStartedEditTxn,
  expectTransformerError,
} from "../IModelTransformerUtils.js";

import { useTransformerHub } from "../TestUtils/TransformerHubFixture.js";

/** shouldExportElement decisions and unchanged dependencies during change processing. */
describe("Change filtering and dependencies", () => {
  const hub = useTransformerHub("ChangeFilteringAndDependencies");
  const { prepareBriefcase, closeAndDeleteBriefcase } = hub;

  describe("processChanges", () => {
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
      await transformer.process();
      transformer.dispose();
      firstEditTxn.end();
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

      transformer.dispose();
      secondEditTxn.end();
    });

    /** Rejects fixed elements, like a filter based on categories or a saved view. */
    class RejectingTransformer extends IModelTransformer {
      private readonly _rejectedIds: ReadonlySet<Id64String>;

      public constructor(
        source: IModelDb,
        target: EditTxn,
        rejectedIds: Id64String[],
        options?: IModelTransformOptions
      ) {
        super({ source, target }, options);
        this._rejectedIds = new Set(rejectedIds);
      }

      public override async shouldExportElement(sourceElement: Element) {
        return !this._rejectedIds.has(sourceElement.id);
      }
    }

    /** Transforms while rejecting the given elements, then pushes the target; abandons the target changes on failure. */
    async function transformRejecting(
      rejectedIds: Id64String[],
      options?: IModelTransformOptions
    ) {
      const editTxn = createStartedEditTxn(targetDb);
      const transformer = new RejectingTransformer(
        sourceDb,
        editTxn,
        rejectedIds,
        options
      );
      try {
        await transformer.process();
        editTxn.end();
      } finally {
        transformer.dispose();
        if (editTxn.isActive) editTxn.end("abandon");
      }
      await targetDb.pushChanges({
        description: "transform",
        retainLocks: true,
      });
    }

    const processChanges: IModelTransformOptions = {
      argsForProcessChanges: {},
    };

    async function pushSource<T>(
      description: string,
      write: (txn: EditTxn) => T
    ) {
      const result = withEditTxn(sourceDb, description, write);
      await sourceDb.pushChanges({ description, retainLocks: true });
      return result;
    }

    /** Inserts a category that a full run rejects, then a subcategory under it. */
    async function insertSubCategoryUnderRejectedCategory() {
      const categoryId = await pushSource("insert category", (txn) =>
        SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "RejectedCategory",
          new SubCategoryAppearance()
        )
      );
      await transformRejecting([categoryId]);
      const subCategoryId = await pushSource("insert subcategory", (txn) =>
        SubCategory.insert(
          txn,
          categoryId,
          "NewSubCategory",
          new SubCategoryAppearance()
        )
      );
      return { categoryId, subCategoryId };
    }

    it("should skip a changed child when shouldExportElement rejects its unchanged parent", async () => {
      const { categoryId } = await insertSubCategoryUnderRejectedCategory();

      // Completing proves the subcategory was skipped: importing it without its category would throw.
      await transformRejecting([categoryId], processChanges);
    });

    it("should throw DependencyMappingMissing when a changed child requires an unchanged parent that is not in the target", async () => {
      const { categoryId, subCategoryId } =
        await insertSubCategoryUnderRejectedCategory();

      const shouldExport = vi.spyOn(
        RejectingTransformer.prototype,
        "shouldExportElement"
      );

      // The filter now accepts the category, but nobody added it in addCustomChanges.
      await expectTransformerError(
        transformRejecting([], processChanges),
        IModelTransformerError.DependencyMappingMissing,
        `Element ${subCategoryId} requires unchanged element ${categoryId}, which is not in the target iModel. Change processing does not insert unchanged elements; to insert element ${categoryId}, add it in addCustomChanges.`
      );
      // The traversal filtered the category; mapping the subcategory reuses that result.
      expect(
        shouldExport.mock.calls.filter(([element]) => element.id === categoryId)
      ).to.have.length(1);
      shouldExport.mockRestore();
    });

    it("should map a changed child's unchanged parent that is found in the target by Code", async () => {
      const { categoryId, subCategoryId } =
        await insertSubCategoryUnderRejectedCategory();
      const targetCategoryId = withEditTxn(targetDb, "insert category", (txn) =>
        SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "RejectedCategory",
          new SubCategoryAppearance()
        )
      );
      const onExportElement = vi.spyOn(
        RejectingTransformer.prototype,
        "onExportElement"
      );

      await transformRejecting([], processChanges);

      expect(
        targetDb.elements.queryElementIdByCode(
          SubCategory.createCode(targetDb, targetCategoryId, "NewSubCategory")
        )
      ).to.not.equal(undefined);
      // The lookup does not run export hooks for the unchanged category, which is never imported.
      expect(
        onExportElement.mock.calls.filter(
          ([element]) => element.id === categoryId
        )
      ).to.have.length(0);
      expect(
        onExportElement.mock.calls.filter(
          ([element]) => element.id === subCategoryId
        )
      ).to.have.length(1);
      onExportElement.mockRestore();
    });

    it("should throw DependencyMappingMissing when a changed element requires a changed category that shouldExportElement rejects", async () => {
      const modelId = await pushSource("insert model", (txn) =>
        PhysicalModel.insert(txn, IModel.rootSubjectId, "PhysicalModel")
      );
      await transformRejecting([]);
      const { categoryId, elementId } = await pushSource(
        "insert category and physical object",
        (txn) => {
          const category = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "RejectedCategory",
            new SubCategoryAppearance()
          );
          return {
            categoryId: category,
            elementId: txn.insertElement({
              classFullName: PhysicalObject.classFullName,
              model: modelId,
              category,
              code: Code.createEmpty(),
            } as GeometricElementProps),
          };
        }
      );

      await expectTransformerError(
        transformRejecting([categoryId], processChanges),
        IModelTransformerError.DependencyMappingMissing,
        `Element ${elementId} requires element ${categoryId}, which was not exported because the export filter rejects it or one of its ancestors. Accept element ${categoryId} and its ancestors, or reject element ${elementId}.`
      );
    });

    it("should throw DependencyMappingMissing when a full transform accepts an element whose category shouldExportElement rejects", async () => {
      const { categoryId, elementId } = await pushSource(
        "insert model, category, and physical object",
        (txn) => {
          const model = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PhysicalModel"
          );
          const category = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "RejectedCategory",
            new SubCategoryAppearance()
          );
          return {
            categoryId: category,
            elementId: txn.insertElement({
              classFullName: PhysicalObject.classFullName,
              model,
              category,
              code: Code.createEmpty(),
            } as GeometricElementProps),
          };
        }
      );

      // Without the check, native cloning inserts a copy of the rejected category.
      await expectTransformerError(
        transformRejecting([categoryId]),
        IModelTransformerError.DependencyMappingMissing,
        `Element ${elementId} requires element ${categoryId}, which was not exported because the export filter rejects it or one of its ancestors. Accept element ${categoryId} and its ancestors, or reject element ${elementId}.`
      );
    });

    it("should throw DependencyMappingMissing when a changed element requires a changed category whose unchanged parent shouldExportElement rejects", async () => {
      const { modelId, parentCategoryId } = await pushSource(
        "insert model and parent category",
        (txn) => ({
          modelId: PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PhysicalModel"
          ),
          parentCategoryId: SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "RejectedParentCategory",
            new SubCategoryAppearance()
          ),
        })
      );
      await transformRejecting([parentCategoryId]);
      const categoryId = await pushSource(
        "insert child category and physical object",
        (txn) => {
          const category = txn.insertElement({
            classFullName: SpatialCategory.classFullName,
            model: IModel.dictionaryId,
            code: SpatialCategory.createCode(
              sourceDb,
              IModel.dictionaryId,
              "ChildCategory"
            ),
            parent: new ElementOwnsChildElements(parentCategoryId),
          });
          txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category,
            code: Code.createEmpty(),
          } as GeometricElementProps);
          return category;
        }
      );

      // The element is exported before its category's rejected parent is filtered, so the error is about the category.
      await expectTransformerError(
        transformRejecting([parentCategoryId], processChanges),
        IModelTransformerError.DependencyMappingMissing,
        `Element ${categoryId} requires element ${parentCategoryId}, which the export filter rejects. Accept element ${parentCategoryId}, or reject element ${categoryId} and every element that requires it.`
      );
    });

    it("should throw DependencyMappingMissing when a changed element requires an unchanged category that shouldExportElement rejects", async () => {
      const { modelId, categoryId } = await pushSource(
        "insert model and category",
        (txn) => ({
          modelId: PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PhysicalModel"
          ),
          categoryId: SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "RejectedCategory",
            new SubCategoryAppearance()
          ),
        })
      );
      await transformRejecting([categoryId]);
      const elementId = await pushSource("insert physical object", (txn) =>
        txn.insertElement({
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: Code.createEmpty(),
        } as GeometricElementProps)
      );

      await expectTransformerError(
        transformRejecting([categoryId], processChanges),
        IModelTransformerError.DependencyMappingMissing,
        `Element ${elementId} requires element ${categoryId}, which the export filter rejects. Accept element ${categoryId}, or reject element ${elementId} and every element that requires it.`
      );
    });
  });
});
