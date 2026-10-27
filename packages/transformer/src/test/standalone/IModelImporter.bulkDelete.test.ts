/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { DbResult, Id64String, Logger } from "@itwin/core-bentley";
import {
  BulkDeleteElementsStatus,
  CategorySelector,
  DefinitionContainer,
  DefinitionModel,
  DisplayStyle3d,
  DocumentListModel,
  Drawing,
  DrawingCategory,
  DrawingGraphic,
  EditTxn,
  GeometryPart,
  IModelDb,
  ModelSelector,
  PhysicalModel,
  Sheet,
  SheetModel,
  SpatialCategory,
  SpatialViewDefinition,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import {
  Code,
  CodeScopeSpec,
  GeometricElement2dProps,
  GeometryPartProps,
  IModel,
  PhysicalElementProps,
  SheetProps,
  SubCategoryAppearance,
  ViewAttachmentProps,
} from "@itwin/core-common";
import { Point2d, Point3d, Range3d } from "@itwin/core-geometry";
import { expect, vi } from "vitest";
import { planBulkDelete } from "../../ElementBulkDelete.js";
import {
  ElementBulkDeleteError,
  IModelImporter,
} from "../../IModelImporter.js";
import { IModelTransformerError } from "../../IModelTransformerError.js";
import {
  createStartedEditTxn,
  expectTransformerError,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils.js";
import { IModelTestUtils } from "../TestUtils/index.js";

interface PhysicalObjectOptions {
  readonly modelId: Id64String;
  readonly categoryId: Id64String;
  readonly codeSpecId?: Id64String;
  readonly parentId?: Id64String;
  readonly codeScope?: Id64String;
  readonly codeValue?: string;
}

function insertPhysicalObject(
  txn: EditTxn,
  options: PhysicalObjectOptions
): Id64String {
  const code =
    options.codeSpecId && options.codeScope && options.codeValue
      ? {
          spec: options.codeSpecId,
          scope: options.codeScope,
          value: options.codeValue,
        }
      : Code.createEmpty();
  return txn.insertElement({
    classFullName: "Generic:PhysicalObject",
    model: options.modelId,
    category: options.categoryId,
    code,
    placement: {
      origin: [0, 0, 0],
      angles: { yaw: 0, pitch: 0, roll: 0 },
    },
    ...(options.parentId
      ? {
          parent: {
            id: options.parentId,
            relClassName: "BisCore:ElementOwnsChildElements",
          },
        }
      : {}),
  } as PhysicalElementProps);
}

/** Inserts a geometry part and a physical object whose geometry places it. */
function insertGeometryPartUser(
  txn: EditTxn,
  db: IModelDb,
  ids: {
    readonly definitionModelId: Id64String;
    readonly modelId: Id64String;
    readonly categoryId: Id64String;
  }
): { partId: Id64String; userId: Id64String } {
  const size = Point3d.create(1, 1, 1);
  const partId = txn.insertElement({
    classFullName: GeometryPart.classFullName,
    model: ids.definitionModelId,
    code: GeometryPart.createCode(db, ids.definitionModelId, "Part"),
    geom: IModelTestUtils.createBox(size),
  } as GeometryPartProps);
  const userId = txn.insertElement({
    classFullName: "Generic:PhysicalObject",
    model: ids.modelId,
    category: ids.categoryId,
    code: Code.createEmpty(),
    placement: { origin: [0, 0, 0], angles: {} },
    geom: IModelTestUtils.createBox(
      size,
      ids.categoryId,
      IModelDb.getDefaultSubCategoryId(ids.categoryId),
      undefined,
      partId
    ),
  } as PhysicalElementProps);
  return { partId, userId };
}

/** Inserts a spatial view with its display style and selectors. */
function insertSpatialView(
  txn: EditTxn,
  definitionModelId: Id64String,
  modelId: Id64String,
  categoryId: Id64String
) {
  const displayStyleId = DisplayStyle3d.insert(txn, definitionModelId, "Style");
  const modelSelectorId = ModelSelector.insert(
    txn,
    definitionModelId,
    "Models",
    [modelId]
  );
  const categorySelectorId = CategorySelector.insert(
    txn,
    definitionModelId,
    "Categories",
    [categoryId]
  );
  const viewId = SpatialViewDefinition.insertWithCamera(
    txn,
    definitionModelId,
    "View",
    modelSelectorId,
    categorySelectorId,
    displayStyleId,
    new Range3d(0, 0, 0, 1, 1, 1)
  );
  return { displayStyleId, modelSelectorId, categorySelectorId, viewId };
}

async function planPhases(
  db: IModelDb,
  elementIds: ReadonlySet<Id64String>
): Promise<Set<Id64String>[]> {
  const plan = await planBulkDelete(db, elementIds);
  expect(plan.keptReferences.size).to.equal(0);
  return plan.phases.map((roots) => new Set(roots));
}

function createTargetDb(testName: string): StandaloneDb {
  const fileName = IModelTransformerTestUtils.prepareOutputFile(
    "IModelImporterBulkDelete",
    `${testName}.bim`
  );
  return StandaloneDb.createEmpty(fileName, {
    rootSubject: { name: testName },
  });
}

describe("IModelImporter bulk element deletion", () => {
  it("routes singular deletion through the batch hook and filters protected roots", async () => {
    const targetDb = createTargetDb("ProtectedRoots");
    try {
      const { protectedId, deletableId } = withEditTxn(
        targetDb,
        "insert subjects",
        (txn) => ({
          protectedId: Subject.create(
            targetDb,
            IModel.rootSubjectId,
            "Protected"
          ).insert(txn),
          deletableId: Subject.create(
            targetDb,
            IModel.rootSubjectId,
            "Deletable"
          ).insert(txn),
        })
      );
      const editTxn = createStartedEditTxn(targetDb);
      class TrackingImporter extends IModelImporter {
        public readonly deletionBatches: ReadonlySet<Id64String>[] = [];

        protected override async onDeleteElements(
          elementIds: ReadonlySet<Id64String>
        ): Promise<void> {
          this.deletionBatches.push(new Set(elementIds));
          await super.onDeleteElements(elementIds);
        }
      }
      const importer = new TrackingImporter(editTxn);
      expect(importer.targetDb).to.equal(editTxn.iModel);
      importer.doNotUpdateElementIds.add(protectedId);

      await importer.deleteElements(new Set([protectedId]));
      await importer.deleteElement(deletableId);

      expect(importer.deletionBatches).to.deep.equal([new Set([deletableId])]);
      expect(targetDb.elements.tryGetElement(protectedId)).to.not.be.undefined;
      expect(targetDb.elements.tryGetElement(deletableId)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("expands transitive code-scope roots and their descendants", async () => {
    const targetDb = createTargetDb("TransitiveCodeScopes");
    try {
      const ids = withEditTxn(targetDb, "insert deletion graph", (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Spatial category",
          new SubCategoryAppearance()
        );
        const codeSpecId = targetDb.codeSpecs.insert(
          txn,
          "RelatedElementCodeSpec",
          CodeScopeSpec.Type.RelatedElement
        );
        const common = { modelId, categoryId, codeSpecId };
        const rootId = insertPhysicalObject(txn, common);
        const childId = insertPhysicalObject(txn, {
          ...common,
          parentId: rootId,
        });
        const firstCodeRootId = insertPhysicalObject(txn, {
          ...common,
          codeScope: childId,
          codeValue: "scoped-by-child",
        });
        const firstCodeChildId = insertPhysicalObject(txn, {
          ...common,
          parentId: firstCodeRootId,
        });
        const secondCodeRootId = insertPhysicalObject(txn, {
          ...common,
          codeScope: firstCodeChildId,
          codeValue: "scoped-by-dependent-child",
        });
        const secondCodeChildId = insertPhysicalObject(txn, {
          ...common,
          parentId: secondCodeRootId,
        });
        return {
          rootId,
          childId,
          firstCodeRootId,
          firstCodeChildId,
          secondCodeRootId,
          secondCodeChildId,
        };
      });
      const explicitRoots = new Set([ids.rootId]);
      expect(await planPhases(targetDb, explicitRoots)).to.deep.equal([
        new Set([ids.rootId, ids.firstCodeRootId, ids.secondCodeRootId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      const nativeDeleteSpy = vi.spyOn(editTxn, "deleteElements");
      // __PUBLISH_EXTRACT_START__ EditTxnInTransformer.custom-importer
      // IModelImporter derives targetDb from the EditTxn.
      const importer = new IModelImporter(editTxn);
      await importer.deleteElements(explicitRoots);
      // __PUBLISH_EXTRACT_END__

      expect(new Set(nativeDeleteSpy.mock.calls[0][0])).to.deep.equal(
        new Set([ids.rootId, ids.firstCodeRootId, ids.secondCodeRootId])
      );
      for (const id of Object.values(ids))
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("handles overlapping explicit roots and ignores missing roots", async () => {
    const targetDb = createTargetDb("OverlappingAndMissingRoots");
    try {
      const ids = withEditTxn(targetDb, "insert element tree", (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Spatial category",
          new SubCategoryAppearance()
        );
        const rootId = insertPhysicalObject(txn, { modelId, categoryId });
        const childId = insertPhysicalObject(txn, {
          modelId,
          categoryId,
          parentId: rootId,
        });
        return { rootId, childId };
      });
      const roots = new Set([ids.rootId, ids.childId, "0xdead"]);
      // The child is deleted with its requested parent, so it is not a root of its own.
      expect(await planPhases(targetDb, roots)).to.deep.equal([
        new Set([ids.rootId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      const nativeDeleteSpy = vi.spyOn(editTxn, "deleteElements");
      await new IModelImporter(editTxn).deleteElements(roots);

      expect(new Set(nativeDeleteSpy.mock.calls[0][0])).to.deep.equal(
        new Set([ids.rootId])
      );
      expect(targetDb.elements.tryGetElement(ids.rootId)).to.be.undefined;
      expect(targetDb.elements.tryGetElement(ids.childId)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes modeled contents and code dependents discovered through them", async () => {
    const targetDb = createTargetDb("ModeledElementCascade");
    try {
      const ids = withEditTxn(targetDb, "insert modeled element", (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const survivorModelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Survivor model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Spatial category",
          new SubCategoryAppearance()
        );
        const codeSpecId = targetDb.codeSpecs.insert(
          txn,
          "ModelContentScopeCodeSpec",
          CodeScopeSpec.Type.RelatedElement
        );
        const contentId = insertPhysicalObject(txn, { modelId, categoryId });
        const codeScopeDependentId = insertPhysicalObject(txn, {
          modelId: survivorModelId,
          categoryId,
          codeSpecId,
          codeScope: contentId,
          codeValue: "scoped-by-model-content",
        });
        const dependentChildId = insertPhysicalObject(txn, {
          modelId: survivorModelId,
          categoryId,
          parentId: codeScopeDependentId,
        });
        return {
          modelId,
          survivorModelId,
          contentId,
          codeScopeDependentId,
          dependentChildId,
        };
      });
      const explicitRoots = new Set([ids.modelId]);
      expect(await planPhases(targetDb, explicitRoots)).to.deep.equal([
        new Set([ids.modelId, ids.codeScopeDependentId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      const nativeDeleteSpy = vi.spyOn(editTxn, "deleteElements");
      await new IModelImporter(editTxn).deleteElements(explicitRoots);

      expect(new Set(nativeDeleteSpy.mock.calls[0][0])).to.deep.equal(
        new Set([ids.modelId, ids.codeScopeDependentId])
      );
      expect(targetDb.elements.tryGetElement(ids.contentId)).to.be.undefined;
      expect(targetDb.elements.tryGetElement(ids.codeScopeDependentId)).to.be
        .undefined;
      expect(targetDb.elements.tryGetElement(ids.dependentChildId)).to.be
        .undefined;
      expect(targetDb.models.tryGetModel(ids.modelId)).to.be.undefined;
      expect(targetDb.elements.tryGetElement(ids.modelId)).to.be.undefined;
      expect(targetDb.models.tryGetModel(ids.survivorModelId)).to.not.be
        .undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes recursively nested submodels and their contents", async () => {
    const targetDb = createTargetDb("NestedSubModels");
    try {
      const ids = withEditTxn(targetDb, "insert nested submodels", (txn) => {
        const outerModelId = DefinitionModel.insert(
          txn,
          IModel.rootSubjectId,
          "Outer definition model"
        );
        const firstContainerId = DefinitionContainer.insert(
          txn,
          outerModelId,
          Code.createEmpty()
        );
        const secondContainerId = DefinitionContainer.insert(
          txn,
          firstContainerId,
          Code.createEmpty()
        );
        const nestedCategoryId = SpatialCategory.insert(
          txn,
          secondContainerId,
          "Nested category",
          new SubCategoryAppearance()
        );
        const unrelatedId = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "Unrelated"
        );
        return {
          outerModelId,
          firstContainerId,
          secondContainerId,
          nestedCategoryId,
          defaultSubCategoryId:
            IModelDb.getDefaultSubCategoryId(nestedCategoryId),
          unrelatedId,
        };
      });
      const explicitRoots = new Set([ids.outerModelId]);
      const expectedDeleteRoots = new Set([
        ids.outerModelId,
        ids.nestedCategoryId,
      ]);
      expect(await planPhases(targetDb, explicitRoots)).to.deep.equal([
        expectedDeleteRoots,
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      const nativeDeleteSpy = vi.spyOn(editTxn, "deleteElements");
      await new IModelImporter(editTxn).deleteElements(explicitRoots);

      expect(new Set(nativeDeleteSpy.mock.calls[0][0])).to.deep.equal(
        expectedDeleteRoots
      );
      for (const id of [
        ids.outerModelId,
        ids.firstContainerId,
        ids.secondContainerId,
        ids.nestedCategoryId,
        ids.defaultSubCategoryId,
      ]) {
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      }
      for (const id of [
        ids.outerModelId,
        ids.firstContainerId,
        ids.secondContainerId,
      ]) {
        expect(targetDb.models.tryGetModel(id)).to.be.undefined;
      }
      expect(targetDb.elements.tryGetElement(ids.unrelatedId)).to.not.be
        .undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes an element and the category it uses in one batch", async () => {
    const targetDb = createTargetDb("ElementAndCategory");
    try {
      const ids = withEditTxn(targetDb, "insert categorized element", (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Deleted category",
          new SubCategoryAppearance()
        );
        const elementId = insertPhysicalObject(txn, { modelId, categoryId });
        return {
          categoryId,
          defaultSubCategoryId: IModelDb.getDefaultSubCategoryId(categoryId),
          elementId,
        };
      });
      const roots = new Set([ids.categoryId, ids.elementId]);
      expect(await planPhases(targetDb, roots)).to.deep.equal([
        new Set([ids.elementId]),
        new Set([ids.categoryId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      const nativeDeleteSpy = vi.spyOn(editTxn, "deleteElements");
      await new IModelImporter(editTxn).deleteElements(roots);

      expect(nativeDeleteSpy).toHaveBeenCalledTimes(2);
      for (const id of Object.values(ids))
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes a chain of definition containers whose nested category is used by a deleted element", async () => {
    const targetDb = createTargetDb("DefinitionChain");
    try {
      const ids = withEditTxn(targetDb, "insert definition chain", (txn) => {
        const definitionModelId = DefinitionModel.insert(
          txn,
          IModel.rootSubjectId,
          "Definition model"
        );
        const outerContainerId = DefinitionContainer.insert(
          txn,
          definitionModelId,
          Code.createEmpty()
        );
        const innerContainerId = DefinitionContainer.insert(
          txn,
          outerContainerId,
          Code.createEmpty()
        );
        const categoryId = SpatialCategory.insert(
          txn,
          innerContainerId,
          "Nested category",
          new SubCategoryAppearance()
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const elementId = insertPhysicalObject(txn, { modelId, categoryId });
        return { outerContainerId, innerContainerId, categoryId, elementId };
      });
      const roots = new Set([ids.outerContainerId, ids.elementId]);
      expect(await planPhases(targetDb, roots)).to.deep.equal([
        new Set([ids.elementId]),
        new Set([ids.outerContainerId, ids.categoryId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(roots);

      for (const id of Object.values(ids))
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes a tree that contains both a category and an element that uses it", async () => {
    const targetDb = createTargetDb("CategoryAndUserInOneTree");
    try {
      const ids = withEditTxn(targetDb, "insert subject tree", (txn) => {
        const subjectId = Subject.insert(txn, IModel.rootSubjectId, "Subject");
        const definitionModelId = DefinitionModel.insert(
          txn,
          subjectId,
          "Definition model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          definitionModelId,
          "Category",
          new SubCategoryAppearance()
        );
        const modelId = PhysicalModel.insert(txn, subjectId, "Physical model");
        const elementId = insertPhysicalObject(txn, { modelId, categoryId });
        return { subjectId, definitionModelId, categoryId, modelId, elementId };
      });
      const roots = new Set([ids.subjectId]);
      expect(await planPhases(targetDb, roots)).to.deep.equal([
        new Set([ids.elementId]),
        new Set([ids.subjectId, ids.categoryId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(roots);

      for (const id of Object.values(ids))
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes a code dependent with a parent before the category user that scopes its code", async () => {
    const targetDb = createTargetDb("ParentedCodeDependentOfCategoryUser");
    try {
      const ids = withEditTxn(targetDb, "insert subject tree", (txn) => {
        const subjectId = Subject.insert(txn, IModel.rootSubjectId, "Subject");
        const definitionModelId = DefinitionModel.insert(
          txn,
          subjectId,
          "Definition model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          definitionModelId,
          "Category",
          new SubCategoryAppearance()
        );
        const keptCategoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Kept category",
          new SubCategoryAppearance()
        );
        const codeSpecId = targetDb.codeSpecs.insert(
          txn,
          "RelatedElementCodeSpec",
          CodeScopeSpec.Type.RelatedElement
        );
        const modelId = PhysicalModel.insert(txn, subjectId, "Physical model");
        const userId = insertPhysicalObject(txn, { modelId, categoryId });
        const parentId = insertPhysicalObject(txn, {
          modelId,
          categoryId: keptCategoryId,
        });
        const dependentId = insertPhysicalObject(txn, {
          modelId,
          categoryId: keptCategoryId,
          parentId,
          codeSpecId,
          codeScope: userId,
          codeValue: "scoped-by-category-user",
        });
        return { subjectId, categoryId, userId, parentId, dependentId };
      });
      const roots = new Set([ids.subjectId]);
      expect(await planPhases(targetDb, roots)).to.deep.equal([
        new Set([ids.userId, ids.dependentId]),
        new Set([ids.subjectId, ids.categoryId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(roots);

      for (const id of Object.values(ids))
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes category users first when a tree deleted last has an element whose code another tree scopes", async () => {
    const targetDb = createTargetDb("CodeScopeInOtherTree");
    try {
      const ids = withEditTxn(targetDb, "insert trees", (txn) => {
        const subjectId = Subject.insert(txn, IModel.rootSubjectId, "Subject");
        const definitionModelId = DefinitionModel.insert(
          txn,
          subjectId,
          "Definition model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          definitionModelId,
          "Category",
          new SubCategoryAppearance()
        );
        const keptCategoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Kept category",
          new SubCategoryAppearance()
        );
        const codeSpecId = targetDb.codeSpecs.insert(
          txn,
          "RelatedElementCodeSpec",
          CodeScopeSpec.Type.RelatedElement
        );
        const scopeId = Subject.insert(txn, IModel.rootSubjectId, "Scope");
        const modelId = PhysicalModel.insert(txn, subjectId, "Physical model");
        const parentId = insertPhysicalObject(txn, {
          modelId,
          categoryId: keptCategoryId,
        });
        const dependentId = insertPhysicalObject(txn, {
          modelId,
          categoryId: keptCategoryId,
          parentId,
          codeSpecId,
          codeScope: scopeId,
          codeValue: "scoped-by-other-tree",
        });
        const userModelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "User model"
        );
        const userId = insertPhysicalObject(txn, {
          modelId: userModelId,
          categoryId,
        });
        return {
          subjectId,
          categoryId,
          scopeId,
          parentId,
          dependentId,
          userModelId,
          userId,
        };
      });
      const requested = new Set([ids.subjectId, ids.scopeId, ids.userModelId]);
      // Deleting the scope's tree before the subject's would delete the scope while its dependent still exists.
      expect(await planPhases(targetDb, requested)).to.deep.equal([
        new Set([ids.userId]),
        new Set([ids.subjectId, ids.categoryId, ids.scopeId, ids.userModelId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(requested);
      for (const id of Object.values(ids))
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes a 2D element and the drawing category it uses in one batch", async () => {
    const targetDb = createTargetDb("DrawingCategoryUsedInBatch");
    try {
      const ids = withEditTxn(targetDb, "insert drawing", (txn) => {
        const documentListModelId = DocumentListModel.insert(
          txn,
          IModel.rootSubjectId,
          "Documents"
        );
        const drawingModelId = Drawing.insert(
          txn,
          documentListModelId,
          "Drawing"
        );
        const categoryId = DrawingCategory.insert(
          txn,
          IModel.dictionaryId,
          "Drawing category",
          new SubCategoryAppearance()
        );
        const graphicId = txn.insertElement({
          classFullName: DrawingGraphic.classFullName,
          model: drawingModelId,
          category: categoryId,
          code: Code.createEmpty(),
          placement: { origin: Point2d.createZero(), angle: 0 },
        } as GeometricElement2dProps);
        return {
          categoryId,
          defaultSubCategoryId: IModelDb.getDefaultSubCategoryId(categoryId),
          graphicId,
        };
      });
      const requested = new Set([ids.categoryId, ids.graphicId]);
      expect(await planPhases(targetDb, requested)).to.deep.equal([
        new Set([ids.graphicId]),
        new Set([ids.categoryId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(requested);
      for (const id of Object.values(ids))
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes an element in two requested trees before the category it uses", async () => {
    const targetDb = createTargetDb("CategoryUserInTwoTrees");
    try {
      const ids = withEditTxn(targetDb, "insert overlapping trees", (txn) => {
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Category",
          new SubCategoryAppearance()
        );
        const codeSpecId = targetDb.codeSpecs.insert(
          txn,
          "RelatedElementCodeSpec",
          CodeScopeSpec.Type.RelatedElement
        );
        const scopeId = Subject.insert(txn, IModel.rootSubjectId, "Scope");
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        // In the model's tree, and a root of its own because the scope's tree scopes its code.
        const userId = insertPhysicalObject(txn, {
          modelId,
          categoryId,
          codeSpecId,
          codeScope: scopeId,
          codeValue: "in-two-trees",
        });
        return { categoryId, scopeId, modelId, userId };
      });
      const requested = new Set([ids.categoryId, ids.scopeId, ids.modelId]);
      expect(await planPhases(targetDb, requested)).to.deep.equal([
        new Set([ids.scopeId, ids.modelId, ids.userId]),
        new Set([ids.categoryId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(requested);
      for (const id of Object.values(ids))
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes a deep chain of requested elements as one root", async () => {
    const targetDb = createTargetDb("DeepRequestedChain");
    try {
      const chainIds = withEditTxn(targetDb, "insert chain", (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Spatial category",
          new SubCategoryAppearance()
        );
        const ids: Id64String[] = [];
        for (let i = 0; i < 500; i++)
          ids.push(
            insertPhysicalObject(txn, {
              modelId,
              categoryId,
              parentId: ids[i - 1],
            })
          );
        return ids;
      });
      // Each requested element is walked once, not once per requested ancestor.
      expect(await planPhases(targetDb, new Set(chainIds))).to.deep.equal([
        new Set([chainIds[0]]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(new Set(chainIds));

      for (const id of chainIds)
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("keeps a category that an element outside the batch still uses and deletes the rest", async () => {
    const targetDb = createTargetDb("CategoryStillInUse");
    try {
      const ids = withEditTxn(targetDb, "insert used category", (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Used spatial category",
          new SubCategoryAppearance()
        );
        const deletedElementId = insertPhysicalObject(txn, {
          modelId,
          categoryId,
        });
        const survivorId = insertPhysicalObject(txn, { modelId, categoryId });
        const independentId = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "Independent"
        );
        return {
          modelId,
          categoryId,
          defaultSubCategoryId: IModelDb.getDefaultSubCategoryId(categoryId),
          deletedElementId,
          survivorId,
          independentId,
        };
      });
      // A source deletion lists the category's sub-categories too. They stay with the kept category.
      const requested = new Set([
        ids.categoryId,
        ids.defaultSubCategoryId,
        ids.deletedElementId,
        ids.independentId,
      ]);
      const plan = await planBulkDelete(targetDb, requested);
      expect(plan.keptReferences).to.deep.equal(
        new Map([[ids.categoryId, ids.survivorId]])
      );
      expect(plan.phases.map((roots) => new Set(roots))).to.deep.equal([
        new Set([ids.deletedElementId, ids.independentId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      const nativeDeleteSpy = vi.spyOn(editTxn, "deleteElements");
      const warningSpy = vi.spyOn(Logger, "logWarning");
      try {
        await new IModelImporter(editTxn).deleteElements(requested);
        expect(warningSpy).toHaveBeenCalledOnce();
        expect(warningSpy.mock.calls[0][1]).to.contain(
          `${ids.categoryId} (referenced by ${ids.survivorId})`
        );
      } finally {
        warningSpy.mockRestore();
      }

      expect(nativeDeleteSpy).toHaveBeenCalledOnce();
      for (const id of [ids.deletedElementId, ids.independentId])
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      for (const id of [
        ids.categoryId,
        ids.defaultSubCategoryId,
        ids.survivorId,
      ])
        expect(targetDb.elements.tryGetElement(id)).to.not.be.undefined;
      // The category remains usable in the same transaction.
      insertPhysicalObject(editTxn, {
        modelId: ids.modelId,
        categoryId: ids.categoryId,
      });
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("keeps the definition model around a kept category and deletes its other requested contents", async () => {
    const targetDb = createTargetDb("KeptCategoryInDefinitionModel");
    try {
      const ids = withEditTxn(targetDb, "insert definitions", (txn) => {
        const definitionModelId = DefinitionModel.insert(
          txn,
          IModel.rootSubjectId,
          "Definition model"
        );
        const usedCategoryId = SpatialCategory.insert(
          txn,
          definitionModelId,
          "Used category",
          new SubCategoryAppearance()
        );
        const unusedCategoryId = SpatialCategory.insert(
          txn,
          definitionModelId,
          "Unused category",
          new SubCategoryAppearance()
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const survivorId = insertPhysicalObject(txn, {
          modelId,
          categoryId: usedCategoryId,
        });
        return {
          definitionModelId,
          usedCategoryId,
          usedSubCategoryId: IModelDb.getDefaultSubCategoryId(usedCategoryId),
          unusedCategoryId,
          unusedSubCategoryId:
            IModelDb.getDefaultSubCategoryId(unusedCategoryId),
          survivorId,
        };
      });
      const requested = new Set([
        ids.definitionModelId,
        ids.usedCategoryId,
        ids.usedSubCategoryId,
        ids.unusedCategoryId,
        ids.unusedSubCategoryId,
      ]);
      const plan = await planBulkDelete(targetDb, requested);
      expect(plan.keptReferences).to.deep.equal(
        new Map([[ids.usedCategoryId, ids.survivorId]])
      );
      expect(plan.phases).to.deep.equal([[ids.unusedCategoryId]]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(requested);

      for (const id of [ids.unusedCategoryId, ids.unusedSubCategoryId])
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      for (const id of [
        ids.definitionModelId,
        ids.usedCategoryId,
        ids.usedSubCategoryId,
        ids.survivorId,
      ])
        expect(targetDb.elements.tryGetElement(id)).to.not.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("keeps a category used by an element that another outside reference keeps", async () => {
    const targetDb = createTargetDb("KeptElementKeepsItsCategory");
    try {
      const ids = withEditTxn(targetDb, "insert references", (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Category",
          new SubCategoryAppearance()
        );
        const keptCategoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Kept category",
          new SubCategoryAppearance()
        );
        const codeSpecId = targetDb.codeSpecs.insert(
          txn,
          "RelatedElementCodeSpec",
          CodeScopeSpec.Type.RelatedElement
        );
        const scopeId = insertPhysicalObject(txn, { modelId, categoryId });
        const parentId = insertPhysicalObject(txn, {
          modelId,
          categoryId: keptCategoryId,
        });
        // A child whose code is scoped by the element; code-scope dependents with a parent are not deleted with their scope.
        const outsideId = insertPhysicalObject(txn, {
          modelId,
          categoryId: keptCategoryId,
          parentId,
          codeSpecId,
          codeScope: scopeId,
          codeValue: "scoped-from-outside",
        });
        return { categoryId, scopeId, outsideId };
      });
      const requested = new Set([
        ids.scopeId,
        ids.categoryId,
        IModelDb.getDefaultSubCategoryId(ids.categoryId),
      ]);
      // The first pass keeps the scope element; the second keeps the category it uses.
      const plan = await planBulkDelete(targetDb, requested);
      expect(plan.keptReferences).to.deep.equal(
        new Map([
          [ids.scopeId, ids.outsideId],
          [ids.categoryId, ids.scopeId],
        ])
      );
      expect(plan.phases).to.deep.equal([]);

      const editTxn = createStartedEditTxn(targetDb);
      const nativeDeleteSpy = vi.spyOn(editTxn, "deleteElements");
      await new IModelImporter(editTxn).deleteElements(requested);

      expect(nativeDeleteSpy).not.toHaveBeenCalled();
      for (const id of Object.values(ids))
        expect(targetDb.elements.tryGetElement(id)).to.not.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("lists at most ten kept references in the warning", async () => {
    const targetDb = createTargetDb("ManyKeptReferences");
    try {
      const keptReferences = withEditTxn(
        targetDb,
        "insert used categories",
        (txn) => {
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "Physical model"
          );
          const references = new Map<Id64String, Id64String>();
          for (let i = 0; i < 12; i++) {
            const categoryId = SpatialCategory.insert(
              txn,
              IModel.dictionaryId,
              `Used category ${i}`,
              new SubCategoryAppearance()
            );
            references.set(
              categoryId,
              insertPhysicalObject(txn, { modelId, categoryId })
            );
          }
          return references;
        }
      );
      const requested = new Set(keptReferences.keys());
      const plan = await planBulkDelete(targetDb, requested);
      expect([...plan.keptReferences].sort()).to.deep.equal(
        [...keptReferences].sort()
      );

      const editTxn = createStartedEditTxn(targetDb);
      const warningSpy = vi.spyOn(Logger, "logWarning");
      try {
        await new IModelImporter(editTxn).deleteElements(requested);
        const listed = [...plan.keptReferences]
          .slice(0, 10)
          .map(
            ([id, referencingId]) => `${id} (referenced by ${referencingId})`
          )
          .join(", ");
        expect(warningSpy).toHaveBeenCalledOnce();
        expect(warningSpy.mock.calls[0][1]).to.equal(
          `Kept 12 elements that elements outside the deleted trees still reference, and the elements that contain them: ${listed}, and 2 more`
        );
      } finally {
        warningSpy.mockRestore();
      }
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes an element and the geometry part it uses in one batch", async () => {
    const targetDb = createTargetDb("GeometryPartUsedInBatch");
    try {
      const ids = withEditTxn(targetDb, "insert part and user", (txn) => {
        const definitionModelId = DefinitionModel.insert(
          txn,
          IModel.rootSubjectId,
          "Definition model"
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Spatial category",
          new SubCategoryAppearance()
        );
        return insertGeometryPartUser(txn, targetDb, {
          definitionModelId,
          modelId,
          categoryId,
        });
      });
      const requested = new Set([ids.partId, ids.userId]);
      // Native validation refuses the part while its user exists, even when both are in the same call.
      expect(await planPhases(targetDb, requested)).to.deep.equal([
        new Set([ids.userId]),
        new Set([ids.partId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(requested);
      for (const id of requested)
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes a tree that contains both a geometry part and an element that uses it", async () => {
    const targetDb = createTargetDb("GeometryPartAndUserInOneTree");
    try {
      const ids = withEditTxn(targetDb, "insert subject", (txn) => {
        const subjectId = Subject.insert(txn, IModel.rootSubjectId, "Subject");
        const definitionModelId = DefinitionModel.insert(
          txn,
          subjectId,
          "Definition model"
        );
        const modelId = PhysicalModel.insert(txn, subjectId, "Physical model");
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Spatial category",
          new SubCategoryAppearance()
        );
        return {
          subjectId,
          ...insertGeometryPartUser(txn, targetDb, {
            definitionModelId,
            modelId,
            categoryId,
          }),
        };
      });
      const requested = new Set([ids.subjectId]);
      // The part's code is scoped by its definition model, so the part is a root of its own.
      expect(await planPhases(targetDb, requested)).to.deep.equal([
        new Set([ids.userId]),
        new Set([ids.subjectId, ids.partId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(requested);
      for (const id of [ids.subjectId, ids.partId, ids.userId])
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("deletes a view attachment before the view it shows", async () => {
    const targetDb = createTargetDb("ViewAttachmentInBatch");
    try {
      const ids = withEditTxn(targetDb, "insert sheet and view", (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Spatial category",
          new SubCategoryAppearance()
        );
        const view = insertSpatialView(
          txn,
          IModel.dictionaryId,
          modelId,
          categoryId
        );
        const documentListModelId = DocumentListModel.insert(
          txn,
          IModel.rootSubjectId,
          "Documents"
        );
        const sheetId = txn.insertElement({
          classFullName: Sheet.classFullName,
          model: documentListModelId,
          code: Code.createEmpty(),
          height: 1,
          width: 1,
        } as SheetProps);
        const sheetModelId = txn.insertModel({
          classFullName: SheetModel.classFullName,
          modeledElement: { id: sheetId },
        });
        const drawingCategoryId = DrawingCategory.insert(
          txn,
          IModel.dictionaryId,
          "Drawing category",
          new SubCategoryAppearance()
        );
        const attachmentId = txn.insertElement({
          classFullName: "BisCore:ViewAttachment",
          model: sheetModelId,
          category: drawingCategoryId,
          code: Code.createEmpty(),
          view: { id: view.viewId },
          placement: { origin: [0, 0], angle: 0 },
        } as ViewAttachmentProps);
        return { sheetId, attachmentId, viewId: view.viewId };
      });
      const requested = new Set([ids.sheetId, ids.viewId]);
      expect(await planPhases(targetDb, requested)).to.deep.equal([
        new Set([ids.sheetId]),
        new Set([ids.viewId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      await new IModelImporter(editTxn).deleteElements(requested);
      for (const id of [ids.sheetId, ids.attachmentId, ids.viewId])
        expect(targetDb.elements.tryGetElement(id)).to.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("keeps the display style and selectors that a view outside the batch uses", async () => {
    const targetDb = createTargetDb("ViewDefinitionsStillInUse");
    try {
      const ids = withEditTxn(targetDb, "insert view", (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Spatial category",
          new SubCategoryAppearance()
        );
        const view = insertSpatialView(
          txn,
          IModel.dictionaryId,
          modelId,
          categoryId
        );
        const independentId = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "Independent"
        );
        return { ...view, independentId };
      });
      const requested = new Set([
        ids.displayStyleId,
        ids.modelSelectorId,
        ids.categorySelectorId,
        ids.independentId,
      ]);
      const plan = await planBulkDelete(targetDb, requested);
      expect(plan.keptReferences).to.deep.equal(
        new Map([
          [ids.displayStyleId, ids.viewId],
          [ids.modelSelectorId, ids.viewId],
          [ids.categorySelectorId, ids.viewId],
        ])
      );
      expect(plan.phases).to.deep.equal([[ids.independentId]]);

      // Deleting the view with them needs one call, because native validation accepts a view definition's references
      // to definitions in the same call.
      expect(
        await planPhases(targetDb, new Set([...requested, ids.viewId]))
      ).to.have.length(1);
    } finally {
      targetDb.close();
    }
  });

  it("keeps a default sub-category requested without its category", async () => {
    const targetDb = createTargetDb("DefaultSubCategoryWithoutCategory");
    try {
      const ids = withEditTxn(targetDb, "insert category", (txn) => {
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Spatial category",
          new SubCategoryAppearance()
        );
        const independentId = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "Independent"
        );
        return {
          categoryId,
          defaultSubCategoryId: IModelDb.getDefaultSubCategoryId(categoryId),
          independentId,
        };
      });
      const plan = await planBulkDelete(
        targetDb,
        new Set([ids.defaultSubCategoryId, ids.independentId])
      );
      expect(plan.keptReferences).to.deep.equal(
        new Map([[ids.defaultSubCategoryId, ids.categoryId]])
      );
      expect(plan.phases).to.deep.equal([[ids.independentId]]);
    } finally {
      targetDb.close();
    }
  });

  it("throws when an element outside the batch uses a geometry part in it", async () => {
    const targetDb = createTargetDb("GeometryPartStillInUse");
    try {
      const ids = withEditTxn(targetDb, "insert part and user", (txn) => {
        const definitionModelId = DefinitionModel.insert(
          txn,
          IModel.rootSubjectId,
          "Definition model"
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "Physical model"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Spatial category",
          new SubCategoryAppearance()
        );
        return insertGeometryPartUser(txn, targetDb, {
          definitionModelId,
          modelId,
          categoryId,
        });
      });
      // Only native code can read which geometry uses a part, so the plan doesn't keep it and native validation refuses it.
      const plan = await planBulkDelete(targetDb, new Set([ids.partId]));
      expect(plan.keptReferences.size).to.equal(0);

      const editTxn = createStartedEditTxn(targetDb);
      await expectTransformerError(
        async () =>
          new IModelImporter(editTxn).deleteElements(new Set([ids.partId])),
        IModelTransformerError.ElementBulkDeleteFailed,
        `Bulk element deletion failed: status DeletionFailed, SQLite status ${DbResult.BE_SQLITE_OK}, failed element IDs: ${ids.partId}`
      );
      expect(targetDb.elements.tryGetElement(ids.partId)).to.not.be.undefined;
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("reports native failures without retrying individual roots", async () => {
    const targetDb = createTargetDb("NativeFailure");
    try {
      const independentId = withEditTxn(targetDb, "insert subject", (txn) =>
        Subject.insert(txn, IModel.rootSubjectId, "Independent")
      );
      const editTxn = createStartedEditTxn(targetDb);
      // Simulates a reference that the transformer does not check before deletion, such as one from a domain schema.
      const nativeDeleteSpy = vi
        .spyOn(editTxn, "deleteElements")
        .mockReturnValue({
          status: BulkDeleteElementsStatus.PartialSuccess,
          sqlDeleteStatus: DbResult.BE_SQLITE_OK,
          failedIds: new Set([independentId]),
        });
      const error = (await expectTransformerError(
        async () =>
          new IModelImporter(editTxn).deleteElements(new Set([independentId])),
        IModelTransformerError.ElementBulkDeleteFailed,
        `Bulk element deletion failed: status PartialSuccess, SQLite status ${DbResult.BE_SQLITE_OK}, failed element IDs: ${independentId}`
      )) as ElementBulkDeleteError;

      expect(error.status).to.equal(BulkDeleteElementsStatus.PartialSuccess);
      expect(error.sqlDeleteStatus).to.equal(DbResult.BE_SQLITE_OK);
      expect(error.failedIds).to.deep.equal(new Set([independentId]));
      expect(nativeDeleteSpy).toHaveBeenCalledOnce();
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });
});
