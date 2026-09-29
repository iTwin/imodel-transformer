/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { DbResult, Id64String } from "@itwin/core-bentley";
import {
  BulkDeleteElementsStatus,
  DefinitionContainer,
  DefinitionModel,
  EditTxn,
  IModelDb,
  PhysicalModel,
  SpatialCategory,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import {
  Code,
  CodeScopeSpec,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { expect, vi } from "vitest";
import { planBulkDelete } from "../../ElementBulkDelete";
import {
  ElementBulkDeleteBlockedError,
  ElementBulkDeleteError,
  IModelImporter,
} from "../../IModelImporter";
import { IModelTransformerError } from "../../IModelTransformerError";
import {
  createStartedEditTxn,
  expectTransformerError,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

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

async function planPhases(
  db: IModelDb,
  elementIds: ReadonlySet<Id64String>
): Promise<Set<Id64String>[]> {
  const plan = await planBulkDelete(db, elementIds);
  expect(plan.blockedReferences.size).to.equal(0);
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
      expect(await planPhases(targetDb, roots)).to.deep.equal([
        new Set([ids.rootId, ids.childId]),
      ]);

      const editTxn = createStartedEditTxn(targetDb);
      const nativeDeleteSpy = vi.spyOn(editTxn, "deleteElements");
      await new IModelImporter(editTxn).deleteElements(roots);

      expect(new Set(nativeDeleteSpy.mock.calls[0][0])).to.deep.equal(
        new Set([ids.rootId, ids.childId])
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

  it("deletes nothing when an element outside the batch still uses a deleted category", async () => {
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
      const editTxn = createStartedEditTxn(targetDb);
      const nativeDeleteSpy = vi.spyOn(editTxn, "deleteElements");
      const error = (await expectTransformerError(
        async () =>
          new IModelImporter(editTxn).deleteElements(
            new Set([ids.categoryId, ids.deletedElementId, ids.independentId])
          ),
        IModelTransformerError.ElementBulkDeleteBlocked,
        `Bulk element deletion blocked: elements outside the deleted trees still reference ${ids.categoryId} (referenced by ${ids.survivorId})`
      )) as ElementBulkDeleteBlockedError;

      expect(error.blockedReferences).to.deep.equal(
        new Map([[ids.categoryId, ids.survivorId]])
      );
      expect(nativeDeleteSpy).not.toHaveBeenCalled();
      for (const id of [
        ids.categoryId,
        ids.defaultSubCategoryId,
        ids.deletedElementId,
        ids.survivorId,
        ids.independentId,
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
