/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  EditTxn,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  SubCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Guid } from "@itwin/core-bentley";
import {
  Code,
  CodeScopeSpec,
  CodeSpec,
  ElementProps,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";
import { expect } from "vitest";
import { IModelTransformer } from "../../IModelTransformer";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";

describe("In-place IModelTransformer", () => {
  it("should update Category elements", async () => {
    const iModelFile = IModelTransformerTestUtils.prepareOutputFile(
      "InPlaceIModelTransformer",
      "CategoryUpdate.bim"
    );
    const iModel = SnapshotDb.createEmpty(iModelFile, {
      rootSubject: { name: "CategoryUpdate" },
    });

    const { categoryId: insertedCategoryId, elementId: insertedElementId } =
      withEditTxn(iModel, "insert test data", (txn) => {
        const subjectId = Subject.insert(
          txn,
          IModel.rootSubjectId,
          "TestSubject"
        );
        const modelId = PhysicalModel.insert(txn, subjectId, "TestModel");
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "TestCategory",
          { invisible: false }
        );
        const elementId = txn.insertElement({
          category: categoryId,
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          code: Code.createEmpty(),
          federationGuid: Guid.createValue(),
          userLabel: "TestElement",
        } as PhysicalElementProps);
        return { categoryId, elementId };
      });
    const defaultSubCategoryId =
      IModelDb.getDefaultSubCategoryId(insertedCategoryId);

    class InPlaceTransformer extends IModelTransformer {
      public override async shouldExportElement(
        sourceElement: Element
      ): Promise<boolean> {
        this.context.remapElement(sourceElement.id, sourceElement.id);
        return true;
      }

      public override async onTransformElement(
        sourceElement: Element
      ): Promise<ElementProps> {
        const targetElementProps = await super.onTransformElement(
          sourceElement
        );
        targetElementProps.federationGuid = sourceElement.federationGuid;
        return targetElementProps;
      }
    }

    const editTxn = new EditTxn(iModel, "transform in place");
    editTxn.start();
    const transformer = new InPlaceTransformer(
      { source: iModel, target: editTxn },
      { noProvenance: true }
    );
    try {
      await transformer.process();
      editTxn.saveChanges();

      expect(
        iModel.elements.queryElementIdByCode(
          SpatialCategory.createCode(
            iModel,
            IModel.dictionaryId,
            "TestCategory"
          )
        )
      ).toBe(insertedCategoryId);
      expect(
        iModel.elements.getElement(defaultSubCategoryId).classFullName
      ).toBe(SubCategory.classFullName);
      expect(iModel.elements.getElement(insertedElementId).userLabel).toBe(
        "TestElement"
      );
    } finally {
      transformer.dispose();
      editTxn.end();
      iModel.close();
    }
  });

  it("should preserve CodeSpec remapping when cloning an identity-mapped element", async () => {
    const iModelFile = IModelTransformerTestUtils.prepareOutputFile(
      "InPlaceIModelTransformer",
      "CodeSpecRemapping.bim"
    );
    const iModel = SnapshotDb.createEmpty(iModelFile, {
      rootSubject: { name: "CodeSpecRemapping" },
    });
    const testData = withEditTxn(iModel, "insert test data", (txn) => {
      const sourceCodeSpecId = iModel.codeSpecs.insert(
        txn,
        CodeSpec.create(
          iModel,
          "ReviewSource",
          CodeScopeSpec.Type.RelatedElement
        )
      );
      const targetCodeSpecId = iModel.codeSpecs.insert(
        txn,
        CodeSpec.create(
          iModel,
          "ReviewTarget",
          CodeScopeSpec.Type.RelatedElement
        )
      );
      const subjectId = Subject.insert(
        txn,
        IModel.rootSubjectId,
        "TestSubject"
      );
      const modelId = PhysicalModel.insert(txn, subjectId, "TestModel");
      const categoryId = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "TestCategory",
        {}
      );
      const elementId = txn.insertElement({
        category: categoryId,
        classFullName: PhysicalObject.classFullName,
        code: new Code({
          scope: subjectId,
          spec: sourceCodeSpecId,
          value: "TestElement",
        }),
        model: modelId,
      } as PhysicalElementProps);
      return {
        categoryId,
        elementId,
        modelId,
        sourceCodeSpecId,
        targetCodeSpecId,
      };
    });

    const editTxn = new EditTxn(iModel, "transform in place");
    editTxn.start();
    const transformer = new IModelTransformer(
      { source: iModel, target: editTxn },
      { noProvenance: true }
    );
    try {
      transformer.context.remapElement(
        testData.categoryId,
        testData.categoryId
      );
      transformer.context.remapElement(testData.modelId, testData.modelId);
      transformer.context.remapElement(testData.elementId, testData.elementId);
      transformer.context.remapCodeSpec("ReviewSource", "ReviewTarget");

      const transformed = await transformer.onTransformElement(
        iModel.elements.getElement(testData.elementId)
      );

      expect(transformed.code.spec).toBe(testData.targetCodeSpecId);
      expect(transformed.code.spec).not.toBe(testData.sourceCodeSpecId);
      expect(transformed.code.value).toBe("TestElement");
    } finally {
      transformer.dispose();
      editTxn.end();
      iModel.close();
    }
  });

  it("should preserve code-scope remapping when cloning an identity-mapped element", async () => {
    const iModelFile = IModelTransformerTestUtils.prepareOutputFile(
      "InPlaceIModelTransformer",
      "CodeScopeRemapping.bim"
    );
    const iModel = SnapshotDb.createEmpty(iModelFile, {
      rootSubject: { name: "CodeScopeRemapping" },
    });
    const testData = withEditTxn(iModel, "insert test data", (txn) => {
      const sourceCodeSpecId = iModel.codeSpecs.insert(
        txn,
        CodeSpec.create(
          iModel,
          "ReviewScope",
          CodeScopeSpec.Type.RelatedElement
        )
      );
      const sourceScopeId = Subject.insert(
        txn,
        IModel.rootSubjectId,
        "SourceScope"
      );
      const targetScopeId = Subject.insert(
        txn,
        IModel.rootSubjectId,
        "TargetScope"
      );
      const modelId = PhysicalModel.insert(txn, sourceScopeId, "TestModel");
      const categoryId = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "TestCategory",
        {}
      );
      const elementId = txn.insertElement({
        category: categoryId,
        classFullName: PhysicalObject.classFullName,
        code: new Code({
          scope: sourceScopeId,
          spec: sourceCodeSpecId,
          value: "TestElement",
        }),
        model: modelId,
      } as PhysicalElementProps);
      return {
        categoryId,
        elementId,
        modelId,
        sourceCodeSpecId,
        sourceScopeId,
        targetScopeId,
      };
    });

    const editTxn = new EditTxn(iModel, "transform in place");
    editTxn.start();
    const transformer = new IModelTransformer(
      { source: iModel, target: editTxn },
      { noProvenance: true }
    );
    try {
      transformer.context.remapElement(
        testData.categoryId,
        testData.categoryId
      );
      transformer.context.remapElement(testData.modelId, testData.modelId);
      transformer.context.remapElement(testData.elementId, testData.elementId);
      transformer.context.remapElement(
        testData.sourceScopeId,
        testData.targetScopeId
      );

      const transformed = await transformer.onTransformElement(
        iModel.elements.getElement(testData.elementId)
      );

      expect(transformed.code.spec).toBe(testData.sourceCodeSpecId);
      expect(transformed.code.scope).toBe(testData.targetScopeId);
      expect(transformed.code.scope).not.toBe(testData.sourceScopeId);
      expect(transformed.code.value).toBe("TestElement");
    } finally {
      transformer.dispose();
      editTxn.end();
      iModel.close();
    }
  });
});
