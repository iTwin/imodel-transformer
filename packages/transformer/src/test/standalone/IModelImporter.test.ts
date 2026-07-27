/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "vitest";
import {
  ElementOwnsMultiAspects,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Code, ElementAspectProps, IModel } from "@itwin/core-common";
import { Id64String } from "@itwin/core-bentley";
import { IModelImporter } from "../../IModelImporter";
import { IModelTransformerError } from "../../IModelTransformerError";
import {
  createStartedEditTxn,
  expectTransformerError,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

describe("IModelImporter", () => {
  it("deleteElement skips elements in doNotUpdateElementIds (no-op guard)", async () => {
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelImporter",
      "DeleteElementGuard.bim"
    );
    const targetDb = StandaloneDb.createEmpty(targetDbFile, {
      rootSubject: { name: "DeleteElementGuard" },
    });
    try {
      const { protectedId, deletableId } = withEditTxn(
        targetDb,
        "insert subjects",
        (txn) => {
          const pId = Subject.create(
            targetDb,
            IModel.rootSubjectId,
            "Protected"
          ).insert(txn);
          const dId = Subject.create(
            targetDb,
            IModel.rootSubjectId,
            "Deletable"
          ).insert(txn);
          return { protectedId: pId, deletableId: dId };
        }
      );

      const editTxn = createStartedEditTxn(targetDb);
      // __PUBLISH_EXTRACT_START__ EditTxnInTransformer.custom-importer
      // IModelImporter derives targetDb from the EditTxn.
      const importer = new IModelImporter(editTxn);
      // __PUBLISH_EXTRACT_END__
      expect(importer.targetDb).to.equal(editTxn.iModel);
      importer.doNotUpdateElementIds.add(protectedId);

      await importer.deleteElement(protectedId);
      editTxn.saveChanges();
      expect(
        targetDb.elements.tryGetElement(protectedId),
        "guarded element should NOT be deleted"
      ).to.not.be.undefined;

      // Control: an element not in the set is actually deleted.
      await importer.deleteElement(deletableId);
      editTxn.saveChanges();
      expect(
        targetDb.elements.tryGetElement(deletableId),
        "unguarded element should be deleted"
      ).to.be.undefined;
      editTxn.end();
    } finally {
      targetDb.close();
    }
  });

  it("importElementMultiAspects preserves result order when deleting surplus aspects", async () => {
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelImporter",
      "DeleteElementAspect.bim"
    );
    const targetDb = StandaloneDb.createEmpty(targetDbFile, {
      rootSubject: { name: "DeleteElementAspect" },
    });
    try {
      const schema = `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="TestImporterSchema" alias="tis" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="TestMultiAspect" modifier="Sealed">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
  </ECEntityClass>
  <ECEntityClass typeName="OtherTestMultiAspect" modifier="Sealed">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
  </ECEntityClass>
</ECSchema>`;
      await targetDb.importSchemaStrings([schema]);

      const elementId: Id64String = withEditTxn(
        targetDb,
        "insert subject",
        (txn) => {
          return Subject.create(
            targetDb,
            IModel.rootSubjectId,
            "AspectHost"
          ).insert(txn);
        }
      );

      const aspectClassFullName = "TestImporterSchema:TestMultiAspect";
      const otherAspectClassFullName =
        "TestImporterSchema:OtherTestMultiAspect";
      const makeAspectProps = (classFullName: string): ElementAspectProps => ({
        classFullName,
        element: new ElementOwnsMultiAspects(elementId),
      });

      const editTxn = createStartedEditTxn(targetDb);
      const importer = new IModelImporter(editTxn);

      await importer.importElementMultiAspects([
        makeAspectProps(aspectClassFullName),
        makeAspectProps(aspectClassFullName),
        makeAspectProps(otherAspectClassFullName),
      ]);
      editTxn.saveChanges();
      const currentAspects = targetDb.elements.getAspects(
        elementId,
        aspectClassFullName
      );
      const currentOtherAspects = targetDb.elements.getAspects(
        elementId,
        otherAspectClassFullName
      );
      expect(
        currentAspects.length,
        "two aspects should have been inserted"
      ).to.equal(2);
      expect(
        currentOtherAspects.length,
        "one other aspect should have been inserted"
      ).to.equal(1);

      const result = await importer.importElementMultiAspects([
        makeAspectProps(otherAspectClassFullName),
        makeAspectProps(aspectClassFullName),
      ]);
      editTxn.saveChanges();
      expect(
        result,
        "ids should follow the proposed aspect order"
      ).to.deep.equal([currentOtherAspects[0].id, currentAspects[0].id]);
      expect(
        targetDb.elements.getAspects(elementId, aspectClassFullName).length,
        "surplus aspect should have been deleted"
      ).to.equal(1);
      editTxn.end();
    } finally {
      targetDb.close();
    }
  });

  it("insert write paths surface a helpful error when the class is missing from the target", async () => {
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelImporter",
      "MissingClass.bim"
    );
    const targetDb = StandaloneDb.createEmpty(targetDbFile, {
      rootSubject: { name: "MissingClass" },
    });
    try {
      const editTxn = createStartedEditTxn(targetDb);
      const importer = new IModelImporter(editTxn);
      const missing = "TestImporterSchema:DoesNotExist";
      const errors = await Promise.all([
        expectTransformerError(
          (importer as any).onInsertModel({
            classFullName: missing,
            modeledElement: { id: IModel.rootSubjectId },
          }),
          IModelTransformerError.TargetClassNotFound,
          `Model class "${missing}" not found in the target iModel. Was the latest version of the schema imported?`
        ),
        expectTransformerError(
          (importer as any).onInsertElement({
            classFullName: missing,
            model: IModel.repositoryModelId,
            code: Code.createEmpty(),
          }),
          IModelTransformerError.TargetClassNotFound,
          `Element class "${missing}" not found in the target iModel. Was the latest version of the schema imported?`
        ),
        expectTransformerError(
          (importer as any).onInsertElementAspect({
            classFullName: missing,
            element: { id: IModel.rootSubjectId },
          }),
          IModelTransformerError.TargetClassNotFound,
          `ElementAspect class "${missing}" not found in the target iModel. Was the latest version of the schema imported?`
        ),
        expectTransformerError(
          (importer as any).onInsertRelationship({
            classFullName: missing,
            sourceId: IModel.rootSubjectId,
            targetId: IModel.rootSubjectId,
          }),
          IModelTransformerError.TargetClassNotFound,
          `Relationship class "${missing}" not found in the target iModel. Was the latest version of the schema imported?`
        ),
      ]);
      for (const error of errors)
        expect(error).to.have.property("cause").that.is.instanceOf(Error);
      expect(
        await importer.importElementMultiAspects([]),
        "empty aspect array should be a no-op"
      ).to.deep.equal([]);
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("write-path guard clauses throw when required ids are missing", async () => {
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelImporter",
      "MissingIds.bim"
    );
    const targetDb = StandaloneDb.createEmpty(targetDbFile, {
      rootSubject: { name: "MissingIds" },
    });
    try {
      const editTxn = createStartedEditTxn(targetDb);
      const importer = new IModelImporter(editTxn);
      const invalidModelIdMessage =
        "Model Id not provided, should be the same as the ModeledElementId";
      await expectTransformerError(
        importer.importModel({} as any),
        IModelTransformerError.InvalidModelId,
        invalidModelIdMessage
      );
      await expectTransformerError(
        importer.importModel({ id: "invalid" } as any),
        IModelTransformerError.InvalidModelId,
        invalidModelIdMessage
      );
      await expectTransformerError(
        (importer as any).onUpdateElement({
          classFullName: "BisCore:Subject",
        }),
        IModelTransformerError.ElementIdRequired,
        "ElementId not provided"
      );
      await expectTransformerError(
        (importer as any).onUpdateRelationship({
          classFullName: "BisCore:ElementRefersToElements",
        }),
        IModelTransformerError.RelationshipIdRequired,
        "Relationship instance Id not provided"
      );
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("validates element and subcategory ids when preserveElementIdsForFiltering is set", async () => {
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelImporter",
      "PreserveIds.bim"
    );
    const targetDb = StandaloneDb.createEmpty(targetDbFile, {
      rootSubject: { name: "PreserveIds" },
    });
    try {
      const editTxn = createStartedEditTxn(targetDb);
      const importer = new IModelImporter(editTxn, {
        preserveElementIdsForFiltering: true,
      });
      await expectTransformerError(
        importer.importElement({
          classFullName: "BisCore:Subject",
          model: IModel.repositoryModelId,
          code: Code.createEmpty(),
        } as any),
        IModelTransformerError.ElementIdRequired,
        "elementProps.id must be defined during a preserveIds operation"
      );
      await expectTransformerError(
        importer.importElement({
          id: "invalid",
          classFullName: "BisCore:SubCategory",
          model: IModel.dictionaryId,
          code: Code.createEmpty(),
        } as any),
        IModelTransformerError.InvalidSubCategory,
        "subcategory had invalid id"
      );
      await expectTransformerError(
        importer.importElement({
          id: "0x123",
          classFullName: "BisCore:SubCategory",
          model: IModel.dictionaryId,
          code: Code.createEmpty(),
        } as any),
        IModelTransformerError.InvalidSubCategory,
        "subcategory with id 0x123 had no parent"
      );
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });
});
