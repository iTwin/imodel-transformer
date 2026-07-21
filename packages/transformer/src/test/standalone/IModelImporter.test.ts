/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as chai from "chai";
import { expect } from "chai";
import * as chaiAsPromised from "chai-as-promised";
import "./TransformerTestStartup";

chai.use(chaiAsPromised);
import {
  ElementOwnsMultiAspects,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Code, ElementAspectProps, IModel } from "@itwin/core-common";
import { Id64String } from "@itwin/core-bentley";
import { IModelImporter } from "../../IModelImporter";
import {
  createStartedEditTxn,
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

  it("importElementMultiAspects deletes surplus aspects via onDeleteElementAspect", async () => {
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
      const makeAspectProps = (): ElementAspectProps => ({
        classFullName: aspectClassFullName,
        element: new ElementOwnsMultiAspects(elementId),
      });

      const editTxn = createStartedEditTxn(targetDb);
      const importer = new IModelImporter(editTxn);

      await importer.importElementMultiAspects([
        makeAspectProps(),
        makeAspectProps(),
      ]);
      editTxn.saveChanges();
      expect(
        targetDb.elements.getAspects(elementId, aspectClassFullName).length,
        "two aspects should have been inserted"
      ).to.equal(2);

      // Re-import with one aspect: the surplus is removed via onDeleteElementAspect.
      await importer.importElementMultiAspects([makeAspectProps()]);
      editTxn.saveChanges();
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
      await expect(
        (importer as any).onInsertModel({
          classFullName: missing,
          modeledElement: { id: IModel.rootSubjectId },
        })
      ).to.be.rejectedWith(/not found in the target iModel/);
      await expect(
        (importer as any).onInsertElement({
          classFullName: missing,
          model: IModel.repositoryModelId,
          code: Code.createEmpty(),
        })
      ).to.be.rejectedWith(/not found in the target iModel/);
      await expect(
        (importer as any).onInsertElementAspect({
          classFullName: missing,
          element: { id: IModel.rootSubjectId },
        })
      ).to.be.rejectedWith(/not found in the target iModel/);
      await expect(
        (importer as any).onInsertRelationship({
          classFullName: missing,
          sourceId: IModel.rootSubjectId,
          targetId: IModel.rootSubjectId,
        })
      ).to.be.rejectedWith(/not found in the target iModel/);
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
      await expect(importer.importModel({} as any)).to.be.rejectedWith(
        /Model Id not provided/
      );
      await expect(
        (importer as any).onUpdateElement({
          classFullName: "BisCore:Subject",
        })
      ).to.be.rejectedWith(/ElementId not provided/);
      await expect(
        (importer as any).onUpdateRelationship({
          classFullName: "BisCore:ElementRefersToElements",
        })
      ).to.be.rejectedWith(/Relationship instance Id not provided/);
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });

  it("importElement requires an id when preserveElementIdsForFiltering is set", async () => {
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
      await expect(
        importer.importElement({
          classFullName: "BisCore:Subject",
          model: IModel.repositoryModelId,
          code: Code.createEmpty(),
        } as any)
      ).to.be.rejectedWith(/must be defined during a preserveIds operation/);
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });
});
