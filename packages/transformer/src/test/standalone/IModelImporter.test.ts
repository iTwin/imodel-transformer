/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "vitest";
import {
  ElementAspect,
  ElementOwnsExternalSourceAspects,
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
  ElementUniqueAspect,
  ExternalSourceAspect,
  StandaloneDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import {
  Code,
  ElementAspectProps,
  ExternalSourceAspectProps,
  IModel,
} from "@itwin/core-common";
import { Id64String } from "@itwin/core-bentley";
import { IModelImporter } from "../../IModelImporter";
import { IModelTransformerError } from "../../IModelTransformerError";
import {
  createStartedEditTxn,
  expectTransformerError,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

describe("IModelImporter", () => {
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

  it("aspect imports match existing aspects of the exact class only", async () => {
    const targetDb = StandaloneDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "IModelImporter",
        "ExactClassAspectMatching.bim"
      ),
      { rootSubject: { name: "ExactClassAspectMatching" } }
    );
    try {
      // Derived classes are declared first so their ECClassIds sort before
      // their base classes in polymorphic getAspects results.
      await targetDb.importSchemaStrings([
        `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="TestExactClassSchema" alias="tecs" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="DerivedMulti">
    <BaseClass>BaseMulti</BaseClass>
  </ECEntityClass>
  <ECEntityClass typeName="DerivedUnique">
    <BaseClass>BaseUnique</BaseClass>
  </ECEntityClass>
  <ECEntityClass typeName="BaseMulti">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
  <ECEntityClass typeName="BaseUnique">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
</ECSchema>`,
      ]);
      const baseMulti = "TestExactClassSchema:BaseMulti";
      const baseUnique = "TestExactClassSchema:BaseUnique";
      const { elementId, derivedMultiId, derivedUniqueId } = withEditTxn(
        targetDb,
        "insert derived aspects",
        (txn) => {
          const ownerId = Subject.insert(txn, IModel.rootSubjectId, "Owner");
          return {
            elementId: ownerId,
            derivedMultiId: txn.insertAspect({
              classFullName: "TestExactClassSchema:DerivedMulti",
              element: new ElementOwnsMultiAspects(ownerId),
              value: "derived",
            } as ElementAspectProps),
            derivedUniqueId: txn.insertAspect({
              classFullName: "TestExactClassSchema:DerivedUnique",
              element: new ElementOwnsUniqueAspect(ownerId),
              value: "derived",
            } as ElementAspectProps),
          };
        }
      );

      const editTxn = createStartedEditTxn(targetDb);
      const importer = new IModelImporter(editTxn);
      const [baseMultiId] = await importer.importElementMultiAspects([
        {
          classFullName: baseMulti,
          element: new ElementOwnsMultiAspects(elementId),
          value: "base",
        } as ElementAspectProps,
      ]);
      const baseUniqueId = await importer.importElementUniqueAspect({
        classFullName: baseUnique,
        element: new ElementOwnsUniqueAspect(elementId),
        value: "base",
      } as ElementAspectProps);
      editTxn.end();

      // The derived multi-aspect is left for its own class group.
      expect(baseMultiId).to.not.equal(derivedMultiId);
      // The derived unique aspect is not updated in place with base-class
      // properties. It is deleted because it occupies the same unique slot.
      expect(baseUniqueId).to.not.equal(derivedUniqueId);
      const values = targetDb.elements
        .getAspects(elementId)
        .map((aspect) => `${aspect.classFullName}=${aspect.asAny.value}`)
        .sort();
      expect(values).to.deep.equal([
        `${baseMulti}=base`,
        `${baseUnique}=base`,
        "TestExactClassSchema:DerivedMulti=derived",
      ]);
    } finally {
      targetDb.close();
    }
  });

  it("aspect cleanup deletes unretained aspects and preserves excluded and transformer provenance aspects", async () => {
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelImporter",
      "DeleteElementAspects.bim"
    );
    const targetDb = StandaloneDb.createEmpty(targetDbFile, {
      rootSubject: { name: "DeleteElementAspects" },
    });
    try {
      await targetDb.importSchemaStrings([
        `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="TestDeleteAspectsSchema" alias="tdas" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="TestUniqueAspect" modifier="Sealed">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
  </ECEntityClass>
  <ECEntityClass typeName="TestReplaceableUniqueAspect" modifier="Sealed">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
  </ECEntityClass>
  <ECEntityClass typeName="TestMultiAspect" modifier="Sealed">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
  </ECEntityClass>
</ECSchema>`,
      ]);
      const schemaEditTxn = createStartedEditTxn(targetDb);
      schemaEditTxn.saveChanges();
      schemaEditTxn.end();

      const { elementId, provenanceScopeId } = withEditTxn(
        targetDb,
        "insert aspect test data",
        (txn) => {
          const element = Subject.create(
            targetDb,
            IModel.rootSubjectId,
            "AspectHost"
          ).insert(txn);
          const provenanceScope = Subject.create(
            targetDb,
            IModel.rootSubjectId,
            "ProvenanceScope"
          ).insert(txn);
          return { elementId: element, provenanceScopeId: provenanceScope };
        }
      );
      const aspectIds = withEditTxn(
        targetDb,
        "insert target aspects",
        (txn) => ({
          excluded: txn.insertAspect({
            classFullName: "TestDeleteAspectsSchema:TestUniqueAspect",
            element: new ElementOwnsUniqueAspect(elementId),
          }),
          replaceableUnique: txn.insertAspect({
            classFullName:
              "TestDeleteAspectsSchema:TestReplaceableUniqueAspect",
            element: new ElementOwnsUniqueAspect(elementId),
          }),
          replaceable: txn.insertAspect({
            classFullName: "TestDeleteAspectsSchema:TestMultiAspect",
            element: new ElementOwnsMultiAspects(elementId),
          }),
          retained: txn.insertAspect({
            classFullName: "TestDeleteAspectsSchema:TestMultiAspect",
            element: new ElementOwnsMultiAspects(elementId),
          }),
          nonProvenance: txn.insertAspect({
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(elementId),
            scope: { id: IModel.rootSubjectId },
            identifier: "replaceable",
            kind: ExternalSourceAspect.Kind.Element,
          } as ExternalSourceAspectProps),
          provenance: txn.insertAspect({
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(elementId),
            scope: { id: provenanceScopeId },
            identifier: "provenance",
            kind: ExternalSourceAspect.Kind.Element,
          } as ExternalSourceAspectProps),
          scopeOwned: txn.insertAspect({
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(provenanceScopeId),
            scope: { id: IModel.rootSubjectId },
            identifier: "scope-owned",
            kind: ExternalSourceAspect.Kind.Scope,
          } as ExternalSourceAspectProps),
        })
      );

      const editTxn = createStartedEditTxn(targetDb);
      class TrackingImporter extends IModelImporter {
        public deletedAspectCount = 0;
        public deletedExternalSourceIdentifiers: string[] = [];

        protected override async onDeleteElementAspect(
          aspect: ElementAspect
        ): Promise<void> {
          this.deletedAspectCount++;
          if (aspect instanceof ExternalSourceAspect) {
            this.deletedExternalSourceIdentifiers.push(aspect.identifier);
          }
          await super.onDeleteElementAspect(aspect);
        }
      }
      const importer = new TrackingImporter(editTxn);
      const querySpy = vi.spyOn(targetDb, "createQueryReader");
      await importer.elementAspectCleanup.collect(
        new Set([elementId, provenanceScopeId]),
        new Set(["TestDeleteAspectsSchema:TestUniqueAspect"]),
        provenanceScopeId
      );
      importer.elementAspectCleanup.retain(aspectIds.retained);
      await importer.elementAspectCleanup.deleteUnretained();
      editTxn.saveChanges();

      // ExternalSourceAspect is a multi-aspect, so only that pass needs the provenance filter.
      const queries = querySpy.mock.calls.map(([ecsql]) => ecsql);
      const uniqueQueries = queries.filter((ecsql) =>
        ecsql.includes(`FROM ${ElementUniqueAspect.classFullName} aspect`)
      );
      expect(uniqueQueries).not.toHaveLength(0);
      for (const ecsql of uniqueQueries)
        expect(ecsql).not.toContain(ExternalSourceAspect.classFullName);
      querySpy.mockRestore();

      const hasAspect = (id: string) =>
        [elementId, provenanceScopeId].some((ownerId) =>
          targetDb.elements
            .getAspects(ownerId)
            .some((aspect) => aspect.id === id)
        );
      expect(hasAspect(aspectIds.excluded)).to.be.true;
      expect(hasAspect(aspectIds.replaceableUnique)).to.be.false;
      expect(hasAspect(aspectIds.replaceable)).to.be.false;
      expect(hasAspect(aspectIds.retained)).to.be.true;
      expect(hasAspect(aspectIds.nonProvenance)).to.be.false;
      expect(hasAspect(aspectIds.provenance)).to.be.true;
      expect(hasAspect(aspectIds.scopeOwned)).to.be.true;
      expect(importer.deletedAspectCount).to.equal(3);
      expect(importer.deletedExternalSourceIdentifiers).to.deep.equal([
        "replaceable",
      ]);
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
        }),
        IModelTransformerError.ElementIdRequired,
        "elementProps.id must be defined during a preserveIds operation"
      );
      await expectTransformerError(
        importer.importElement({
          id: "invalid",
          classFullName: "BisCore:SubCategory",
          model: IModel.dictionaryId,
          code: Code.createEmpty(),
        }),
        IModelTransformerError.InvalidSubCategory,
        "subcategory had invalid id"
      );
      await expectTransformerError(
        importer.importElement({
          id: "0x123",
          classFullName: "BisCore:SubCategory",
          model: IModel.dictionaryId,
          code: Code.createEmpty(),
        }),
        IModelTransformerError.InvalidSubCategory,
        "subcategory with id 0x123 had no parent"
      );
      editTxn.end("abandon");
    } finally {
      targetDb.close();
    }
  });
});
