/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect, vi } from "vitest";
import {
  ElementAspect,
  ElementOwnsExternalSourceAspects,
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
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
import { isTransformerProvenanceAspect } from "../../ProvenanceManager";
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

  it("aspect import options protect filtered aspects and update known target aspects", async () => {
    const targetDb = StandaloneDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "IModelImporter",
        "AspectImportOptions.bim"
      ),
      { rootSubject: { name: "AspectImportOptions" } }
    );
    try {
      await targetDb.importSchemaStrings([
        `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="ImportOptions" alias="io" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="UniqueValue" modifier="Sealed">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
  <ECEntityClass typeName="MultiValue" modifier="Sealed">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
</ECSchema>`,
      ]);
      const ownerId = withEditTxn(targetDb, "insert subject", (txn) =>
        Subject.create(targetDb, IModel.rootSubjectId, "Owner").insert(txn)
      );
      const unique = (value: string): ElementAspectProps =>
        ({
          classFullName: "ImportOptions:UniqueValue",
          element: new ElementOwnsUniqueAspect(ownerId),
          value,
        }) as ElementAspectProps;
      const multi = (value: string, id?: Id64String): ElementAspectProps =>
        ({
          classFullName: "ImportOptions:MultiValue",
          element: new ElementOwnsMultiAspects(ownerId),
          value,
          id,
        }) as ElementAspectProps;
      const values = (classFullName: string) =>
        targetDb.elements
          .getAspects(ownerId, classFullName)
          .map((a) => `${a.id}=${a.asAny.value}`);

      const editTxn = createStartedEditTxn(targetDb);
      const importer = new IModelImporter(editTxn);
      const uniqueId = await importer.importElementUniqueAspect(unique("u"));
      const [aId, bId] = await importer.importElementMultiAspects([
        multi("a"),
        multi("b"),
      ]);

      // A rejected aspect holding the unique slot is kept; nothing is imported.
      expect(
        await importer.importElementUniqueAspect(unique("u2"), {
          filter: (a) => a.id !== uniqueId,
        })
      ).to.equal(undefined);
      expect(values("ImportOptions:UniqueValue")).to.deep.equal([
        `${uniqueId}=u`,
      ]);

      // A rejected multi-aspect is neither matched nor deleted.
      expect(
        await importer.importElementMultiAspects([multi("x")], {
          filter: (a) => a.id !== aId,
        })
      ).to.deep.equal([bId]);
      expect(values("ImportOptions:MultiValue")).to.deep.equal([
        `${aId}=a`,
        `${bId}=x`,
      ]);

      // A known target aspect is updated in place; a stray incoming props.id is ignored.
      expect(
        await importer.importElementMultiAspects([multi("b2", aId)], {
          targetAspectIds: [bId],
        })
      ).to.deep.equal([bId]);
      expect(values("ImportOptions:MultiValue")).to.deep.equal([`${bId}=b2`]);
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

  it("aspect imports inside an owner batch read loaded aspects and see their own writes", async () => {
    const targetDb = StandaloneDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "IModelImporter",
        "BatchedAspectReads.bim"
      ),
      { rootSubject: { name: "BatchedAspectReads" } }
    );
    try {
      await targetDb.importSchemaStrings([
        `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="TestBatchedReadsSchema" alias="tbrs" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="BatchMulti">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
  <ECEntityClass typeName="BatchUnique">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
</ECSchema>`,
      ]);
      const multi = "TestBatchedReadsSchema:BatchMulti";
      const unique = "TestBatchedReadsSchema:BatchUnique";
      const elementId = withEditTxn(targetDb, "insert owner", (txn) =>
        Subject.insert(txn, IModel.rootSubjectId, "Owner")
      );
      const multiProps = (value: string) =>
        ({
          classFullName: multi,
          element: new ElementOwnsMultiAspects(elementId),
          value,
        }) as ElementAspectProps;
      const uniqueProps = (value: string) =>
        ({
          classFullName: unique,
          element: new ElementOwnsUniqueAspect(elementId),
          value,
        }) as ElementAspectProps;

      const editTxn = createStartedEditTxn(targetDb);
      const importer = new IModelImporter(editTxn);
      const cleanup = importer.elementAspectCleanup;
      // A failed collect leaves no partially loaded batch behind.
      const queryAspects = vi
        .spyOn(targetDb.elements, "queryAspects")
        .mockImplementationOnce(() => {
          throw new Error("query failed");
        });
      await expect(
        cleanup.collect(new Set([elementId]), new Set<string>())
      ).rejects.toThrow("query failed");
      queryAspects.mockRestore();
      expect(cleanup.getAspects(elementId, multi)).to.equal(undefined);

      const getAspects = vi.spyOn(targetDb.elements, "getAspects");
      await cleanup.collect(new Set([elementId]), new Set<string>());

      // The first reads come from the batch's loaded aspects.
      const [firstMultiId] = await importer.importElementMultiAspects([
        multiProps("first"),
      ]);
      expect(getAspects).not.toHaveBeenCalled();
      // After writing to the owner, reads go to the target and see the insert.
      const firstUniqueId = await importer.importElementUniqueAspect(
        uniqueProps("first")
      );
      const [secondMultiId] = await importer.importElementMultiAspects([
        multiProps("second"),
      ]);
      const secondUniqueId = await importer.importElementUniqueAspect(
        uniqueProps("second")
      );
      expect(getAspects).toHaveBeenCalled();
      getAspects.mockRestore();
      expect(secondMultiId).to.equal(firstMultiId);
      expect(secondUniqueId).to.equal(firstUniqueId);

      await cleanup.deleteUnretained();
      expect(cleanup.getAspects(elementId, multi)).to.equal(undefined);
      editTxn.end();

      const values = targetDb.elements
        .getAspects(elementId)
        .map((aspect) => `${aspect.classFullName}=${aspect.asAny.value}`)
        .sort();
      expect(values).to.deep.equal([`${multi}=second`, `${unique}=second`]);
    } finally {
      targetDb.close();
    }
  });

  it.each([
    { writtenBy: "the base hook", direct: false },
    { writtenBy: "EditTxn directly", direct: true },
  ])(
    "aspect imports see another owner's aspect written by $writtenBy during a hook",
    async ({ direct }) => {
      const targetDb = StandaloneDb.createEmpty(
        IModelTransformerTestUtils.prepareOutputFile(
          "IModelImporter",
          `CrossOwnerAspectWrites-${direct ? "direct" : "hook"}.bim`
        ),
        { rootSubject: { name: "CrossOwnerAspectWrites" } }
      );
      try {
        await targetDb.importSchemaStrings([
          `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="TestCrossOwnerSchema" alias="tcos" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="CrossMulti">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
</ECSchema>`,
        ]);
        const multi = "TestCrossOwnerSchema:CrossMulti";
        const [ownerA, ownerB] = withEditTxn(
          targetDb,
          "insert owners",
          (txn) => [
            Subject.insert(txn, IModel.rootSubjectId, "OwnerA"),
            Subject.insert(txn, IModel.rootSubjectId, "OwnerB"),
          ]
        );
        const props = (elementId: Id64String) =>
          ({
            classFullName: multi,
            element: new ElementOwnsMultiAspects(elementId),
            value: "value",
          }) as ElementAspectProps;

        // Inserting owner A's aspect also inserts owner B's.
        class CrossOwnerImporter extends IModelImporter {
          protected override async onInsertElementAspect(
            aspectProps: ElementAspectProps
          ): Promise<Id64String> {
            if (aspectProps.element.id === ownerA) {
              if (direct) this.editTxn.insertAspect(props(ownerB));
              else await super.onInsertElementAspect(props(ownerB));
            }
            return super.onInsertElementAspect(aspectProps);
          }
        }
        const editTxn = createStartedEditTxn(targetDb);
        const importer = new CrossOwnerImporter(editTxn);
        await importer.elementAspectCleanup.collect(
          new Set([ownerA, ownerB]),
          new Set<string>()
        );
        await importer.importElementMultiAspects([props(ownerA)]);
        const [ownerBAspectId] = await importer.importElementMultiAspects([
          props(ownerB),
        ]);
        editTxn.end();

        // Owner B's import sees the aspect the hook inserted instead of adding another.
        const ownerBAspects = targetDb.elements.getAspects(ownerB, multi);
        expect(ownerBAspects.map((aspect) => aspect.id)).to.deep.equal([
          ownerBAspectId,
        ]);
      } finally {
        targetDb.close();
      }
    }
  );

  it("aspect imports inside an owner batch see aspects written directly through EditTxn", async () => {
    const targetDb = StandaloneDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "IModelImporter",
        "DirectAspectWrites.bim"
      ),
      { rootSubject: { name: "DirectAspectWrites" } }
    );
    try {
      await targetDb.importSchemaStrings([
        `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="TestDirectWriteSchema" alias="tdws" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="DirectMulti">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
    <ECProperty propertyName="Value" typeName="string"/>
  </ECEntityClass>
</ECSchema>`,
      ]);
      const multi = "TestDirectWriteSchema:DirectMulti";
      const elementId = withEditTxn(targetDb, "insert owner", (txn) =>
        Subject.insert(txn, IModel.rootSubjectId, "Owner")
      );
      const props = (value: string) =>
        ({
          classFullName: multi,
          element: new ElementOwnsMultiAspects(elementId),
          value,
        }) as ElementAspectProps;

      const editTxn = createStartedEditTxn(targetDb);
      const importer = new IModelImporter(editTxn);
      await importer.elementAspectCleanup.collect(
        new Set([elementId]),
        new Set<string>()
      );
      // Written after the batch loaded its aspects, without the importer.
      const directId = editTxn.insertAspect(props("direct"));
      const [importedId] = await importer.importElementMultiAspects([
        props("imported"),
      ]);
      editTxn.end();

      // The import matched the direct aspect instead of adding a second one.
      expect(importedId).to.equal(directId);
      expect(
        targetDb.elements
          .getAspects(elementId, multi)
          .map((aspect) => `${aspect.id}=${aspect.asAny.value}`)
      ).to.deep.equal([`${directId}=imported`]);
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
          scopedDocument: txn.insertAspect({
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(elementId),
            scope: { id: provenanceScopeId },
            identifier: "scoped-document",
            kind: "Document",
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
        (aspect) =>
          isTransformerProvenanceAspect(aspect, new Set([provenanceScopeId]))
      );
      importer.elementAspectCleanup.retain(aspectIds.retained);
      await importer.elementAspectCleanup.deleteUnretained();
      editTxn.saveChanges();

      // Cleanup applies the caller's protection to the aspects it loads, so
      // its candidate queries know nothing about provenance.
      for (const [ecsql] of querySpy.mock.calls)
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
      // Only element, relationship, and scope provenance kinds are preserved.
      expect(hasAspect(aspectIds.scopedDocument)).to.be.false;
      expect(importer.deletedAspectCount).to.equal(4);
      expect(importer.deletedExternalSourceIdentifiers.sort()).to.deep.equal([
        "replaceable",
        "scoped-document",
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
