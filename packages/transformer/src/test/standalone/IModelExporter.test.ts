/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementAspect,
  ElementMultiAspect,
  ElementOwnsExternalSourceAspects,
  ElementOwnsUniqueAspect,
  ElementRefersToElements,
  ElementUniqueAspect,
  ExternalSourceAspect,
  GeometryPart,
  GraphicalElement3dRepresentsElement,
  IModelDb,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  PhysicalPartition,
  Relationship,
  SnapshotDb,
  SpatialCategory,
  Subject,
  SubjectOwnsPartitionElements,
  withEditTxn,
} from "@itwin/core-backend";
import { GuidString, Id64, Id64String, ITwinError } from "@itwin/core-bentley";
import {
  Code,
  ExternalSourceAspectProps,
  GeometryPartProps,
  GeometryStreamBuilder,
  IModel,
  PhysicalElementProps,
  RelationshipProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { assert, expect, vi } from "vitest";
import * as path from "node:path";
import {
  ChangedInstanceIds,
  ExportChangesOptions,
  ExporterInitOptions,
  IModelExporter,
  IModelExportHandler,
} from "../../IModelExporter";
import { ElementAspectExportCoordinator } from "../../ElementAspectExportCoordinator";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "../../IModelTransformerError";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { ProvenanceManager } from "../../ProvenanceManager";
import { importElementAspectTestSchema } from "../TestUtils/ElementAspectTestUtils";
import { createBRepDataProps } from "../TestUtils/GeometryTestUtil";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";

export async function elementAspectExportExample(
  sourceDb: IModelDb,
  handler: IModelExportHandler
): Promise<void> {
  // __PUBLISH_EXTRACT_START__ ElementAspectProcessingExamples_exportAll.code
  const exporter = new IModelExporter(sourceDb);
  exporter.registerHandler(handler);
  await exporter.exportAll();
  // __PUBLISH_EXTRACT_END__
}

export function elementAspectHandlerExample(): IModelExportHandler {
  // __PUBLISH_EXTRACT_START__ ElementAspectProcessingExamples_handler.code
  class MyExportHandler extends IModelExportHandler {
    public override async shouldExportElementAspect(
      aspect: ElementAspect
    ): Promise<boolean> {
      return aspect.classFullName !== "Example:InternalAspect";
    }

    public override async onExportElementUniqueAspect(
      _aspect: ElementUniqueAspect,
      _isUpdate: boolean | undefined
    ): Promise<void> {
      // Transform or import the unique aspect.
    }

    public override async onExportElementMultiAspects(
      _aspects: ElementMultiAspect[]
    ): Promise<void> {
      // Process all multi-aspects in this owner group.
    }
  }
  return new MyExportHandler();
  // __PUBLISH_EXTRACT_END__
}

export function deletedElementAspectChangeExample(
  changes: ChangedInstanceIds,
  deletedAspectId: Id64String,
  owningElementId: Id64String
): void {
  // __PUBLISH_EXTRACT_START__ ElementAspectProcessingExamples_deletedChange.code
  changes.addCustomAspectChange("Deleted", deletedAspectId, owningElementId);
  // __PUBLISH_EXTRACT_END__
}

async function createBulkRelationshipTestData(fileName: string): Promise<{
  sourceDb: SnapshotDb;
  relClassFullName: string;
  relId: Id64String;
  sourceElemId: Id64String;
  targetElemId: Id64String;
}> {
  const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
    "IModelExporter",
    fileName
  );
  const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
    rootSubject: { name: "bulk-relationship-hydration" },
  });
  const relSchemaPath = IModelTransformerTestUtils.prepareOutputFile(
    "IModelExporter",
    "BulkRelHydrationSchema.ecschema.xml"
  );
  IModelJsFs.writeFileSync(
    relSchemaPath,
    `<?xml version="1.0" encoding="UTF-8"?>
    <ECSchema schemaName="BulkRelHydration" alias="brh" version="01.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
      <ECSchemaReference name="BisCore" version="01.00" alias="bis"/>
      <ECRelationshipClass typeName="RelWithProps" strength="referencing" modifier="None">
        <BaseClass>bis:ElementRefersToElements</BaseClass>
        <ECProperty propertyName="myString" typeName="string"/>
        <ECProperty propertyName="myDouble" typeName="double"/>
        <Source multiplicity="(0..*)" roleLabel="refers to" polymorphic="true">
          <Class class="bis:Element"/>
        </Source>
        <Target multiplicity="(0..*)" roleLabel="is referenced by" polymorphic="true">
          <Class class="bis:Element"/>
        </Target>
      </ECRelationshipClass>
    </ECSchema>`
  );
  await sourceDb.importSchemas([relSchemaPath]);

  const relClassFullName = "BulkRelHydration:RelWithProps";
  const { relId, sourceElemId, targetElemId } = withEditTxn(
    sourceDb,
    "insert elements and relationship",
    (txn) => {
      const categoryId = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "SpatialCategory",
        new SubCategoryAppearance()
      );
      const modelId = PhysicalModel.insert(
        txn,
        IModel.rootSubjectId,
        "PhysicalModel"
      );
      const physicalObjectProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: Code.createEmpty(),
      };
      const insertedSourceElemId = txn.insertElement(physicalObjectProps);
      const insertedTargetElemId = txn.insertElement(physicalObjectProps);
      const insertedRelId = txn.insertRelationship({
        classFullName: relClassFullName,
        sourceId: insertedSourceElemId,
        targetId: insertedTargetElemId,
        myString: "hello",
        myDouble: 3.14,
      } as RelationshipProps);
      return {
        relId: insertedRelId,
        sourceElemId: insertedSourceElemId,
        targetElemId: insertedTargetElemId,
      };
    }
  );
  return { sourceDb, relClassFullName, relId, sourceElemId, targetElemId };
}

describe("IModelExporter", () => {
  const outputDir = path.join(KnownTestLocations.outputDir, "IModelExporter");

  beforeAll(async () => {
    if (!IModelJsFs.existsSync(KnownTestLocations.outputDir)) {
      IModelJsFs.mkdirSync(KnownTestLocations.outputDir);
    }
    if (!IModelJsFs.existsSync(outputDir)) {
      IModelJsFs.mkdirSync(outputDir);
    }
  });

  it("exports aspects from exportAll and honors owner filtering", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelExporter",
      "ElementAspectExportAll.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "ElementAspectExportAll" },
    });
    try {
      const { includedElementId, excludedElementId } = withEditTxn(
        sourceDb,
        "insert aspect test data",
        (txn) => {
          const includedId = Subject.insert(
            txn,
            IModel.rootSubjectId,
            "Included"
          );
          const excludedId = Subject.insert(
            txn,
            IModel.rootSubjectId,
            "Excluded"
          );
          txn.insertAspect({
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(includedId),
            scope: { id: IModel.rootSubjectId },
            identifier: "included",
            kind: ExternalSourceAspect.Kind.Element,
          } as ExternalSourceAspectProps);
          txn.insertAspect({
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(excludedId),
            scope: { id: IModel.rootSubjectId },
            identifier: "excluded",
            kind: ExternalSourceAspect.Kind.Element,
          } as ExternalSourceAspectProps);
          return {
            includedElementId: includedId,
            excludedElementId: excludedId,
          };
        }
      );
      const exportedIdentifiers: string[] = [];
      const preparedOwnerBatchSizes: number[] = [];
      class Handler extends IModelExportHandler {
        public override async shouldExportElement(element: Element) {
          return element.id !== excludedElementId;
        }

        public override async onExportElementMultiAspects(
          aspects: ElementMultiAspect[]
        ) {
          exportedIdentifiers.push(
            ...aspects.map(
              (aspect) => (aspect as ExternalSourceAspect).identifier
            )
          );
        }
      }

      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(new Handler());
      const coordinator = exporter.elementAspectExportCoordinator;
      coordinator.setPreparation(async (_excludedClasses, elementIds) => {
        preparedOwnerBatchSizes.push(elementIds.size);
      });
      await exporter.exportElement(includedElementId);
      expect(exportedIdentifiers).to.deep.equal(["included"]);
      exportedIdentifiers.length = 0;
      await exporter.exportModelContents(IModel.repositoryModelId);
      expect(exportedIdentifiers).to.deep.equal(["included"]);
      exportedIdentifiers.length = 0;
      await exporter.exportModel(IModel.repositoryModelId);
      expect(exportedIdentifiers).to.deep.equal(["included"]);
      exportedIdentifiers.length = 0;
      await exporter.exportAll();

      expect(includedElementId).to.not.equal(excludedElementId);
      expect(exportedIdentifiers).to.deep.equal(["included"]);
      preparedOwnerBatchSizes.length = 0;
      exportedIdentifiers.length = 0;
      coordinator.begin(1);
      await exporter.exportChildElements(IModel.rootSubjectId);
      await coordinator.end();
      expect(preparedOwnerBatchSizes.length).to.be.greaterThan(0);
      expect(preparedOwnerBatchSizes.every((size) => size === 1)).to.be.true;
      expect(exportedIdentifiers).to.deep.equal(["included"]);
    } finally {
      vi.restoreAllMocks();
      sourceDb.close();
    }
  });

  it("processes explicit aspect owner sets in bounded groups", async () => {
    const preparedOwnerBatchSizes: number[] = [];
    const exportedOwnerBatchSizes: number[] = [];
    const coordinator = new ElementAspectExportCoordinator(
      1_000,
      () => new Set<string>(),
      async (ownerBatch) => {
        exportedOwnerBatchSizes.push(ownerBatch.size);
      }
    );
    coordinator.setPreparation(async (_excludedClasses, ownerBatch) => {
      preparedOwnerBatchSizes.push(ownerBatch.size);
    });
    const ownerIds = new Set<Id64String>();
    for (let index = 0; index < 2_001; index++) {
      ownerIds.add(Id64.fromLocalAndBriefcaseIds(index + 1, 0));
    }

    await coordinator.exportOwners(ownerIds);

    expect(preparedOwnerBatchSizes).to.deep.equal([1_000, 1_000, 1]);
    expect(exportedOwnerBatchSizes).to.deep.equal([1_000, 1_000, 1]);
  });

  it("deduplicates owners across scope batches and resets at outer scope boundaries", async () => {
    const ownerA = Id64.fromLocalAndBriefcaseIds(1, 0);
    const ownerB = Id64.fromLocalAndBriefcaseIds(2, 0);
    const exportedOwners: Id64String[] = [];
    let cacheResetCount = 0;
    const coordinator = new ElementAspectExportCoordinator(
      1,
      () => new Set<string>(),
      async (ownerBatch) => {
        exportedOwners.push(...ownerBatch);
      },
      () => cacheResetCount++
    );

    coordinator.begin(1);
    await coordinator.addAcceptedOwner(ownerA);
    coordinator.begin();
    await coordinator.addAcceptedOwner(ownerB);
    await coordinator.addAcceptedOwner(ownerA);
    await coordinator.end();
    await coordinator.end();
    expect(exportedOwners).to.deep.equal([ownerA, ownerB]);
    expect(cacheResetCount).to.equal(1);

    coordinator.begin(1);
    await coordinator.addAcceptedOwner(ownerA);
    await coordinator.end();
    expect(exportedOwners).to.deep.equal([ownerA, ownerB, ownerA]);
    expect(cacheResetCount).to.equal(2);

    await coordinator.exportOwners(new Set([ownerA]));
    await coordinator.exportOwners(new Set([ownerA]));
    expect(exportedOwners).to.deep.equal([
      ownerA,
      ownerB,
      ownerA,
      ownerA,
      ownerA,
    ]);
  });

  it("rebuilds unchanged unique aspects for changed owners", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelExporter",
      "ElementAspectUniqueRebuild.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "ElementAspectUniqueRebuild" },
    });
    try {
      await importElementAspectTestSchema(sourceDb);
      const ownerId = withEditTxn(sourceDb, "insert unique aspect", (txn) => {
        const id = Subject.insert(txn, IModel.rootSubjectId, "Owner");
        txn.insertAspect({
          classFullName: "ExporterAspectTest:UniqueAspect",
          element: new ElementOwnsUniqueAspect(id),
        });
        return id;
      });
      const exporter = new IModelExporter(sourceDb);
      const changes = new ChangedInstanceIds(sourceDb);
      changes.element.updateIds.add(ownerId);
      changes.model.updateIds.add(IModel.repositoryModelId);
      exporter["_sourceDbChanges"] = changes;
      exporter["_elementAspectExportProcessor"].setAspectChanges(
        changes.aspect
      );
      let exportedUniqueAspectCount = 0;
      let exportedUniqueAspectChange: boolean | undefined = false;
      class Handler extends IModelExportHandler {
        public override async onExportElementUniqueAspect(
          aspect: ElementUniqueAspect,
          isUpdate: boolean | undefined
        ) {
          if (aspect.classFullName === "ExporterAspectTest:UniqueAspect") {
            exportedUniqueAspectCount++;
            exportedUniqueAspectChange = isUpdate;
          }
        }
      }
      exporter.registerHandler(new Handler());

      await exporter.exportAll();

      expect(exportedUniqueAspectCount).to.equal(1);
      expect(exportedUniqueAspectChange).to.be.undefined;
    } finally {
      sourceDb.close();
    }
  });

  it("does not select root owners when root propagation is disabled", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelExporter",
      "RootAspectOwnerFiltering.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "RootAspectOwnerFiltering" },
    });
    try {
      const exporter = new IModelExporter(sourceDb);
      exporter["_skipPropagateChangesToRootElements"] = true;

      const elementIds = await exporter["filterOwnerElementIdsForAspectExport"](
        new Set([IModel.rootSubjectId])
      );

      expect(elementIds).to.be.instanceOf(Set);
      expect(elementIds.size).to.equal(0);
    } finally {
      sourceDb.close();
    }
  });

  it("does not select owners in rejected or template models", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelExporter",
      "ModelAspectOwnerFiltering.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "ModelAspectOwnerFiltering" },
    });
    try {
      const ids = withEditTxn(sourceDb, "insert model aspect owners", (txn) => {
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "Category",
          new SubCategoryAppearance()
        );
        const insertPartition = (name: string) =>
          txn.insertElement({
            classFullName: PhysicalPartition.classFullName,
            model: IModel.repositoryModelId,
            code: PhysicalPartition.createCode(
              sourceDb,
              IModel.rootSubjectId,
              name
            ),
            parent: new SubjectOwnsPartitionElements(IModel.rootSubjectId),
          });
        const rejectedPartitionId = insertPartition("RejectedPartition");
        const rejectedModelId = txn.insertModel({
          classFullName: PhysicalModel.classFullName,
          modeledElement: { id: rejectedPartitionId },
        });
        const templatePartitionId = insertPartition("TemplatePartition");
        const templateModelId = txn.insertModel({
          classFullName: PhysicalModel.classFullName,
          modeledElement: { id: templatePartitionId },
          isTemplate: true,
        });
        const insertOwner = (modelId: Id64String, name: string) => {
          const ownerId = txn.insertElement({
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: name,
          } as PhysicalElementProps);
          txn.insertAspect({
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(ownerId),
            scope: { id: IModel.rootSubjectId },
            identifier: name,
            kind: ExternalSourceAspect.Kind.Element,
          } as ExternalSourceAspectProps);
          return ownerId;
        };
        return {
          rejectedPartitionId,
          rejectedModelId,
          rejectedOwnerId: insertOwner(rejectedModelId, "RejectedOwner"),
          templateOwnerId: insertOwner(templateModelId, "TemplateOwner"),
        };
      });
      const exporter = new IModelExporter(sourceDb);
      class Handler extends IModelExportHandler {
        public override async shouldExportElement(element: Element) {
          return element.id !== ids.rejectedPartitionId;
        }
      }
      exporter.registerHandler(new Handler());
      exporter.wantTemplateModels = false;
      const elementIds = await exporter["filterOwnerElementIdsForAspectExport"](
        new Set([ids.rejectedOwnerId, ids.templateOwnerId])
      );

      expect(elementIds).to.be.instanceOf(Set);
      expect(elementIds.size).to.equal(0);
    } finally {
      sourceDb.close();
    }
  });

  it("prefilters changed aspect owners with shared structural queries", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelExporter",
      "AspectOwnerStructuralPrefilter.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "AspectOwnerStructuralPrefilter" },
    });
    try {
      const { ownerIds } = withEditTxn(
        sourceDb,
        "insert aspect owner hierarchy",
        (txn) => {
          const parentId = Subject.insert(
            txn,
            IModel.rootSubjectId,
            "SharedParent"
          );
          return {
            ownerIds: [
              Subject.insert(txn, parentId, "OwnerA"),
              Subject.insert(txn, parentId, "OwnerB"),
            ],
          };
        }
      );
      const exporter = new IModelExporter(sourceDb);
      for (const ownerId of ownerIds) {
        exporter.excludeElement(ownerId);
      }
      const getElement = vi.spyOn(sourceDb.elements, "getElement");
      const getModel = vi.spyOn(sourceDb.models, "getModel");
      const createQueryReader = vi.spyOn(sourceDb, "createQueryReader");

      const acceptedOwnerIds = await exporter[
        "filterOwnerElementIdsForAspectExport"
      ](new Set(ownerIds));

      expect(acceptedOwnerIds).to.deep.equal(new Set<Id64String>());
      expect(getElement.mock.calls.length).to.equal(0);
      expect(getModel.mock.calls.length).to.equal(0);
      const queries = createQueryReader.mock.calls.map((call) =>
        String(call[0])
      );
      expect(
        queries.filter((query) => query.includes("FROM bis.Element e"))
      ).to.have.lengthOf(3);
      expect(
        queries.filter((query) => query.includes("FROM bis.Model"))
      ).to.have.lengthOf(1);
    } finally {
      vi.restoreAllMocks();
      sourceDb.close();
    }
  });

  it("preserves custom exporter element-filter overrides for aspect owners", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelExporter",
      "AspectOwnerCustomFilter.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "AspectOwnerCustomFilter" },
    });
    try {
      const ownerId = withEditTxn(sourceDb, "insert aspect owner", (txn) =>
        Subject.insert(txn, IModel.rootSubjectId, "Owner")
      );
      let ownerFilterCalls = 0;
      class CustomExporter extends IModelExporter {
        public override async shouldExportElement(
          element: Element
        ): Promise<boolean> {
          if (element.id === ownerId) ownerFilterCalls++;
          return true;
        }
      }
      const exporter = new CustomExporter(sourceDb);
      exporter.excludeElement(ownerId);

      const acceptedOwnerIds = await exporter[
        "filterOwnerElementIdsForAspectExport"
      ](new Set([ownerId]));

      expect(acceptedOwnerIds).to.deep.equal(new Set([ownerId]));
      expect(ownerFilterCalls).to.equal(1);
    } finally {
      sourceDb.close();
    }
  });

  it("forwards change-source options and preserves the default start changeset", async () => {
    const sourceDb = {
      changeset: { id: "current-changeset" },
      isBriefcaseDb: () => true,
      createQueryReader: () => ({
        async *[Symbol.asyncIterator]() {},
      }),
    } as unknown as IModelDb;
    const changedInstanceIds = new ChangedInstanceIds(sourceDb);

    class TestExporter extends IModelExporter {
      public initializedWith?: ExporterInitOptions;

      public constructor(db: IModelDb) {
        super(db);
        this.registerHandler(new (class extends IModelExportHandler {})());
      }

      public override async initialize(
        options: ExporterInitOptions
      ): Promise<void> {
        this.initializedWith = options;
        this["_sourceDbChanges"] = new ChangedInstanceIds(this.sourceDb);
      }

      public override async exportCodeSpecs(): Promise<void> {}
      public override async exportFonts(): Promise<void> {}
      public override async exportModel(): Promise<void> {}
      public override async exportChildElements(): Promise<void> {}
      public override async exportModelContents(): Promise<void> {}
      public override async exportSubModels(): Promise<void> {}
      public override async exportRelationships(): Promise<void> {}
    }

    const optionsToTest: ExportChangesOptions[] = [
      {
        skipPropagateChangesToRootElements: true,
        startChangeset: { id: "start-changeset", index: 3 },
      },
      { skipPropagateChangesToRootElements: false, csFileProps: [] },
      {
        skipPropagateChangesToRootElements: true,
        changesetRanges: [[1, 2]],
      },
      { skipPropagateChangesToRootElements: false, changedInstanceIds },
    ];

    for (const options of optionsToTest) {
      const exporter = new TestExporter(sourceDb);
      await exporter.exportChanges(options);
      expect(exporter.initializedWith).to.equal(options);
    }

    const skipOnlyOptions: ExportChangesOptions = {
      skipPropagateChangesToRootElements: true,
    };
    const defaultExporter = new TestExporter(sourceDb);
    await defaultExporter.exportChanges(skipOnlyOptions);
    expect(defaultExporter.initializedWith).to.deep.equal({
      startChangeset: { id: "current-changeset" },
      ...skipOnlyOptions,
    });
  });

  it("exports schemas discovered by the canonical enumerator", async () => {
    const sourceDb = SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "IModelExporter",
        "SchemaEnumeration.bim"
      ),
      { rootSubject: { name: "SchemaEnumeration" } }
    );
    try {
      class Handler extends IModelExportHandler {
        public shouldExportCount = 0;
        public onExportCount = 0;

        public override async shouldExportSchema(): Promise<boolean> {
          ++this.shouldExportCount;
          return true;
        }

        public override async onExportSchema(): Promise<void> {
          ++this.onExportCount;
        }
      }

      const handler = new Handler();
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(handler);
      const enumeratedSchemas: string[] = [];
      for await (const schema of exporter.enumerateSchemas()) {
        enumeratedSchemas.push(schema.name);
      }

      await exporter.exportSchemas();
      expect(enumeratedSchemas.length).to.be.greaterThan(0);
      expect(handler.shouldExportCount).to.equal(enumeratedSchemas.length);
      expect(handler.onExportCount).to.equal(enumeratedSchemas.length);
    } finally {
      sourceDb.close();
    }
  });

  it("throws typed errors for sources that cannot export changes", async () => {
    class TestExporter extends IModelExporter {
      public exportAllCalled = false;

      public override async exportAll(): Promise<void> {
        this.exportAllCalled = true;
      }
    }

    const standaloneExporter = new TestExporter({
      isBriefcaseDb: () => false,
    } as unknown as IModelDb);
    try {
      await standaloneExporter.exportChanges();
      assert.fail("Expected exportChanges() to throw");
    } catch (error) {
      expect(
        ITwinError.isError(
          error,
          IModelTransformerErrorScope,
          IModelTransformerError.ExportChangesRequiresBriefcase
        )
      ).to.be.true;
      expect(error).to.have.property(
        "message",
        "Must be a briefcase to export changes"
      );
    }

    const sourceDb = {
      changeset: { id: "" },
      isBriefcaseDb: () => true,
    } as unknown as IModelDb;

    const exporter = new TestExporter(sourceDb);
    try {
      await exporter.exportChanges();
      assert.fail("Expected exportChanges() to throw");
    } catch (error) {
      expect(
        ITwinError.isError(
          error,
          IModelTransformerErrorScope,
          IModelTransformerError.NoChangesets
        )
      ).to.be.true;
      expect(error).to.have.property(
        "message",
        "Cannot export changes because the source iModel has no changesets or custom changes. Call exportAll() to export all content."
      );
    }

    expect(exporter.exportAllCalled).to.be.false;
  });

  it("exports caller-supplied changes when the source has no changesets", async () => {
    const sourceDb = {
      changeset: { id: "" },
      isBriefcaseDb: () => true,
    } as unknown as IModelDb;
    const changedInstanceIds = new ChangedInstanceIds(sourceDb);
    changedInstanceIds.element.insertIds.add("0x1");

    class TestExporter extends IModelExporter {
      public exportHookCalled = false;

      public override async exportAll(): Promise<void> {
        assert.fail("exportChanges() must not fall back to exportAll()");
      }

      public override async exportCodeSpecs(): Promise<void> {
        expect(this.sourceDbChanges).to.equal(changedInstanceIds);
        this.exportHookCalled = true;
      }
      public override async exportFonts(): Promise<void> {}
      public override async exportModel(): Promise<void> {}
      public override async exportChildElements(): Promise<void> {}
      public override async exportModelContents(): Promise<void> {}
      public override async exportSubModels(): Promise<void> {}
      public override async exportRelationships(): Promise<void> {}
    }

    const exporter = new TestExporter(sourceDb);
    await exporter.exportChanges({ changedInstanceIds });
    expect(exporter.exportHookCalled).to.be.true;
  });

  it("reports all source element deletions through one batch callback", async () => {
    const sourceDb = {
      changeset: { id: "" },
      isBriefcaseDb: () => true,
    } as unknown as IModelDb;
    const changedInstanceIds = new ChangedInstanceIds(sourceDb);
    changedInstanceIds.element.deleteIds.add("0x11");
    changedInstanceIds.element.deleteIds.add("0x12");

    class TestExporter extends IModelExporter {
      public override async exportCodeSpecs(): Promise<void> {}
      public override async exportFonts(): Promise<void> {}
      public override async exportModel(): Promise<void> {}
      public override async exportChildElements(): Promise<void> {}
      public override async exportModelContents(): Promise<void> {}
      public override async exportSubModels(): Promise<void> {}
      public override async exportRelationships(): Promise<void> {}
    }
    class Handler extends IModelExportHandler {
      public batches: ReadonlySet<Id64String>[] = [];

      public override async onDeleteElements(
        elementIds: ReadonlySet<Id64String>
      ): Promise<void> {
        this.batches.push(new Set(elementIds));
      }
    }

    const handler = new Handler();
    const exporter = new TestExporter(sourceDb);
    exporter.registerHandler(handler);
    await exporter.exportChanges({ changedInstanceIds });

    expect(handler.batches).to.deep.equal([new Set(["0x11", "0x12"])]);
  });

  it("export element with brep geometry", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelExporter",
      "RoundtripBrep.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "brep-roundtrip" },
    });

    const builder = new GeometryStreamBuilder();
    builder.appendBRepData(
      createBRepDataProps(
        Point3d.create(5, 10, 0),
        YawPitchRollAngles.createDegrees(45, 0, 0)
      )
    );

    const geomPartId = withEditTxn(sourceDb, "insert brep geom part", (txn) => {
      return txn.insertElement({
        classFullName: GeometryPart.classFullName,
        model: IModel.dictionaryId,
        code: Code.createEmpty(),
        geom: builder.geometryStream,
      } as GeometryPartProps);
    });

    assert(Id64.isValidId64(geomPartId));
    const geomPartInSource = sourceDb.elements.getElement<GeometryPart>(
      { id: geomPartId, wantGeometry: true, wantBRepData: true },
      GeometryPart
    );
    assert(geomPartInSource.geom?.[1]?.brep?.data !== undefined);

    const flatTargetDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelExporter",
      "RoundtripBrepTarget.bim"
    );
    const flatTargetDb = SnapshotDb.createEmpty(flatTargetDbPath, {
      rootSubject: sourceDb.rootSubject,
    });

    class TestFlatImportHandler extends IModelExportHandler {
      public override async onExportElement(elem: Element): Promise<void> {
        if (elem instanceof GeometryPart)
          withEditTxn(flatTargetDb, "insert exported element", (txn) => {
            txn.insertElement(elem.toJSON());
          });
      }
    }

    const exporter = new IModelExporter(sourceDb);
    exporter.registerHandler(new TestFlatImportHandler());
    exporter.wantGeometry = true;
    await exporter.exportAll();

    const geomPartInTarget = flatTargetDb.elements.getElement<GeometryPart>(
      { id: geomPartId, wantGeometry: true, wantBRepData: true },
      GeometryPart
    );
    assert(geomPartInTarget.geom?.[1]?.brep?.data !== undefined);

    sourceDb.close();
  });

  describe("exportRelationships", () => {
    it("should not export invalid relationships", async () => {
      const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
        "IModelExporter",
        "InvalidRelationship.bim"
      );
      const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
        rootSubject: { name: "invalid-relationships" },
      });

      const physicalObject1 = withEditTxn(
        sourceDb,
        "setup elements and relationships",
        (txn) => {
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "SpatialCategory",
            new SubCategoryAppearance()
          );
          const sourceModelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PhysicalModel"
          );
          const physicalObjectProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: sourceModelId,
            category: categoryId,
            code: Code.createEmpty(),
          };
          const obj1 = txn.insertElement(physicalObjectProps);
          const obj2 = txn.insertElement(physicalObjectProps);
          const obj3 = txn.insertElement(physicalObjectProps);
          const obj4 = txn.insertElement(physicalObjectProps);

          const invalidRelationshipsProps: RelationshipProps[] = [
            // target element will be deleted
            {
              classFullName: GraphicalElement3dRepresentsElement.classFullName,
              targetId: obj1,
              sourceId: obj2,
            },
            // target and source elements are invalid
            {
              classFullName: GraphicalElement3dRepresentsElement.classFullName,
              targetId: "",
              sourceId: "",
            },
            // only target element is invalid
            {
              classFullName: GraphicalElement3dRepresentsElement.classFullName,
              targetId: "",
              sourceId: obj3,
            },
            // only source element is invalid
            {
              classFullName: GraphicalElement3dRepresentsElement.classFullName,
              targetId: obj4,
              sourceId: "",
            },
          ];

          invalidRelationshipsProps.forEach((props) =>
            txn.insertRelationship(props)
          );

          return obj1;
        }
      );

      // this is used to substitute low level C++ functions the connectors would used to introduce invalid relationships.
      withEditTxn(sourceDb, "delete element via raw SQL", () => {
        sourceDb.withSqliteStatement(
          `DELETE FROM bis_Element WHERE Id = ${physicalObject1}`,
          (stmt) => stmt.next()
        );
      });

      const sourceRelationships = [];
      for await (const row of sourceDb.createQueryReader(
        "SELECT ECInstanceId FROM bis.ElementRefersToElements"
      )) {
        sourceRelationships.push(row);
      }
      expect(sourceRelationships.length).to.be.equal(4);

      const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
        "IModelTransformer",
        "relationships-Target.bim"
      );
      const targetDb = SnapshotDb.createEmpty(targetDbFile, {
        rootSubject: { name: "relationships-Target" },
      });

      const exporter = new IModelExporter(sourceDb);
      await exporter.exportRelationships(ElementRefersToElements.classFullName);

      const targetRelationships = [];
      for await (const row of targetDb.createQueryReader(
        "SELECT ECInstanceId FROM bis.ElementRefersToElements"
      )) {
        targetRelationships.push(row);
      }
      expect(
        targetRelationships.length,
        "TargetDb should not contain any invalid relationships"
      ).to.be.equal(0);

      sourceDb.close();
    });

    it("exports hydrated relationship instances identical to getInstance, with endpoint fedguids passed to the handler", async () => {
      const { sourceDb, relClassFullName, relId, sourceElemId, targetElemId } =
        await createBulkRelationshipTestData("BulkRelationshipHydration.bim");
      try {
        const exporter = new IModelExporter(sourceDb);
        const exported: Array<{
          relationship: Relationship;
          isUpdate: boolean | undefined;
          sourceFedGuid?: GuidString;
          targetFedGuid?: GuidString;
        }> = [];
        class CaptureHandler extends IModelExportHandler {
          public override async onExportRelationship(
            exportedRelationship: Relationship,
            exportedIsUpdate: boolean | undefined,
            handlerSourceFedGuid?: GuidString,
            handlerTargetFedGuid?: GuidString
          ): Promise<void> {
            exported.push({
              relationship: exportedRelationship,
              isUpdate: exportedIsUpdate,
              sourceFedGuid: handlerSourceFedGuid,
              targetFedGuid: handlerTargetFedGuid,
            });
          }
        }
        exporter.registerHandler(new CaptureHandler());
        await exporter.exportRelationships(
          ElementRefersToElements.classFullName
        );

        expect(exported.length).to.equal(1);
        const { relationship, isUpdate, sourceFedGuid, targetFedGuid } =
          exported[0];
        expect(isUpdate).to.equal(undefined);

        // the bulk-hydrated instance must match what relationships.getInstance would produce
        const viaGetInstance = sourceDb.relationships.getInstance(
          relClassFullName,
          relId
        );
        expect(relationship.toJSON()).to.deep.equal(viaGetInstance.toJSON());
        expect(relationship.classFullName).to.equal(relClassFullName);
        expect((relationship as any).myString).to.equal("hello");
        expect((relationship as any).myDouble).to.equal(3.14);

        // endpoint fedguids captured by the bulk query and passed to the handler must match per-element lookups
        assert(sourceFedGuid !== undefined);
        assert(targetFedGuid !== undefined);
        expect(sourceFedGuid).to.equal(
          sourceDb.elements.getFederationGuidFromId(sourceElemId)
        );
        expect(targetFedGuid).to.equal(
          sourceDb.elements.getFederationGuidFromId(targetElemId)
        );
      } finally {
        sourceDb.close();
      }
    });

    it("passes endpoint fedguids through targeted and changeset relationship exports", async () => {
      const { sourceDb, relClassFullName, relId, sourceElemId, targetElemId } =
        await createBulkRelationshipTestData("BulkRelationshipPaths.bim");
      try {
        const viaGetInstance = sourceDb.relationships.getInstance(
          relClassFullName,
          relId
        );
        const directExporter = new IModelExporter(sourceDb);
        let directRelationship: Relationship | undefined;
        let directSourceFedGuid: GuidString | undefined;
        let directTargetFedGuid: GuidString | undefined;
        directExporter.registerHandler(
          new (class extends IModelExportHandler {
            public override async onExportRelationship(
              exportedRelationship: Relationship,
              _isUpdate: boolean | undefined,
              sourceFedGuid?: GuidString,
              targetFedGuid?: GuidString
            ): Promise<void> {
              directRelationship = exportedRelationship;
              directSourceFedGuid = sourceFedGuid;
              directTargetFedGuid = targetFedGuid;
            }
          })()
        );
        const getInstance = vi.spyOn(sourceDb.relationships, "getInstance");
        await directExporter.exportRelationship(relClassFullName, relId);
        expect(getInstance).not.toHaveBeenCalled();
        getInstance.mockRestore();

        expect(directRelationship?.toJSON()).to.deep.equal(
          viaGetInstance.toJSON()
        );
        expect(directSourceFedGuid).to.equal(
          sourceDb.elements.getFederationGuidFromId(sourceElemId)
        );
        expect(directTargetFedGuid).to.equal(
          sourceDb.elements.getFederationGuidFromId(targetElemId)
        );

        const changedExporter = new IModelExporter(sourceDb);
        const changed = new ChangedInstanceIds(sourceDb);
        changed.relationship.insertIds.add(relId);
        changedExporter["_sourceDbChanges"] = changed;
        changedExporter.registerHandler(
          new (class extends IModelExportHandler {})()
        );
        const changedQueryReader = vi.spyOn(sourceDb, "createQueryReader");
        await changedExporter.exportRelationships(
          ElementRefersToElements.classFullName
        );
        expect(changedQueryReader.mock.calls[0][0]).toContain(
          "INNER JOIN IdSet(:changedRelationshipIds)"
        );
        changedQueryReader.mockRestore();

        class HookExporter extends IModelExporter {
          public hookCalls = 0;

          protected override async exportRelationshipInstance(
            exportedRelationship: Relationship,
            exportedIsUpdate: boolean | undefined,
            sourceFedGuid?: GuidString,
            targetFedGuid?: GuidString
          ): Promise<void> {
            this.hookCalls++;
            await super.exportRelationshipInstance(
              exportedRelationship,
              exportedIsUpdate,
              sourceFedGuid,
              targetFedGuid
            );
          }
        }
        const hookExporter = new HookExporter(sourceDb);
        hookExporter.registerHandler(
          new (class extends IModelExportHandler {})()
        );
        await hookExporter.exportRelationships(
          ElementRefersToElements.classFullName
        );
        await hookExporter.exportRelationship(relClassFullName, relId);
        expect(hookExporter.hookCalls).to.equal(2);
      } finally {
        sourceDb.close();
      }
    });

    it("uses keyset pagination for large relationship exports", async () => {
      const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
        "IModelExporter",
        "KeysetRelationshipPagination.bim"
      );
      const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
        rootSubject: { name: "keyset-relationship-pagination" },
      });
      try {
        const { sourceId, targetId } = withEditTxn(
          sourceDb,
          "insert relationships for keyset pagination",
          (txn) => {
            const categoryId = SpatialCategory.insert(
              txn,
              IModel.dictionaryId,
              "KeysetCategory",
              new SubCategoryAppearance()
            );
            const modelId = PhysicalModel.insert(
              txn,
              IModel.rootSubjectId,
              "KeysetModel"
            );
            const physicalObjectProps: PhysicalElementProps = {
              classFullName: PhysicalObject.classFullName,
              model: modelId,
              category: categoryId,
              code: Code.createEmpty(),
            };
            return {
              sourceId: txn.insertElement(physicalObjectProps),
              targetId: txn.insertElement(physicalObjectProps),
            };
          }
        );
        withEditTxn(sourceDb, "insert 2001 relationships", (txn) => {
          for (let index = 0; index < 2001; index++) {
            txn.insertRelationship({
              classFullName: GraphicalElement3dRepresentsElement.classFullName,
              sourceId,
              targetId,
            });
          }
        });

        const createQueryReader = vi.spyOn(sourceDb, "createQueryReader");
        const exportedRelationshipIds = new Set<Id64String>();
        const exporter = new IModelExporter(sourceDb);
        exporter.registerHandler(
          new (class extends IModelExportHandler {
            public override async onExportRelationship(
              relationship: Relationship
            ): Promise<void> {
              exportedRelationshipIds.add(relationship.id);
            }
          })()
        );

        await exporter.exportRelationships(
          GraphicalElement3dRepresentsElement.classFullName
        );

        expect(exportedRelationshipIds).to.have.lengthOf(2001);
        expect(createQueryReader).toHaveBeenCalledTimes(3);
        expect(createQueryReader.mock.calls[0][0]).not.toContain(
          ":lastRelationshipId"
        );
        expect(createQueryReader.mock.calls[1][0]).toContain(
          "r.ECInstanceId > :lastRelationshipId"
        );
        expect(createQueryReader.mock.calls[0][2]?.limit).to.deep.equal({
          count: 1000,
        });
        expect(createQueryReader.mock.calls[1][2]?.limit).to.deep.equal({
          count: 1000,
        });
        expect(createQueryReader.mock.calls[2][0]).toContain(
          "r.ECInstanceId > :lastRelationshipId"
        );
        expect(createQueryReader.mock.calls[2][2]?.limit).to.deep.equal({
          count: 1000,
        });
      } finally {
        sourceDb.close();
      }
    });

    it("skips the relationship query when changes contain only deletes", async () => {
      const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
        "IModelExporter",
        "DeletedRelationshipsOnly.bim"
      );
      const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
        rootSubject: { name: "deleted-relationships-only" },
      });
      try {
        const exporter = new IModelExporter(sourceDb);
        const changes = new ChangedInstanceIds(sourceDb);
        changes.relationship.deleteIds.add("0x123");
        exporter["_sourceDbChanges"] = changes;
        const createQueryReader = vi.spyOn(sourceDb, "createQueryReader");

        await exporter.exportRelationships(
          ElementRefersToElements.classFullName
        );

        expect(createQueryReader).not.toHaveBeenCalled();
      } finally {
        sourceDb.close();
      }
    });

    it("still excludes relationship classes on the bulk export path", async () => {
      const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
        "IModelExporter",
        "BulkRelationshipExclusion.bim"
      );
      const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
        rootSubject: { name: "bulk-relationship-exclusion" },
      });

      withEditTxn(sourceDb, "insert elements and relationships", (txn) => {
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "SpatialCategory",
          new SubCategoryAppearance()
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "PhysicalModel"
        );
        const physicalObjectProps: PhysicalElementProps = {
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: Code.createEmpty(),
        };
        const obj1 = txn.insertElement(physicalObjectProps);
        const obj2 = txn.insertElement(physicalObjectProps);
        txn.insertRelationship({
          classFullName: GraphicalElement3dRepresentsElement.classFullName,
          sourceId: obj1,
          targetId: obj2,
        });
      });

      const exporter = new IModelExporter(sourceDb);
      const exportedClassNames: string[] = [];
      class CaptureHandler extends IModelExportHandler {
        public override async onExportRelationship(
          relationship: Relationship
        ): Promise<void> {
          exportedClassNames.push(relationship.classFullName);
        }
      }
      exporter.registerHandler(new CaptureHandler());
      exporter.excludeRelationshipClass(
        GraphicalElement3dRepresentsElement.classFullName
      );
      await exporter.exportRelationships(ElementRefersToElements.classFullName);

      expect(exportedClassNames).to.deep.equal([]);

      sourceDb.close();
    });

    it("produces identical relationship provenance with and without known endpoints", async () => {
      const dbPath = IModelTransformerTestUtils.prepareOutputFile(
        "IModelExporter",
        "RelProvenanceKnownEndpoints.bim"
      );
      const db = SnapshotDb.createEmpty(dbPath, {
        rootSubject: { name: "rel-provenance-known-endpoints" },
      });

      const { relId, sourceElemId } = withEditTxn(
        db,
        "insert elements and relationship",
        (txn) => {
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "SpatialCategory",
            new SubCategoryAppearance()
          );
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "PhysicalModel"
          );
          const physicalObjectProps: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
          };
          const obj1 = txn.insertElement(physicalObjectProps);
          const obj2 = txn.insertElement(physicalObjectProps);
          const insertedRelId = txn.insertRelationship({
            classFullName: GraphicalElement3dRepresentsElement.classFullName,
            sourceId: obj1,
            targetId: obj2,
          });
          return { relId: insertedRelId, sourceElemId: obj1 };
        }
      );

      for (const isReverseSynchronization of [false, true]) {
        for (const forceOldRelationshipProvenanceMethod of [false, true]) {
          const baseArgs = {
            sourceDb: db,
            targetDb: db,
            isReverseSynchronization,
            targetScopeElementId: IModel.rootSubjectId,
            forceOldRelationshipProvenanceMethod,
          };
          const viaQuery =
            await ProvenanceManager.initRelationshipProvenanceOptions(
              relId,
              relId,
              baseArgs
            );
          const viaKnownEndpoints =
            await ProvenanceManager.initRelationshipProvenanceOptions(
              relId,
              relId,
              {
                ...baseArgs,
                knownEndpoints: {
                  sourceRelSourceElementId: sourceElemId,
                  targetRelSourceElementId: sourceElemId,
                },
              }
            );
          expect(viaKnownEndpoints).to.deep.equal(viaQuery);

          // when only the irrelevant endpoint is known, it must fall back to the query
          const viaIrrelevantEndpoint =
            await ProvenanceManager.initRelationshipProvenanceOptions(
              relId,
              relId,
              {
                ...baseArgs,
                knownEndpoints: isReverseSynchronization
                  ? { targetRelSourceElementId: sourceElemId }
                  : { sourceRelSourceElementId: sourceElemId },
              }
            );
          expect(viaIrrelevantEndpoint).to.deep.equal(viaQuery);
        }
      }

      db.close();
    });
  });
});
