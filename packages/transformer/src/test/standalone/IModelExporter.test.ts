/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  EditTxn,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementAspect,
  ElementMultiAspect,
  ElementOwnsExternalSourceAspects,
  ElementOwnsMultiAspects,
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
  SnapshotDb,
  SpatialCategory,
  Subject,
  SubjectOwnsPartitionElements,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64, Id64String, ITwinError } from "@itwin/core-bentley";
import {
  Code,
  ElementAspectProps,
  ExternalSourceAspectProps,
  GeometryPartProps,
  GeometryStreamBuilder,
  IModel,
  PhysicalElementProps,
  RelationshipProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { assert, expect } from "chai";
import * as path from "path";
import * as sinon from "sinon";
import {
  ChangedInstanceIds,
  ExportChangesOptions,
  ExporterInitOptions,
  IModelExporter,
  IModelExportHandler,
} from "../../IModelExporter";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { createBRepDataProps } from "../TestUtils/GeometryTestUtil";
import { KnownTestLocations } from "../TestUtils/KnownTestLocations";

import "./TransformerTestStartup"; // calls startup/shutdown IModelHost before/after all tests

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

describe("IModelExporter", () => {
  const outputDir = path.join(KnownTestLocations.outputDir, "IModelExporter");

  before(async () => {
    if (!IModelJsFs.existsSync(KnownTestLocations.outputDir)) {
      IModelJsFs.mkdirSync(KnownTestLocations.outputDir);
    }
    if (!IModelJsFs.existsSync(outputDir)) {
      IModelJsFs.mkdirSync(outputDir);
    }
  });

  async function importAspectTestSchema(db: IModelDb): Promise<void> {
    await db.importSchemaStrings([
      `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="ExporterAspectTest" alias="eat" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="UniqueAspect" modifier="Sealed">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
  </ECEntityClass>
  <ECEntityClass typeName="MultiAspectA" modifier="Sealed">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
  </ECEntityClass>
  <ECEntityClass typeName="MultiAspectB" modifier="Sealed">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
  </ECEntityClass>
</ECSchema>`,
    ]);
    const editTxn = new EditTxn(db, "import aspect test schema");
    editTxn.start();
    editTxn.saveChanges();
    editTxn.end();
  }

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
      const createQueryReader = sinon.spy(sourceDb, "createQueryReader");
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
      expect(preparedOwnerBatchSizes.at(-1)).to.equal(0);
      expect(preparedOwnerBatchSizes.length).to.be.greaterThan(1);
      expect(preparedOwnerBatchSizes.slice(0, -1).every((size) => size === 1))
        .to.be.true;
      expect(exportedIdentifiers).to.deep.equal(["included"]);
      expect(
        createQueryReader
          .getCalls()
          .filter((call) =>
            String(call.args[0]).includes("ECDbMeta.ClassHasAllBaseClasses")
          )
      ).to.have.lengthOf(2);
    } finally {
      sinon.restore();
      sourceDb.close();
    }
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
      await importAspectTestSchema(sourceDb);
      const ownerId = withEditTxn(sourceDb, "insert unique aspect", (txn) => {
        const id = Subject.insert(txn, IModel.rootSubjectId, "Owner");
        txn.insertAspect({
          classFullName: "ExporterAspectTest:UniqueAspect",
          element: new ElementOwnsUniqueAspect(id),
        } as ElementAspectProps);
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

  it("keeps every multi-aspect batch scoped to one owner", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelExporter",
      "ElementAspectBatchOwners.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "ElementAspectBatchOwners" },
    });
    try {
      await importAspectTestSchema(sourceDb);
      const ownerIds = withEditTxn(
        sourceDb,
        "insert multi-aspect owners",
        (txn) => {
          const ids = [
            Subject.insert(txn, IModel.rootSubjectId, "OwnerA"),
            Subject.insert(txn, IModel.rootSubjectId, "OwnerB"),
          ];
          for (const id of ids) {
            for (const classFullName of [
              "ExporterAspectTest:MultiAspectA",
              "ExporterAspectTest:MultiAspectB",
            ]) {
              txn.insertAspect({
                classFullName,
                element: new ElementOwnsMultiAspects(id),
              } as ElementAspectProps);
              txn.insertAspect({
                classFullName,
                element: new ElementOwnsMultiAspects(id),
              } as ElementAspectProps);
            }
          }
          return ids;
        }
      );
      const batches: Id64String[][] = [];
      let exportedAspectCount = 0;
      class Handler extends IModelExportHandler {
        public override async onExportElementMultiAspects(
          aspects: ElementMultiAspect[]
        ) {
          const batchOwnerIds = [
            ...new Set(aspects.map((aspect) => aspect.element.id)),
          ];
          batches.push(batchOwnerIds);
          exportedAspectCount += aspects.length;
        }
      }
      const exporter = new IModelExporter(sourceDb);
      exporter.registerHandler(new Handler());

      await exporter.exportAll();

      expect(exportedAspectCount).to.equal(8);
      expect(batches.length).to.be.greaterThan(0);
      expect(batches.every((batch) => batch.length === 1)).to.be.true;
      expect(new Set(batches.flat())).to.deep.equal(new Set(ownerIds));
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
      const changes = new ChangedInstanceIds(sourceDb);
      changes.element.updateIds.add(IModel.rootSubjectId);
      exporter["_sourceDbChanges"] = changes;
      exporter["_skipPropagateChangesToRootElements"] = true;

      const elementIds =
        await exporter["getChangedElementIdsForAspectExport"]();

      expect(elementIds).to.be.instanceOf(Set);
      expect(elementIds?.size).to.equal(0);
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
      const changes = new ChangedInstanceIds(sourceDb);
      changes.element.updateIds.add(ids.rejectedOwnerId);
      changes.element.updateIds.add(ids.templateOwnerId);
      exporter["_sourceDbChanges"] = changes;
      exporter["_elementAspectExportProcessor"].setAspectChanges(
        changes.aspect
      );

      const elementIds =
        await exporter["getChangedElementIdsForAspectExport"]();

      expect(elementIds).to.be.instanceOf(Set);
      expect(elementIds?.size).to.equal(0);
    } finally {
      sourceDb.close();
    }
  });

  it("does not retain rejected element decisions during a scoped batch", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "IModelExporter",
      "RejectedElementDecisionCache.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "RejectedElementDecisionCache" },
    });
    try {
      withEditTxn(sourceDb, "insert rejected siblings", (txn) => {
        for (let index = 0; index < 25; index++) {
          Subject.insert(txn, IModel.rootSubjectId, `Rejected${index}`);
        }
      });
      const exporter = new IModelExporter(sourceDb);
      class Handler extends IModelExportHandler {
        public override async shouldExportElement(_element: Element) {
          return false;
        }
      }
      exporter.registerHandler(new Handler());
      const coordinator = exporter.elementAspectExportCoordinator;
      coordinator.begin(10);
      await exporter.exportChildElements(IModel.rootSubjectId);

      expect(coordinator.acceptedOwnerDecisionCount).to.equal(0);
      coordinator.abort();
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

  it("throws instead of falling back to exportAll when the source has no changesets", async () => {
    const sourceDb = {
      changeset: { id: "" },
      isBriefcaseDb: () => true,
    } as unknown as IModelDb;

    class TestExporter extends IModelExporter {
      public exportAllCalled = false;

      public override async exportAll(): Promise<void> {
        this.exportAllCalled = true;
      }
    }

    const exporter = new TestExporter(sourceDb);
    try {
      await exporter.exportChanges();
      assert.fail("Expected exportChanges() to throw");
    } catch (error) {
      expect(
        ITwinError.isError(error, "@itwin/imodel-transformer", "no-changesets")
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
    await expect(exporter.exportChanges({ changedInstanceIds })).to.eventually
      .be.fulfilled;
    expect(exporter.exportHookCalled).to.be.true;
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
    await expect(exporter.exportAll()).to.eventually.be.fulfilled;

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
      await expect(
        exporter.exportRelationships(ElementRefersToElements.classFullName)
      ).to.eventually.be.fulfilled;

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
  });
});
