/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementRefersToElements,
  GeometryPart,
  GraphicalElement3dRepresentsElement,
  IModelDb,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64 } from "@itwin/core-bentley";
import {
  Code,
  GeometryPartProps,
  GeometryStreamBuilder,
  IModel,
  IModelError,
  PhysicalElementProps,
  RelationshipProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { assert, expect } from "chai";
import * as path from "path";
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

  it("forwards change-source options and preserves the default start changeset", async () => {
    const sourceDb = {
      changeset: { id: "current-changeset" },
      isBriefcaseDb: () => true,
    } as unknown as IModelDb;
    const changedInstanceIds = new ChangedInstanceIds(sourceDb);

    class TestExporter extends IModelExporter {
      public initializedWith?: ExporterInitOptions;

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
      startChangeset: { id: undefined },
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
    await expect(exporter.exportChanges()).to.be.rejectedWith(
      IModelError,
      "Cannot export changes because the source iModel has no changesets or custom changes. Call exportAll() to export all content."
    );

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
      public override async exportCodeSpecs(): Promise<void> {}
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
