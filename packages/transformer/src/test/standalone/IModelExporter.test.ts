/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { Element, ElementRefersToElements, GeometryPart, GraphicalElement3dRepresentsElement, IModelJsFs, PhysicalModel, PhysicalObject, SnapshotDb, SpatialCategory } from "@itwin/core-backend";
import { Id64 } from "@itwin/core-bentley";
import { Code, GeometryStreamBuilder, IModel, PhysicalElementProps, RelationshipProps, SubCategoryAppearance } from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { assert, expect } from "chai";
import * as path from "path";
import { IModelExportHandler } from "../../IModelExporter";
import { IModelExporter } from "../../transformer";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { createBRepDataProps } from "../TestUtils";
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

  it("export element with brep geometry", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile("IModelExporter", "RoundtripBrep.bim");
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, { rootSubject: { name: "brep-roundtrip" } });

    const builder = new GeometryStreamBuilder();
    builder.appendBRepData(createBRepDataProps(Point3d.create(5, 10, 0), YawPitchRollAngles.createDegrees(45, 0, 0)));

    const geomPart = new GeometryPart({
      classFullName: GeometryPart.classFullName,
      model: IModel.dictionaryId,
      code: Code.createEmpty(),
      geom: builder.geometryStream,
    }, sourceDb);

    assert(geomPart.geom?.[0]?.brep?.data !== undefined);

    const geomPartId = geomPart.insert();
    assert(Id64.isValidId64(geomPartId));

    const geomPartInSource = sourceDb.elements.getElement<GeometryPart>({ id: geomPartId, wantGeometry: true, wantBRepData: true }, GeometryPart);
    assert(geomPartInSource.geom?.[1]?.brep?.data !== undefined);

    sourceDb.saveChanges();

    const flatTargetDbPath = IModelTransformerTestUtils.prepareOutputFile("IModelExporter", "RoundtripBrepTarget.bim");
    const flatTargetDb = SnapshotDb.createEmpty(flatTargetDbPath, { rootSubject: sourceDb.rootSubject });

    class TestFlatImportHandler extends IModelExportHandler {
      public override onExportElement(elem: Element): void {
        if (elem instanceof GeometryPart)
          flatTargetDb.elements.insertElement(elem.toJSON());
      }
    }

    const exporter = new IModelExporter(sourceDb);
    exporter.registerHandler(new TestFlatImportHandler());
    exporter.wantGeometry = true;
    await expect(exporter.exportAll()).to.eventually.be.fulfilled;

    const geomPartInTarget = flatTargetDb.elements.getElement<GeometryPart>({ id: geomPartId, wantGeometry: true, wantBRepData: true }, GeometryPart);
    assert(geomPartInTarget.geom?.[1]?.brep?.data !== undefined);

    sourceDb.close();
  });

  describe.only("exportRelationships", () => {
    it("should not export invalid relationships", async () => {
      const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile("IModelExporter", "InvalidRelationship.bim");
      const sourceDb = SnapshotDb.createEmpty(sourceDbPath, { rootSubject: { name: "invalid-relationships" } });

      const categoryId = SpatialCategory.insert(sourceDb, IModel.dictionaryId, "SpatialCategory", new SubCategoryAppearance());
      const sourceModelId = PhysicalModel.insert(sourceDb, IModel.rootSubjectId, `PhysicalModel`);
      const physicalObjectProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: sourceModelId,
        category: categoryId,
        code: Code.createEmpty(),
      };
      const physicalObject1 = sourceDb.elements.insertElement(physicalObjectProps);
      const physicalObject2 = sourceDb.elements.insertElement(physicalObjectProps);
      const physicalObject3 = sourceDb.elements.insertElement(physicalObjectProps);
      const physicalObject4 = sourceDb.elements.insertElement(physicalObjectProps);

      const invalidRelationshipsProps: RelationshipProps[] = [
        // target element will be deleted
        {
          classFullName: GraphicalElement3dRepresentsElement.classFullName,
          targetId: physicalObject1,
          sourceId: physicalObject2,
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
          sourceId: physicalObject3,
        },
        // only source element is invalid
        {
          classFullName: GraphicalElement3dRepresentsElement.classFullName,
          targetId: physicalObject4,
          sourceId: "",
        },
      ];

      invalidRelationshipsProps.forEach((props) => sourceDb.relationships.insertInstance(props));
      sourceDb.withSqliteStatement(`DELETE FROM bis_Element WHERE Id = ${parseInt(physicalObject1, 16)}`, (stmt) => stmt.next());
      sourceDb.saveChanges();

      const sourceRelationships = sourceDb.withStatement("SELECT ECInstanceId FROM bis.ElementRefersToElements", (stmt) => [...stmt]);
      expect(sourceRelationships.length).to.be.equal(4);

      const targetDbFile = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", "relationships-Target.bim");
      const targetDb = SnapshotDb.createEmpty(targetDbFile, { rootSubject: { name: "relationships-Target" } });

      const exporter = new IModelExporter(sourceDb);
      await expect(exporter.exportRelationships(ElementRefersToElements.classFullName)).to.eventually.be.fulfilled;

      const targetRelationships = targetDb.withStatement("SELECT ECInstanceId FROM bis.ElementRefersToElements", (stmt) => [...stmt]);
      expect(targetRelationships.length, "TargetDb should not contain any invalid relationships").to.be.equal(0);

      sourceDb.close();
    });
  });
});
