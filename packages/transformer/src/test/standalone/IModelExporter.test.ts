/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { Element, ElementRefersToElements, GeometryPart, GraphicalElement3dRepresentsElement, IModelJsFs, SnapshotDb } from "@itwin/core-backend";
import { Id64 } from "@itwin/core-bentley";
import { Code, GeometryStreamBuilder, IModel, RelationshipProps } from "@itwin/core-common";
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

  describe("exportRelationships", () => {
    it("should not export relationships that do not have source or target elements", async () => {
      const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile("IModelExporter", "InvalidRelationship.bim");
      const sourceDb = SnapshotDb.createEmpty(sourceDbPath, { rootSubject: { name: "invalid-relationships" } });

      const relationshipProps: RelationshipProps = {
        classFullName: GraphicalElement3dRepresentsElement.classFullName,
        targetId: "",
        sourceId: "",
      };

      sourceDb.relationships.insertInstance(relationshipProps);
      sourceDb.saveChanges();

      const sourceRelationships = sourceDb.withStatement("SELECT ECInstanceId FROM bis.ElementRefersToElements", (stmt) => [...stmt]);
      assert(sourceRelationships.length === 1);

      const targetDbFile = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", "relationships-Target.bim");
      const targetDb = SnapshotDb.createEmpty(targetDbFile, { rootSubject: { name: "relationships-Target" } });

      const exporter = new IModelExporter(sourceDb);
      await expect(exporter.exportRelationships(ElementRefersToElements.classFullName)).to.eventually.be.fulfilled;

      const targetRelationships = targetDb.withStatement("SELECT ECInstanceId FROM bis.ElementRefersToElements", (stmt) => [...stmt]);
      assert(targetRelationships.length === 0, "TargetDb should not contain any invalid relationships");

      sourceDb.close();
    });
  });
});
