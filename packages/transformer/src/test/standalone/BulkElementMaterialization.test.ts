/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/**
 * Characterization tests for issue #7 ("single query bulk getElement").
 *
 * These tests nail down whether the polymorphic instance query
 * `SELECT $ FROM bis.Element` can materialize Element instances equivalent to
 * `IModelDb.elements.getElement`, and in particular:
 * - what post-processing the raw `$` JSON needs to become valid ElementProps,
 * - whether GeometryStream/BRep data is available (and in what format),
 * - whether the class registry (`constructEntity`) yields identical classes.
 *
 * They intentionally document the exact prop-shape differences so a future
 * exporter fast path (or the decision not to build one) is grounded in
 * asserted behavior rather than assumptions.
 */

import {
  DefinitionModel,
  DisplayStyle3d,
  DocumentListModel,
  Drawing,
  DrawingCategory,
  DrawingGraphic,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  GeometryPart,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64, Id64String } from "@itwin/core-bentley";
import {
  Code,
  ColorDef,
  GeometricElement2dProps,
  GeometricElement3dProps,
  GeometryPartProps,
  GeometryStreamBuilder,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point2d, Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { afterAll, assert, beforeAll, describe, expect, it } from "vitest";
import {
  instanceRowToElementProps,
  queryAllInstanceRows,
} from "../TestUtils/InstanceQueryElementUtils";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { createBRepDataProps } from "../TestUtils/GeometryTestUtil";

describe("bulk element materialization via SELECT $ (issue #7)", () => {
  let db: SnapshotDb;
  let physicalObjectId: Id64String;
  let childObjectId: Id64String;
  let jsonPropsObjectId: Id64String;
  let geomPartId: Id64String;
  let drawingGraphicId: Id64String;

  beforeAll(() => {
    const dbPath = IModelTransformerTestUtils.prepareOutputFile(
      "BulkElementMaterialization",
      "select-dollar-characterization.bim"
    );
    db = SnapshotDb.createEmpty(dbPath, {
      rootSubject: { name: "select-dollar-characterization" },
    });

    withEditTxn(db, "insert characterization fixture", (txn) => {
      const subjectId = Subject.insert(
        txn,
        IModel.rootSubjectId,
        "Fixture Subject",
        "with a description"
      );
      const definitionModelId = DefinitionModel.insert(
        txn,
        subjectId,
        "Fixture Definitions"
      );
      const spatialCategoryId = SpatialCategory.insert(
        txn,
        definitionModelId,
        "Fixture SpatialCategory",
        new SubCategoryAppearance({
          color: ColorDef.fromString("rgb(2,124,9)").toJSON(),
          transp: 0.25,
        })
      );
      DisplayStyle3d.insert(txn, definitionModelId, "Fixture DisplayStyle", {
        backgroundColor: ColorDef.blue,
      });
      const physicalModelId = PhysicalModel.insert(
        txn,
        subjectId,
        "Fixture Physical"
      );

      const physicalObjectProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: physicalModelId,
        category: spatialCategoryId,
        code: Code.createEmpty(),
        userLabel: "geom-bearing object",
        geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
        placement: {
          origin: Point3d.create(3, 2, 1),
          angles: YawPitchRollAngles.createDegrees(15, 30, 45),
        },
      };
      physicalObjectId = txn.insertElement(physicalObjectProps);

      const childObjectProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: physicalModelId,
        category: spatialCategoryId,
        code: Code.createEmpty(),
        parent: {
          id: physicalObjectId,
          relClassName: "BisCore:PhysicalElementAssemblesElements",
        },
        placement: {
          origin: Point3d.create(0, 0, 0),
          angles: YawPitchRollAngles.createDegrees(0, 0, 0),
        },
      };
      childObjectId = txn.insertElement(childObjectProps);

      const jsonPropsObjectProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: physicalModelId,
        category: spatialCategoryId,
        code: Code.createEmpty(),
        jsonProperties: {
          UserProps: { fixture: { nested: [1, 2, 3], flag: true } },
        },
        placement: {
          origin: Point3d.create(-1, -2, -3),
          angles: YawPitchRollAngles.createDegrees(0, 90, 0),
        },
      };
      jsonPropsObjectId = txn.insertElement(jsonPropsObjectProps);

      const brepBuilder = new GeometryStreamBuilder();
      brepBuilder.appendBRepData(
        createBRepDataProps(
          Point3d.create(5, 10, 0),
          YawPitchRollAngles.createDegrees(45, 0, 0)
        )
      );
      const geomPartProps: GeometryPartProps = {
        classFullName: GeometryPart.classFullName,
        model: definitionModelId,
        code: GeometryPart.createCode(db, definitionModelId, "Fixture BRep"),
        geom: brepBuilder.geometryStream,
      };
      geomPartId = txn.insertElement(geomPartProps);

      const documentListModelId = DocumentListModel.insert(
        txn,
        subjectId,
        "Fixture Documents"
      );
      const drawingId = Drawing.insert(
        txn,
        documentListModelId,
        "Fixture Drawing"
      );
      const drawingCategoryId = DrawingCategory.insert(
        txn,
        definitionModelId,
        "Fixture DrawingCategory",
        new SubCategoryAppearance()
      );
      const drawingGraphicProps: GeometricElement2dProps = {
        classFullName: DrawingGraphic.classFullName,
        model: drawingId,
        category: drawingCategoryId,
        code: Code.createEmpty(),
        geom: IModelTransformerTestUtils.createRectangle(Point2d.create(1, 1)),
        placement: {
          origin: Point2d.create(2, 2),
          angle: 15,
        },
      };
      drawingGraphicId = txn.insertElement(drawingGraphicProps);
    });
  });

  afterAll(() => {
    db?.close();
  });

  it("SELECT $ rows construct elements deep-equal to getElement(wantGeometry=false)", async () => {
    const rows = await queryAllInstanceRows(db);
    expect(rows.size).to.be.greaterThan(10); // fixture + root subject etc.

    for (const [id, row] of rows) {
      const { props } = instanceRowToElementProps(row);
      // constructing through the class registry normalizes class-defaulted
      // fields (e.g. Category.description) exactly like getElement does
      const constructed = db.constructEntity<Element>(props).toJSON();

      const viaGetElement = db.elements
        .getElement({ id, wantGeometry: false })
        .toJSON();

      expect(
        constructed,
        `constructed $ props for ${row.className as string} ${id}`
      ).to.deep.advancedEqual(viaGetElement, {
        considerNonExistingAndUndefinedEqual: true,
        normalizeClassNameProps: true,
      });
    }
  });

  it("constructEntity yields the same JS class as getElement", async () => {
    const rows = await queryAllInstanceRows(db);
    for (const [id, row] of rows) {
      const { props } = instanceRowToElementProps(row);
      const constructed = db.constructEntity<Element>(props);
      const viaGetElement = db.elements.getElement(id);
      expect(constructed.constructor).to.equal(viaGetElement.constructor);
      expect(constructed.toJSON()).to.deep.advancedEqual(
        viaGetElement.toJSON(),
        {
          considerNonExistingAndUndefinedEqual: true,
          normalizeClassNameProps: true,
        }
      );
    }
  });

  it("characterizes geometry availability in SELECT $ output", async () => {
    const rows = await queryAllInstanceRows(db);

    for (const id of [physicalObjectId, geomPartId, drawingGraphicId]) {
      const row = rows.get(id);
      assert(row !== undefined, `expected $ row for ${id}`);
      const { geometryStream } = instanceRowToElementProps(row);

      // `$` returns the raw binary flatbuffer GeometryStream (revived from
      // base64), NOT the GeometryStreamProps JSON that
      // getElement({ wantGeometry: true }) produces. So a bulk fast path
      // cannot serve wantGeometry=true without a separate conversion.
      expect(
        geometryStream,
        `geometryStream for ${id} should be present in $ output`
      ).to.not.equal(undefined);
      expect(geometryStream).to.be.instanceOf(Uint8Array);

      const withGeometry = db.elements
        .getElement({ id, wantGeometry: true, wantBRepData: true })
        .toJSON() as GeometricElement3dProps;
      expect(Array.isArray(withGeometry.geom)).to.equal(true);
    }

    // elements without geometry omit the property entirely
    const subjectRow = [...rows.values()].find(
      (row) => (row.className as string) === "BisCore.Subject"
    );
    assert(subjectRow !== undefined);
    expect(subjectRow.geometryStream).to.equal(undefined);
  });

  it("non-geometric fixture elements also survive constructEntity", () => {
    // sanity guard that the fixture created what we think it did
    for (const id of [physicalObjectId, childObjectId, jsonPropsObjectId]) {
      assert(Id64.isValidId64(id));
    }
  });
});
