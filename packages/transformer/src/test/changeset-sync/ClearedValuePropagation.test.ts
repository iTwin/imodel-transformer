/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterEach, beforeEach, expect } from "vitest";
import {
  BriefcaseDb,
  IModelDb,
  PhysicalElementAssemblesElements,
  PhysicalElementIsOfType,
  PhysicalModel,
  PhysicalObject,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import {
  DbResult,
  Guid,
  GuidString,
  Id64,
  Id64String,
} from "@itwin/core-bentley";
import {
  Code,
  GeometricElement3dProps,
  GeometryStreamBuilder,
  IModel,
  PhysicalElementProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import {
  LineString3d,
  Loop,
  Point3d,
  YawPitchRollAngles,
} from "@itwin/core-geometry";
import {
  IModelTransformer,
  IModelTransformOptions,
} from "../../IModelTransformer.js";
import { createStartedEditTxn } from "../IModelTransformerUtils.js";
import { useTransformerHub } from "../TestUtils/TransformerHubFixture.js";

/** The stored row. Element props cannot tell a NULL origin from a real origin of zero. */
interface StoredGeometry {
  stream: string | null;
  originX: number | null;
  yaw: number | null;
  bboxLowX: number | null;
}

const clearedRow: StoredGeometry = {
  stream: null,
  originX: null,
  yaw: null,
  bboxLowX: null,
};

function readStoredGeometry(
  db: IModelDb,
  elementId: Id64String
): StoredGeometry {
  return db.withPreparedSqliteStatement(
    "SELECT GeometryStream, Origin_X, Yaw, BBoxLow_X FROM bis_GeometricElement3d WHERE ElementId=?",
    (stmt) => {
      stmt.bindId(1, elementId);
      expect(stmt.step()).to.equal(DbResult.BE_SQLITE_ROW);
      const double = (column: number) =>
        stmt.getValue(column).isNull ? null : stmt.getValue(column).getDouble();
      return {
        stream: stmt.getValue(0).isNull
          ? null
          : Buffer.from(stmt.getValueBlob(0)).toString("hex"),
        originX: double(1),
        yaw: double(2),
        bboxLowX: double(3),
      };
    }
  );
}

/** Clears an element's geometry the only way the element API can: the documented empty builder. */
function clearWithEmptyBuilder(db: BriefcaseDb, elementId: Id64String) {
  withEditTxn(db, "clear with empty builder", (txn) => {
    const props =
      db.elements.getElementProps<GeometricElement3dProps>(elementId);
    txn.updateElement<GeometricElement3dProps>({
      ...props,
      elementGeometryBuilderParams: { entryArray: [] },
    });
  });
}

/** Reaches the same row without the element API, as replaying someone else's changeset can. */
function clearWithRawSql(db: BriefcaseDb, elementId: Id64String) {
  withEditTxn(db, "clear with raw sql", () => {
    db.withSqliteStatement(
      `UPDATE bis_GeometricElement3d SET GeometryStream=NULL,
         Origin_X=NULL, Origin_Y=NULL, Origin_Z=NULL, Yaw=NULL, Pitch=NULL, Roll=NULL,
         BBoxLow_X=NULL, BBoxLow_Y=NULL, BBoxLow_Z=NULL,
         BBoxHigh_X=NULL, BBoxHigh_Y=NULL, BBoxHigh_Z=NULL
       WHERE ElementId=?`,
      (stmt) => {
        stmt.bindId(1, elementId);
        expect(stmt.step()).to.equal(DbResult.BE_SQLITE_DONE);
      }
    );
  });
}

const testSchema = `<?xml version="1.0" encoding="UTF-8"?>
  <ECSchema schemaName="TestClear" alias="tclr" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
    <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
    <ECEntityClass typeName="Widget">
      <BaseClass>bis:PhysicalElement</BaseClass>
      <ECProperty propertyName="Note" typeName="string"/>
      <ECProperty propertyName="Size" typeName="double"/>
      <ECArrayProperty propertyName="Tags" typeName="string" minOccurs="0" maxOccurs="unbounded"/>
    </ECEntityClass>
    <ECEntityClass typeName="WidgetType">
      <BaseClass>bis:PhysicalType</BaseClass>
    </ECEntityClass>
  </ECSchema>`;

interface WidgetProps extends PhysicalElementProps {
  note?: string;
  size?: number;
  tags?: string[];
}

/** iTwin/imodel-transformer#487: a value cleared in the source must be cleared in an existing target element. */
describe("Cleared value propagation", () => {
  const hub = useTransformerHub("ClearedValuePropagation");
  const { prepareBriefcase, closeAndDeleteBriefcase } = hub;
  let sourceDb: BriefcaseDb;
  let targetDb: BriefcaseDb;
  let modelId: Id64String;
  let categoryId: Id64String;
  let parentId: Id64String;
  let parentGuid: GuidString;
  let controlGuid: GuidString;

  function loopGeometry() {
    const builder = new GeometryStreamBuilder();
    builder.appendGeometry(
      Loop.create(
        LineString3d.create([
          Point3d.create(0, 0, 0),
          Point3d.create(3, 0, 0),
          Point3d.create(3, 3, 0),
          Point3d.create(0, 3, 0),
          Point3d.create(0, 0, 0),
        ])
      )
    );
    return builder.geometryStream;
  }

  function insertObject(
    db: BriefcaseDb,
    extra: Partial<WidgetProps> = {},
    withGeometry = true
  ) {
    const federationGuid = Guid.createValue();
    const id = withEditTxn(db, "insert object", (txn) =>
      txn.insertElement({
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: Code.createEmpty(),
        federationGuid,
        placement: {
          origin: Point3d.create(10, 0, 0),
          angles: YawPitchRollAngles.createDegrees(15, 0, 0),
        },
        geom: withGeometry ? loopGeometry() : undefined,
        ...extra,
      })
    );
    return { id, federationGuid };
  }

  async function pushSource(description: string) {
    await sourceDb.pushChanges({ description, retainLocks: true });
  }

  async function runPass(
    options: IModelTransformOptions = {},
    { withSchemas = false } = {}
  ): Promise<void> {
    const editTxn = createStartedEditTxn(targetDb);
    const transformer = new IModelTransformer(
      { source: sourceDb, target: editTxn },
      options
    );
    let succeeded = false;
    try {
      if (withSchemas) await transformer.processSchemas();
      await transformer.process();
      succeeded = true;
    } finally {
      transformer.dispose();
      editTxn.end(succeeded ? "save" : "abandon");
    }
    await targetDb.pushChanges({
      accessToken: hub.accessToken,
      description: "transform",
      retainLocks: true,
    });
  }

  const incremental: IModelTransformOptions = { argsForProcessChanges: {} };

  function targetIdOf(federationGuid: GuidString): Id64String {
    const id = targetDb.elements.getIdFromFederationGuid(federationGuid);
    expect(id, `no target element for ${federationGuid}`).to.not.be.undefined;
    return id!;
  }

  beforeEach(async () => {
    sourceDb = await prepareBriefcase("ClearedValueSource");
    targetDb = await prepareBriefcase("ClearedValueTarget");

    await sourceDb.importSchemaStrings([testSchema]);
    await pushSource("import schema");
    withEditTxn(sourceDb, "create model and category", (txn) => {
      modelId = PhysicalModel.insert(txn, IModel.rootSubjectId, "Physical");
      categoryId = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "Category",
        new SubCategoryAppearance()
      );
    });
    const parent = insertObject(sourceDb);
    parentId = parent.id;
    parentGuid = parent.federationGuid;
    const control = insertObject(sourceDb);
    controlGuid = control.federationGuid;
    await pushSource("fixture");
    await runPass({}, { withSchemas: true });
  });

  afterEach(async () => {
    await closeAndDeleteBriefcase(sourceDb);
    await closeAndDeleteBriefcase(targetDb);
  });

  describe("geometry", () => {
    it.each([
      ["an empty builder", clearWithEmptyBuilder],
      ["raw SQL", clearWithRawSql],
    ])(
      "clears a target's geometry after the source clears it with %s",
      async (_name, clear) => {
        const targetParentId = targetIdOf(parentGuid);
        const targetControlId = targetIdOf(controlGuid);
        expect(readStoredGeometry(targetDb, targetParentId).stream).to.not.be
          .null;
        const controlBefore = readStoredGeometry(targetDb, targetControlId);

        // The source moves the parent's geometry onto a new child.
        clear(sourceDb, parentId);
        const child = insertObject(sourceDb, {
          parent: new PhysicalElementAssemblesElements(parentId),
        });
        await pushSource("move geometry to child");
        // Native clones read a cached element, and raw SQL does not invalidate that cache. A fresh
        // connection reads the row, as a process that pulled the change would.
        const sourcePath = sourceDb.pathName;
        sourceDb.close();
        sourceDb = await BriefcaseDb.open({ fileName: sourcePath });
        expect(readStoredGeometry(sourceDb, parentId)).to.deep.equal(
          clearedRow
        );

        await runPass(incremental);
        expect(readStoredGeometry(targetDb, targetParentId)).to.deep.equal(
          clearedRow
        );
        expect(
          readStoredGeometry(targetDb, targetIdOf(child.federationGuid)).stream
        ).to.not.be.null;
        expect(readStoredGeometry(targetDb, targetControlId)).to.deep.equal(
          controlBefore
        );

        // A later full pass keeps the target cleared.
        await runPass();
        expect(readStoredGeometry(targetDb, targetParentId)).to.deep.equal(
          clearedRow
        );
      }
    );

    it.each([
      ["an incremental pass", incremental],
      ["a full pass", {}],
    ])(
      "clears a target's geometry in %s when cloning with JSON geometry",
      async (_name, options) => {
        const targetParentId = targetIdOf(parentGuid);
        expect(readStoredGeometry(targetDb, targetParentId).stream).to.not.be
          .null;

        clearWithEmptyBuilder(sourceDb, parentId);
        await pushSource("clear");
        await runPass({ ...options, cloneUsingBinaryGeometry: false });

        expect(readStoredGeometry(targetDb, targetParentId)).to.deep.equal(
          clearedRow
        );
      }
    );

    it("inserts an element whose geometry was cleared before the first pass", async () => {
      const cleared = insertObject(sourceDb);
      clearWithEmptyBuilder(sourceDb, cleared.id);
      await pushSource("clear before the first pass");
      await runPass(incremental);
      expect(
        readStoredGeometry(targetDb, targetIdOf(cleared.federationGuid))
      ).to.deep.equal(clearedRow);
    });

    it("keeps the placement of a source element that has a placement and no geometry", async () => {
      const placedOnly = insertObject(sourceDb, {}, false);
      await pushSource("placed element without geometry");
      const sourceRow = readStoredGeometry(sourceDb, placedOnly.id);
      expect(sourceRow.stream).to.be.null;
      expect(sourceRow.originX).to.equal(10);

      await runPass(incremental);
      const targetId = targetIdOf(placedOnly.federationGuid);
      expect(readStoredGeometry(targetDb, targetId).originX).to.equal(10);

      withEditTxn(sourceDb, "touch", (txn) =>
        txn.updateElement({
          ...sourceDb.elements.getElementProps(placedOnly.id),
          userLabel: "touched",
        })
      );
      await pushSource("touch");
      await runPass(incremental);
      const targetRow = readStoredGeometry(targetDb, targetId);
      expect(targetRow.stream).to.be.null;
      expect(targetRow.originX).to.equal(10);
      expect(targetRow.yaw).to.be.closeTo(15, 1e-9);
    });
  });

  describe("other properties", () => {
    let widget: { id: Id64String; federationGuid: GuidString };

    beforeEach(async () => {
      const typeId = withEditTxn(sourceDb, "insert widget type", (txn) =>
        txn.insertElement({
          classFullName: "TestClear:WidgetType",
          model: IModel.dictionaryId,
          code: Code.createEmpty(),
        })
      );
      widget = insertObject(sourceDb, {
        classFullName: "TestClear:Widget",
        userLabel: "widget",
        parent: new PhysicalElementAssemblesElements(parentId),
        typeDefinition: new PhysicalElementIsOfType(typeId),
        jsonProperties: { testClear: { kept: false } },
        note: "note",
        size: 2.5,
        tags: ["a", "b"],
      });
      await pushSource("insert widget");
      await runPass(incremental);
    });

    function readWidget(db: IModelDb, id: Id64String) {
      return db.elements.getElementProps<WidgetProps>(id);
    }

    it.each([
      ["an incremental pass", incremental],
      ["a full pass", {}],
    ])("clears them in %s", async (_name, options) => {
      const targetWidgetId = targetIdOf(widget.federationGuid);
      const before = readWidget(targetDb, targetWidgetId);
      expect(before.userLabel).to.equal("widget");
      expect(before.parent?.id).to.equal(targetIdOf(parentGuid));
      expect(Id64.isValidId64(before.typeDefinition?.id ?? "")).to.be.true;
      expect(before.jsonProperties).to.deep.equal({
        testClear: { kept: false },
      });
      expect(before.note).to.equal("note");
      expect(before.size).to.equal(2.5);
      expect(before.tags).to.deep.equal(["a", "b"]);

      withEditTxn(sourceDb, "clear widget values", (txn) =>
        txn.updateElement<WidgetProps>({
          ...readWidget(sourceDb, widget.id),
          userLabel: undefined,
          parent: undefined,
          typeDefinition: undefined,
          jsonProperties: {},
          note: undefined,
          size: undefined,
          tags: undefined,
        })
      );
      await pushSource("clear widget values");
      const sourceAfter = readWidget(sourceDb, widget.id);
      expect(sourceAfter.userLabel).to.be.undefined;
      expect(sourceAfter.parent).to.be.undefined;
      expect(sourceAfter.typeDefinition).to.be.undefined;
      expect(sourceAfter.jsonProperties ?? {}).to.deep.equal({});
      expect(sourceAfter.note).to.be.undefined;
      expect(sourceAfter.size).to.be.undefined;
      expect(sourceAfter.tags).to.be.undefined;

      await runPass(options);

      const after = readWidget(targetDb, targetWidgetId);
      expect(after.userLabel).to.be.undefined;
      expect(after.parent).to.be.undefined;
      expect(after.typeDefinition).to.be.undefined;
      expect(after.jsonProperties ?? {}).to.deep.equal({});
      expect(after.note).to.be.undefined;
      expect(after.size).to.be.undefined;
      expect(after.tags).to.be.undefined;
      expect(after.federationGuid).to.equal(widget.federationGuid);
      expect(readStoredGeometry(targetDb, targetWidgetId).stream).to.not.be
        .null;
    });
  });
});
