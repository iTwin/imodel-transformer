/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import {
  _nativeDb,
  BriefcaseDb,
  ChangesetECAdaptor,
  ChannelControl,
  DrawingCategory,
  GraphicalElement2d,
  HubMock,
  SqliteChangesetReader,
} from "@itwin/core-backend";
import { GuidString, Id64 } from "@itwin/core-bentley";
import {
  Code,
  ColorDef,
  GeometryStreamProps,
  IModel,
  QueryBinder,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Arc3d, IModelJson, Point3d } from "@itwin/core-geometry";
import { assert, expect } from "chai";
import path = require("path");
import { HubWrappers, IModelTestUtils, KnownTestLocations } from "../TestUtils";
import { ChangedInstanceIds, ChangesetProcessor } from "../../IModelExporter";

describe.only("BadChangeSet", () => {
  let iTwinId: GuidString;

  before(async () => {
    HubMock.startup("BadChangeSet", KnownTestLocations.outputDir);
    iTwinId = HubMock.iTwinId;
  });

  after(async () => {
    HubMock.shutdown();
  });

  it("Instance update to a different class (bug)", async () => {
    /**
     * Test scenario: Verifies changeset reader behavior when an instance ID is reused with a different class.
     *
     * Steps:
     * 1. Import schema with two classes (T1 and T2) that inherit from GraphicalElement2d.
     *    - T1 has property 'p' of type string
     *    - T2 has property 'p' of type long
     * 2. Insert an element of type T1 with id=elId and property p="wwww"
     * 3. Push changeset #1: "insert element"
     * 4. Delete the T1 element
     * 5. Manipulate the element ID sequence to force reuse of the same ID
     * 6. Insert a new element of type T2 with the same id=elId but property p=1111
     * 7. Push changeset #2: "buggy changeset"
     *
     * Verification:
     * - Changeset #2 should show an "Updated" operation (not Delete+Insert)
     * - In bis_Element table: ECClassId changes from T1 to T2
     * - In bis_GeometricElement2d table: ECClassId changes from T1 to T2
     * - Property 'p' changes from string "wwww" to integer 1111
     *
     * This tests the changeset reader's ability to handle instance class changes,
     * which can occur in edge cases where IDs are reused with different types.
     */
    const adminToken = "super manager token";
    const iModelName = "test";
    const modelId = await HubMock.createNewIModel({
      iTwinId,
      iModelName,
      description: "TestSubject",
      accessToken: adminToken,
    });
    assert.isNotEmpty(modelId);
    let b1 = await HubWrappers.downloadAndOpenBriefcase({
      iTwinId,
      iModelId: modelId,
      accessToken: adminToken,
    });
    // 1. Import schema with classes that span overflow table.
    const schema = `<?xml version="1.0" encoding="UTF-8"?>
    <ECSchema schemaName="TestDomain" alias="ts" version="01.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
        <ECSchemaReference name="BisCore" version="01.00" alias="bis"/>
        <ECEntityClass typeName="T1">
            <BaseClass>bis:GraphicalElement2d</BaseClass>
              <ECProperty propertyName="p" typeName="string"/>
        </ECEntityClass>
        <ECEntityClass typeName="T2">
            <BaseClass>bis:GraphicalElement2d</BaseClass>
              <ECProperty propertyName="p" typeName="long"/>
        </ECEntityClass>
    </ECSchema>`;

    await b1.importSchemaStrings([schema]);
    b1.channels.addAllowedChannel(ChannelControl.sharedChannelName);

    // Create drawing model and category
    await b1.locks.acquireLocks({ shared: IModel.dictionaryId });
    const codeProps = Code.createEmpty();
    codeProps.value = "DrawingModel";
    const [, drawingModelId] =
      IModelTestUtils.createAndInsertDrawingPartitionAndModel(
        b1,
        codeProps,
        true
      );
    let drawingCategoryId = DrawingCategory.queryCategoryIdByName(
      b1,
      IModel.dictionaryId,
      "MyDrawingCategory"
    );
    if (undefined === drawingCategoryId)
      drawingCategoryId = DrawingCategory.insert(
        b1,
        IModel.dictionaryId,
        "MyDrawingCategory",
        new SubCategoryAppearance({
          color: ColorDef.fromString("rgb(255,0,0)").toJSON(),
        })
      );

    const geomArray: Arc3d[] = [
      Arc3d.createXY(Point3d.create(0, 0), 5),
      Arc3d.createXY(Point3d.create(5, 5), 2),
      Arc3d.createXY(Point3d.create(-5, -5), 20),
    ];

    const geometryStream: GeometryStreamProps = [];
    for (const geom of geomArray) {
      const arcData = IModelJson.Writer.toIModelJson(geom);
      geometryStream.push(arcData);
    }

    const geomElementT1 = {
      classFullName: "TestDomain:T1",
      model: drawingModelId,
      category: drawingCategoryId,
      code: Code.createEmpty(),
      geom: geometryStream,
      p: "wwww",
    };

    const elId = b1.elements.insertElement(geomElementT1);
    assert.isTrue(Id64.isValidId64(elId), "insert worked");
    b1.saveChanges();
    await b1.pushChanges({ description: "insert element" });

    await b1.locks.acquireLocks({ shared: drawingModelId, exclusive: elId });
    await b1.locks.acquireLocks({ shared: IModel.dictionaryId });
    b1.elements.deleteElement(elId);
    b1.saveChanges();

    // Force id set to reproduce same instance with different classid
    const bid = BigInt(elId) - 1n;
    b1[_nativeDb].saveLocalValue("bis_elementidsequence", bid.toString());
    b1.saveChanges();
    const fileName = b1[_nativeDb].getFilePath();
    b1.close();

    b1 = await BriefcaseDb.open({ fileName });
    b1.channels.addAllowedChannel(ChannelControl.sharedChannelName);

    const geomElementT2 = {
      classFullName: "TestDomain:T2",
      model: drawingModelId,
      category: drawingCategoryId,
      code: Code.createEmpty(),
      geom: geometryStream,
      p: 1111,
    };

    const elId2 = b1.elements.insertElement(geomElementT2);
    expect(elId).equals(elId2);

    b1.saveChanges();
    await b1.pushChanges({ description: "buggy changeset" });

    const getChanges = async () => {
      return HubMock.downloadChangesets({
        iModelId: modelId,
        targetDir: path.join(
          KnownTestLocations.outputDir,
          modelId,
          "changesets"
        ),
      });
    };

    const changesets = await getChanges();
    expect(changesets.length).equals(2);
    expect(changesets[0].description).equals("insert element");
    expect(changesets[1].description).equals("buggy changeset");

    const getClassId = async (name: string) => {
      const r = b1.createQueryReader(
        "SELECT FORMAT('0x%x', ec_classid(?))",
        QueryBinder.from([name])
      );
      if (await r.step()) {
        return r.current[0];
      }
    };

    const t1ClassId = await getClassId("TestDomain:T1");
    const t2ClassId = await getClassId("TestDomain:T2");

    const reader = SqliteChangesetReader.openFile({
      fileName: changesets[1].pathname,
      disableSchemaCheck: true,
      db: b1,
    });
    let bisElementAsserted = false;
    let bisGeometricElement2dAsserted = false;
    while (reader.step()) {
      if (reader.tableName === "bis_Element" && reader.op === "Updated") {
        bisElementAsserted = true;
        expect(reader.getColumnNames(reader.tableName)).deep.equals([
          "Id",
          "ECClassId",
          "ModelId",
          "LastMod",
          "CodeSpecId",
          "CodeScopeId",
          "CodeValue",
          "UserLabel",
          "ParentId",
          "ParentRelECClassId",
          "FederationGuid",
          "JsonProperties",
        ]);

        const oldId = reader.getChangeValueId(0, "Old");
        const newId = reader.getChangeValueId(0, "New");
        expect(oldId).equals(elId);
        expect(newId).to.be.undefined;

        const oldClassId = reader.getChangeValueId(1, "Old");
        const newClassId = reader.getChangeValueId(1, "New");
        expect(oldClassId).equals(t1ClassId);
        expect(newClassId).equals(t2ClassId);
        expect(oldClassId).is.not.equal(newClassId);
      }
      if (
        reader.tableName === "bis_GeometricElement2d" &&
        reader.op === "Updated"
      ) {
        bisGeometricElement2dAsserted = true;
        expect(reader.getColumnNames(reader.tableName)).deep.equals([
          "ElementId",
          "ECClassId",
          "CategoryId",
          "Origin_X",
          "Origin_Y",
          "Rotation",
          "BBoxLow_X",
          "BBoxLow_Y",
          "BBoxHigh_X",
          "BBoxHigh_Y",
          "GeometryStream",
          "TypeDefinitionId",
          "TypeDefinitionRelECClassId",
          "js1",
          "js2",
        ]);

        // ECInstanceId
        const oldId = reader.getChangeValueId(0, "Old");
        const newId = reader.getChangeValueId(0, "New");
        expect(oldId).equals(elId);
        expect(newId).to.be.undefined;

        // ECClassId (changed)
        const oldClassId = reader.getChangeValueId(1, "Old");
        const newClassId = reader.getChangeValueId(1, "New");
        expect(oldClassId).equals(t1ClassId);
        expect(newClassId).equals(t2ClassId);
        expect(oldClassId).is.not.equal(newClassId);

        // Property 'p' changed type and value.
        const oldP = reader.getChangeValueText(13, "Old");
        const newP = reader.getChangeValueInteger(13, "New");
        expect(oldP).equals("wwww");
        expect(newP).equals(1111);
      }
    }

    expect(bisElementAsserted).to.be.true;
    expect(bisGeometricElement2dAsserted).to.be.true;
    reader.close();

    // ChangesetECAdaptor works incorrectly as it does not expect ECClassId to change in an update.
    // const adaptor = new ChangesetECAdaptor(
    //   SqliteChangesetReader.openFile({
    //     fileName: changesets[1].pathname,
    //     disableSchemaCheck: true,
    //     db: b1,
    //   })
    // );

    // adaptor.acceptClass(GraphicalElement2d.classFullName);
    // adaptor.acceptOp("Updated");

    // let ecChangeForElementAsserted = false;
    // let ecChangeForGeometricElement2dAsserted = false;
    // while (adaptor.step()) {
    //   if (adaptor.reader.tableName === "bis_Element") {
    //     ecChangeForElementAsserted = true;
    //     expect(adaptor.inserted?.$meta?.classFullName).equals("TestDomain:T1"); // WRONG should be TestDomain:T2
    //     expect(adaptor.deleted?.$meta?.classFullName).equals("TestDomain:T1"); // WRONG should be TestDomain:T2
    //   }
    //   if (adaptor.reader.tableName === "bis_GeometricElement2d") {
    //     ecChangeForGeometricElement2dAsserted = true;
    //     expect(adaptor.inserted?.$meta?.classFullName).equals("TestDomain:T1"); // WRONG should be TestDomain:T2
    //     expect(adaptor.deleted?.$meta?.classFullName).equals("TestDomain:T1"); // WRONG should be TestDomain:T2
    //     expect(adaptor.inserted?.p).equals("0x457"); // CORRECT p in T2 is integer
    //     expect(adaptor.deleted?.p).equals("wwww"); // CORRECT p in T1 is string
    //   }
    // }
    // expect(ecChangeForElementAsserted).to.be.true;
    // expect(ecChangeForGeometricElement2dAsserted).to.be.true;
    // adaptor.close();

    const changedInstanceIds = new ChangedInstanceIds(b1);
    const processor = new ChangesetProcessor(b1);

    await processor.processFiles(changesets, changedInstanceIds);
    // await processor.processFile(changesets[1], changedInstanceIds);
    changedInstanceIds.deletedReusedIds.forEach((reusedId) =>
      expect(reusedId.classId).to.equal(t2ClassId)
    );

    // PartialECChangeUnifier fail to combine changes correctly when ECClassId is updated.
    // const adaptor2 = new ChangesetECAdaptor(
    //   SqliteChangesetReader.openFile({ fileName: changesets[1].pathname, disableSchemaCheck: true, db: b1 })
    // );
    // const unifier = new PartialECChangeUnifier(b1);
    // adaptor2.acceptClass(GraphicalElement2d.classFullName)
    // adaptor2.acceptOp("Updated");
    // while(adaptor2.step()){
    //   unifier.appendFrom(adaptor2);
    // }

    // expect(unifier.getInstanceCount()).to.be.equals(2); // WRONG should be 1

    b1.saveChanges();
    b1.close();
  });
});
