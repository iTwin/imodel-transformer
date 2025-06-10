/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import * as path from "path";
import {
  DefinitionModel,
  GeometricElement3d,
  IModelDb,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  Subject,
} from "@itwin/core-backend";
import {
  Cartographic,
  Code,
  ColorDef,
  EcefLocation,
  GeometryStreamBuilder,
  PhysicalElementProps,
} from "@itwin/core-common";
import { Point3d, Sphere, YawPitchRollAngles } from "@itwin/core-geometry";
import { assert } from "console";
import {
  IModelTransformer,
  IModelTransformOptions,
} from "../../IModelTransformer";
import { expect } from "chai";

describe("Linear Geolocation Transformations", () => {
  function initOutputFile(fileBaseName: string) {
    const outputDirName = path.join(__dirname, "output");
    if (!IModelJsFs.existsSync(outputDirName)) {
      IModelJsFs.mkdirSync(outputDirName);
    }
    const outputFileName = path.join(outputDirName, fileBaseName);
    if (IModelJsFs.existsSync(outputFileName)) {
      IModelJsFs.removeSync(outputFileName);
    }
    return outputFileName;
  }

  function convertLatLongToEcef(lat: number, long: number): EcefLocation {
    const cartographic = Cartographic.fromDegrees({
      longitude: long,
      latitude: lat,
      height: 0,
    });
    const ecef = EcefLocation.createFromCartographicOrigin(cartographic);

    return ecef;
  }

  // Create a test iModel with a specified ECEF location and number of spherical elements
  // Elements are of radius 1, placed in 2 by 2 by x grids 5 meters apart, and first eleement is inserted at origin
  // which is the specified ECEF location
  function createTestSnapshotDb(
    ecef: EcefLocation,
    dbName: string,
    numElements: number = 1,
    color: string = "red"
  ): SnapshotDb {
    const dbFileName = initOutputFile(`${dbName}.bim`);
    const imodelDb = SnapshotDb.createEmpty(dbFileName, {
      rootSubject: { name: dbName },
      ecefLocation: ecef,
    });
    // bug in SnapshotEb.createEmpty does not properly set ecefLocation, this was corrected in itwinjs-core 5.1, and will be fixed when transformer repo is updated
    imodelDb.setEcefLocation(ecef);

    const subjectId = Subject.insert(
      imodelDb,
      IModelDb.rootSubjectId,
      "Test Subject"
    );
    const defintionModelId = DefinitionModel.insert(
      imodelDb,
      subjectId,
      "DefinitionModel"
    );

    const categoryId = SpatialCategory.insert(
      imodelDb,
      defintionModelId,
      `${color} Category`,
      { color: ColorDef.fromString(color).toJSON() }
    );

    const modelId = PhysicalModel.insert(imodelDb, subjectId, "Test Model");

    const builder = new GeometryStreamBuilder();
    builder.appendGeometry(Sphere.createCenterRadius(Point3d.createZero(), 1));
    for (let i = 0; i < numElements; i++) {
      // Arrange elements in a 2x2 grid, incrementing z every 4 elements
      const x = (i % 2) * 5;
      const y = (Math.floor(i / 2) % 2) * 5;
      const z = Math.floor(i / 4) * 5;

      const elementProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: Code.createEmpty(),
        geom: builder.geometryStream,
        placement: {
          origin: Point3d.create(x, y, z),
          angles: YawPitchRollAngles.createDegrees(0, 0, 0),
        },
      };
      imodelDb.elements.insertElement(elementProps);
    }
    imodelDb.saveChanges("Created test elements");

    return imodelDb;
  }

  // Get all GeometricElement3d elements from the iModel
  // Used to find and compare placement of elements before and after transform
  async function getGeometric3dElements(
    iModelDb: IModelDb
  ): Promise<GeometricElement3d[]> {
    const elements: GeometricElement3d[] = [];
    const query = "SELECT ECInstanceId FROM bis.GeometricElement3d";
    for await (const row of iModelDb.createQueryReader(query)) {
      const element = iModelDb.elements.getElement<GeometricElement3d>(row.id);
      elements.push(element);
    }
    return elements;
  }

  it("should transform placement of src elements using core transfromer", async function () {
    const srcEcef = convertLatLongToEcef(
      39.952959446468206,
      -75.16349515933572
    ); // City Hall
    const targetEcef = convertLatLongToEcef(
      39.95595450339434,
      -75.16697176954752
    ); // Bentley Cherry Street

    // generate imodels with ecef locations specified above, and number of spherical elements inserted
    const sourceDb = createTestSnapshotDb(
      srcEcef,
      "Source-ECEF-core-Transform",
      12,
      "red"
    );
    const targetDb = createTestSnapshotDb(
      targetEcef,
      "Target-ECEF-core-Transform",
      12,
      "blue"
    );
    assert(sourceDb.geographicCoordinateSystem === undefined);
    assert(targetDb.geographicCoordinateSystem === undefined);
    assert(sourceDb.ecefLocation !== undefined);
    assert(targetDb.ecefLocation !== undefined);

    // get Fed Guid of one geomentric element in srcDb so we can compare the transfromed element in targetDb
    const srcElements = await getGeometric3dElements(sourceDb);
    const srcElemFedGuid = srcElements[0].federationGuid;

    const transformerOptions: IModelTransformOptions = {
      alignECEFLocations: true,
    };
    const transfrom = new IModelTransformer(
      sourceDb,
      targetDb,
      transformerOptions
    );

    await transfrom.process();
    targetDb.saveChanges("clone contents from source");

    const srcElemPositionPostTransform =
      targetDb.elements.getElement<GeometricElement3d>(
        srcElemFedGuid!
      ).placement;
    srcElemPositionPostTransform.multiplyTransform(targetEcef.getTransform());
    // assert that the element at the origin of sourceDb still has the same ecef location when transformed to targetDb
    expect(
      srcEcef.origin.isAlmostEqual(srcElemPositionPostTransform.origin),
      "Source element position's ecef location does not match target element position's ecef location after transform"
    ).to.be.true;

    targetDb.close();
    sourceDb.close();
    transfrom.dispose();
  });
});
