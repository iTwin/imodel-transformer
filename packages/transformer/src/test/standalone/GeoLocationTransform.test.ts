/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import * as fs from "fs";
import * as path from "path";
import {
  DefinitionModel,
  GenericPhysicalType,
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
import {
  Geometry,
  Point3d,
  Sphere,
  Transform,
  YawPitchRollAngles,
} from "@itwin/core-geometry";
import { IModelTransformer3d } from "../IModelTransformerUtils";
import { get } from "http";
import { assert } from "console";

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

  function convertLatLongToEcef(
    longitude: number,
    latitude: number
  ): EcefLocation {
    const cartographic = Cartographic.fromDegrees({
      longitude,
      latitude,
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

    let x = 0,
      y = 0,
      z = 0;
    const builder = new GeometryStreamBuilder();
    builder.appendGeometry(Sphere.createCenterRadius(Point3d.createZero(), 1));
    for (let i = 0; i < numElements; i++) {
      if (i % 4 === 0 && i !== 0) {
        x = 0;
        y = 0;
        z += 5;
      } else if (i % 4 === 1) {
        x += 5;
      } else if (i % 4 === 2) {
        y += 5;
      } else if (i % 4 === 3) {
        x -= 5;
      }

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

  // TEMP: Function to get ECEF location from an existing local iModel
  // testing purposes only atm
  function getEcefFromExistingDb(filePath: string): EcefLocation {
    const imodelDb = SnapshotDb.openFile(filePath);
    const ecefLocation = imodelDb.ecefLocation;
    if (ecefLocation === undefined) {
      throw new Error("No ECEF location found in the iModel");
    }
    imodelDb.close();
    return ecefLocation;
  }

  // Calculate the transform between two ECEF locations
  // Converts relative coords from the src imodel to the new relative coords in the target imodel based on the shift between the src and target ECEF locations
  function getEcefTransform(
    srcEcefLoc: EcefLocation,
    targetEcefLoc: EcefLocation
  ): Transform {
    if (srcEcefLoc.getTransform().isAlmostEqual(targetEcefLoc.getTransform()))
      return Transform.createIdentity();

    const srcSpatialToECEF = srcEcefLoc.getTransform(); // converts relative to ECEF in relation to source
    const targetECEFToSpatial = targetEcefLoc.getTransform().inverse()!; // converts ECEF to relative in relation to target
    const ecefTransform =
      srcSpatialToECEF.multiplyTransformTransform(targetECEFToSpatial); // chain both transforms

    return ecefTransform;
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

  // Get the difference between two points
  // When compared difference between 2 ecef locations should equal the difference between the src elements placement before and after transform
  function getPositionDifference(
    point1: Point3d,
    point2: Point3d
  ): { x: number; y: number; z: number } {
    return {
      x: point1.x - point2.x,
      y: point1.y - point2.y,
      z: point1.z - point2.z,
    };
  }

  it.only("should transform placement of src elements when target has different ECEF", async function () {
    // const srcEcef =   convertLatLongToEcef(39.95512097639021, -75.16578267595735) // 1515 Arch
    // const targetEcef = convertLatLongToEcef(39.95595450339434, -75.16697176954752) // Bentley Cherry Street

    // grab ecefs from existing iModels currently using ben's linearly located discs
    const srcEcef = getEcefFromExistingDb(
      "D:/GCS-transformer-poc/lib/output/source-iModel.bim"
    );
    const targetEcef = getEcefFromExistingDb(
      "D:/GCS-transformer-poc/lib/output/target-iModel.bim"
    );

    // generate imodels with ecef locations specified above, and number of spherical elements inserted
    const sourceDb = createTestSnapshotDb(
      srcEcef,
      "Source-ECEF-Transform",
      12,
      "red"
    );
    const targetDb = createTestSnapshotDb(
      targetEcef,
      "Target-ECEF-Transform",
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
    const srcElemPositionPreTransform = srcElements[0].placement.origin;

    const ecefDiff = getPositionDifference(srcEcef.origin, targetEcef.origin);

    const ecefTransform = getEcefTransform(
      sourceDb.ecefLocation!,
      targetDb.ecefLocation!
    );

    const transfrom3d = new IModelTransformer3d(
      sourceDb,
      targetDb,
      ecefTransform
    );

    await transfrom3d.process();
    targetDb.saveChanges("clone contents from source");

    const srcElemPositionPostTransform =
      targetDb.elements.getElement<GeometricElement3d>(srcElemFedGuid!)
        .placement.origin;
    const srcElemPositionDiff = getPositionDifference(
      srcElemPositionPostTransform,
      srcElemPositionPreTransform
    );
    console.log("srcElemPositionDiff", srcElemPositionDiff);
    console.log("ecefDiff", ecefDiff);
    console.log("ecef transform origin", ecefTransform.origin);

    targetDb.close();
    sourceDb.close();
    transfrom3d.dispose();
  });
});
