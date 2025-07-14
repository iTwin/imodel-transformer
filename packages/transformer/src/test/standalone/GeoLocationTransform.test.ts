/* eslint-disable no-console */
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
  AdditionalTransform,
  Cartographic,
  Code,
  ColorDef,
  EcefLocation,
  GeographicCRS,
  GeometryStreamBuilder,
  HorizontalCRS,
  PhysicalElementProps,
  VerticalCRS,
} from "@itwin/core-common";
import { Point3d, Sphere, YawPitchRollAngles } from "@itwin/core-geometry";
import { assert } from "console";
import {
  IModelTransformer,
  IModelTransformOptions,
} from "../../IModelTransformer";
import { expect } from "chai";

interface GeolocationData {
  ecefLocation: EcefLocation | undefined;
  geographicCRS: GeographicCRS | undefined;
}

// Create a test iModel with a specified ECEF location and number of spherical elements
// Elements are of radius 1, placed in 2 by 2 by x grids 5 meters apart, and first eleement is inserted at origin
// which is the specified ECEF location
function createTestSnapshotDb(
  geolocData: GeolocationData,
  dbName: string,
  numElements: number = 1,
  color: string = "red"
): SnapshotDb {
  const dbFileName = initOutputFile(`${dbName}.bim`);
  const imodelDb = SnapshotDb.createEmpty(dbFileName, {
    rootSubject: { name: dbName },
    ecefLocation: geolocData.ecefLocation,
    geographicCoordinateSystem: geolocData.geographicCRS,
  });
  // bug in SnapshotEb.createEmpty does not properly set ecefLocation or geographicCoordinateSystem, this was corrected in itwinjs-core 5.1, and will be fixed when transformer repo is updated
  if (geolocData.ecefLocation !== undefined)
    imodelDb.setEcefLocation(geolocData.ecefLocation);
  if (geolocData.geographicCRS !== undefined)
    imodelDb.setGeographicCoordinateSystem(geolocData.geographicCRS);

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

describe("Linear Geolocation Transformations", () => {
  it("should transform placement of src elements using core transfromer", async function () {
    const srcEcef = convertLatLongToEcef(
      39.952959446468206,
      -75.16349515933572
    ); // City Hall
    const targetEcef = convertLatLongToEcef(
      39.95595450339434,
      -75.16697176954752
    ); // Bentley Cherry Street

    const srcGeolocData: GeolocationData = {
      ecefLocation: srcEcef,
      geographicCRS: undefined,
    };
    const targetGeolocData: GeolocationData = {
      ecefLocation: targetEcef,
      geographicCRS: undefined,
    };

    // generate imodels with ecef locations specified above, and number of spherical elements inserted
    const sourceDb = createTestSnapshotDb(
      srcGeolocData,
      "Source-ECEF-core-Transform",
      12,
      "red"
    );
    const targetDb = createTestSnapshotDb(
      targetGeolocData,
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

describe("Non Linear Geolocation Transformations", () => {
  it.only("should transform placement of src elements when target and source have matching GCS but different addtionalTransforms", async function () {
    const horizontalCRS = new HorizontalCRS({
      id: "10TM115-27",
      description: "",
      source: "Mentor Software Client",
      deprecated: false,
      datumId: "NAD27",
      unit: "Meter",
      projection: {
        method: "TransverseMercator",
        centralMeridian: -115,
        latitudeOfOrigin: 0,
        scaleFactor: 0.9992,
        falseEasting: 0.0,
        falseNorthing: 0.0,
      },
      extent: {
        southWest: { latitude: 48, longitude: -120.5 },
        northEast: { latitude: 84, longitude: -109.5 },
      },
    });
    const verticalCRS = new VerticalCRS({
      id: "GEOID",
    });

    const srcAdditionalTransform = new AdditionalTransform({
      helmert2DWithZOffset: {
        translationX: 100.0,
        translationY: 100.0,
        translationZ: 0.0,
        rotDeg: 45,
        scale: 1,
      },
    });

    const targetAddtionalTransform = new AdditionalTransform({
      helmert2DWithZOffset: {
        translationX: 10.0,
        translationY: 10.0,
        translationZ: 0.0,
        rotDeg: 90,
        scale: 1,
      },
    });

    const srcGCS = new GeographicCRS({
      horizontalCRS,
      verticalCRS,
      additionalTransform: srcAdditionalTransform,
    });

    const targetGCS = new GeographicCRS({
      horizontalCRS,
      verticalCRS,
      additionalTransform: targetAddtionalTransform,
    });

    const sourceDb = createTestSnapshotDb(
      { ecefLocation: undefined, geographicCRS: srcGCS },
      "Source-non-linear-core-Transform",
      1,
      "red"
    );

    const targetDb = createTestSnapshotDb(
      { ecefLocation: undefined, geographicCRS: targetGCS },
      "Target-non-linear-core-Transform",
      1,
      "blue"
    );

    assert(
      sourceDb.geographicCoordinateSystem !== undefined,
      "Source iModel should have a geographic coordinate system"
    );
    assert(
      targetDb.geographicCoordinateSystem !== undefined,
      "Target iModel should have a geographic coordinate system"
    );

    const transformerOptions: IModelTransformOptions = {
      alignAdditionalTransforms: true,
    };

    const transform = new IModelTransformer(
      sourceDb,
      targetDb,
      transformerOptions
    );

    const srcHelmert = transform.convertHelmertToTransform(
      targetAddtionalTransform.helmert2DWithZOffset
    );
    const targetHelmert = transform.convertHelmertToTransform(
      srcAdditionalTransform.helmert2DWithZOffset
    );

    // goal is to have element from src and target be in the same position after additionalTransform is applied.
    // both points start at 0,0,0. Only changing placement of source element
    // apply target helmert transform to source. This way if source additional transform was the identity the elements will be at the same position as at render time
    // apply inv of source helmert to source element, so when additional transform is applied, the element will be at the same position as target element at render time
    // const srcSpatialTransform = srcHelmert.inverse()!.multiplyTransformTransform(targetHelmert);
    const srcSpatialTransform = targetHelmert
      .inverse()!
      .multiplyTransformTransform(srcHelmert);

    const srcElems = await getGeometric3dElements(sourceDb);
    const srcElem = srcElems[0];
    console.log("Source element placement:", srcElem.placement.origin);

    const targetElems = await getGeometric3dElements(targetDb);
    const targetElem = targetElems[0];
    console.log("Target element placement:", targetElem.placement.origin);

    srcElem.placement.multiplyTransform(srcSpatialTransform);
    srcElem.update();
    sourceDb.saveChanges("update placement of source element");
    console.log("Source element placement:", srcElem.placement.origin);

    await transform.process();
    targetDb.saveChanges("clone contents from source");

    const srcElemPostTransform =
      targetDb.elements.getElement<GeometricElement3d>(srcElem.federationGuid!);

    console.log(
      "Src element post transform placement:",
      srcElemPostTransform.placement.origin
    );

    expect(
      srcElemPostTransform.placement.origin.isAlmostEqual(
        targetElem.placement.origin
      ),
      "Source element placement should match target element placement"
    ).to.be.true;

    targetDb.close();
    sourceDb.close();
    transform.dispose();
  });
});
