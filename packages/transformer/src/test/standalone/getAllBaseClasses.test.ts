/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { IModelHost, StandaloneDb } from "@itwin/core-backend";
import * as fs from "fs";
import * as path from "path";
import { IModelTransformer } from "../../IModelTransformer";
import { KnownTestLocations } from "../TestUtils";

describe("getAllBaseClasses", () => {
  it.only("fail during transform", async () => {
    await IModelHost.startup();

    const sourceDbPath = path.join(
      KnownTestLocations.outputDir,
      "SourceDb.bim"
    );
    const targetDbPath = path.join(
      KnownTestLocations.outputDir,
      "TargetDb.bim"
    );

    // Delete existing files if they exist
    if (fs.existsSync(sourceDbPath)) {
      fs.unlinkSync(sourceDbPath);
    }
    if (fs.existsSync(targetDbPath)) {
      fs.unlinkSync(targetDbPath);
    }

    // Create source db with cross-schema inheritance
    const sourceDb = StandaloneDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "Source" },
      allowEdit: JSON.stringify({ txns: true }),
    });

    // Import schemas where BuildingSpatial.Zone extends SpatialComposition.Zone
    // You need to install these
    const aecUnitsPath = require.resolve(
      "@bentley/aec-units-schema/AecUnits.ecschema.xml"
    );
    const spatialCompositionPath = require.resolve(
      "@bentley/spatialcomposition-schema/SpatialComposition.ecschema.xml"
    );
    const buildingSpatialPath = require.resolve(
      "@bentley/building-spatial-schema/BuildingSpatial.ecschema.xml"
    );

    await sourceDb.importSchemas([
      aecUnitsPath,
      spatialCompositionPath,
      buildingSpatialPath,
    ]);
    sourceDb.saveChanges();

    // Create empty target db
    const targetDb = StandaloneDb.createEmpty(targetDbPath, {
      rootSubject: { name: "Target" },
      allowEdit: JSON.stringify({ txns: true }),
    });

    // Use IModelTransformer
    const transformer = new IModelTransformer(sourceDb, targetDb);
    await transformer.processSchemas();

    // This fails with: "An unknown root class 'SpatialComposition.Zone' was encountered..."
    await transformer.process();

    transformer.dispose();
    sourceDb.close();
    targetDb.close();
    await IModelHost.shutdown();
  });
});
