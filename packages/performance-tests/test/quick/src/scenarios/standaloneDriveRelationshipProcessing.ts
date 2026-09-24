/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn, ElementDrivesElement, SnapshotDb } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  PreparedDataset,
  requireStandaloneDataset,
} from "../fixtures/FixtureProvider.js";
import { realisticBuildingTransformLargeFixture } from "../fixtures/recipes/realisticBuildingTransformLarge.js";
import { realisticBuildingTransformFixture } from "../fixtures/recipes/realisticBuildingTransform.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";

async function classDistribution(
  db: SnapshotDb,
  className: string
): Promise<unknown[]> {
  const rows: unknown[] = [];
  const reader = db.createQueryReader(
    `SELECT ec_classname(ECClassId, 's.c') className, count(*) cnt
     FROM ${className}
     GROUP BY ECClassId
     ORDER BY className`,
    undefined,
    { usePrimaryConn: true }
  );
  while (await reader.step())
    rows.push({
      className: reader.current.className,
      count: reader.current.cnt,
    });
  return rows;
}

async function outputShapeDigest(targetDb: SnapshotDb): Promise<string> {
  const [
    aspects,
    drives,
    elements,
    geometricElementsWithGeometry,
    geometryPartsWithGeometry,
    models,
    relationships,
  ] = await Promise.all([
    classDistribution(targetDb, "bis.ElementAspect"),
    classDistribution(targetDb, "bis.ElementDrivesElement"),
    classDistribution(targetDb, "bis.Element"),
    classDistribution(
      targetDb,
      "bis.GeometricElement3d WHERE GeometryStream IS NOT NULL"
    ),
    classDistribution(
      targetDb,
      "bis.GeometryPart WHERE GeometryStream IS NOT NULL"
    ),
    classDistribution(targetDb, "bis.Model"),
    classDistribution(targetDb, "bis.ElementRefersToElements"),
  ]);
  return canonicalSha256({
    aspects,
    drives,
    elements,
    geometricElementsWithGeometry,
    geometryPartsWithGeometry,
    models,
    relationships,
  });
}

export function standaloneDriveRelationshipProcessing(
  dataset: PreparedDataset
): BenchmarkScenario {
  const { sourceDb, targetDb } = requireStandaloneDataset(dataset);
  const editTxn = new EditTxn(
    targetDb,
    "Quick standalone drive relationship processing"
  );
  editTxn.start();
  const transformer = new IModelTransformer(
    { source: sourceDb, target: editTxn },
    { loadSourceGeometry: true, noProvenance: true }
  );
  let disposed = false;
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    const errors: unknown[] = [];
    try {
      transformer.dispose();
    } catch (error) {
      errors.push(error);
    }
    try {
      if (editTxn.isActive) editTxn.end();
    } catch (error) {
      errors.push(error);
    }
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1)
      throw new AggregateError(
        errors,
        "Failed to dispose standalone drive relationship processing"
      );
  };
  return {
    abort: dispose,
    async prepare() {
      await transformer.processSchemas();
      await transformer.process();
    },
    async measure() {
      await transformer.processRelationships(
        ElementDrivesElement.classFullName
      );
    },
    async finish() {
      transformer.importer.finalize();
      editTxn.saveChanges(
        "complete quick standalone drive relationship processing"
      );
      dispose();
      return outputShapeDigest(targetDb);
    },
  };
}

export const standaloneDriveRelationshipProcessingScenario: BenchmarkScenarioDefinition =
  {
    id: "standalone-drive-relationship-processing",
    defaultFixtureId: "realistic-building-transform",
    capabilities: {
      topology: "standalone-source-and-empty-target",
      requiredClaims: ["full transformation", "drive relationship processing"],
    },
    factory: standaloneDriveRelationshipProcessing,
  };

export const standaloneDriveRelationshipProcessingBenchmark = defineBenchmark({
  scenario: standaloneDriveRelationshipProcessingScenario,
  fixtures: [
    realisticBuildingTransformFixture,
    realisticBuildingTransformLargeFixture,
  ],
});
