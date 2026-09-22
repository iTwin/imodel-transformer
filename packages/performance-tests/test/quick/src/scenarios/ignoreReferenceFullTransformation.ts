/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { SnapshotDb } from "@itwin/core-backend";
import {
  ignoreReferenceNavigationPropertyCount,
  ignoreReferenceSchemaName,
  ignoreReferenceTransformFixture,
  populatedReferenceCountQuery,
} from "../fixtures/recipes/ignoreReferenceTransform.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";
import { PreparedDataset } from "../fixtures/FixtureProvider.js";
import {
  createStandaloneFullTransformation,
  standaloneStructuralIdentity,
} from "./standaloneFullTransformation.js";

async function queryCount(db: SnapshotDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(
      `Ignore-reference output count query returned no row: ${ecsql}`
    );
  return reader.current.cnt as number;
}

async function ignoreReferenceOutputIdentity(db: SnapshotDb): Promise<unknown> {
  const holders = await queryCount(
    db,
    `SELECT count(*) cnt FROM ${ignoreReferenceSchemaName}.RefHolder`
  );
  const fullyPopulatedHolders = await queryCount(
    db,
    populatedReferenceCountQuery(ignoreReferenceNavigationPropertyCount)
  );
  if (holders === 0 || fullyPopulatedHolders !== holders)
    throw new Error(
      `Ignore-reference transformation lost navigation references: holders=${holders}, fullyPopulatedHolders=${fullyPopulatedHolders}`
    );
  return {
    navigationReferences: {
      fullyPopulatedHolders,
      holders,
      referenceCount: ignoreReferenceNavigationPropertyCount,
    },
    structure: await standaloneStructuralIdentity(db),
  };
}

export function ignoreReferenceFullTransformation(
  dataset: PreparedDataset
): BenchmarkScenario {
  return createStandaloneFullTransformation(dataset, {
    description: "quick ignore-reference full transformation",
    outputIdentity: ignoreReferenceOutputIdentity,
    transformerOptions: {
      danglingReferencesBehavior: "ignore",
      loadSourceGeometry: false,
      noProvenance: true,
    },
  });
}

export const ignoreReferenceFullTransformationScenario: BenchmarkScenarioDefinition =
  {
    id: "ignore-reference-full-transformation",
    defaultFixtureId: "ignore-reference-transform",
    capabilities: {
      topology: "standalone-source-and-empty-target",
      requiredClaims: ["ignore-mode navigation-reference full transformation"],
    },
    configuration: {
      danglingReferencesBehavior: "ignore",
      loadSourceGeometry: "false",
      noProvenance: "true",
    },
    factory: ignoreReferenceFullTransformation,
  };

export const ignoreReferenceFullTransformationBenchmark = defineBenchmark({
  scenario: ignoreReferenceFullTransformationScenario,
  fixtures: [ignoreReferenceTransformFixture],
});
