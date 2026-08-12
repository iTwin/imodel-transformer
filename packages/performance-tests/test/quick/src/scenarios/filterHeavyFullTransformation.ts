/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn, SnapshotDb } from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  PreparedDataset,
  requireStandaloneDataset,
} from "../fixtures/FixtureProvider.js";
import {
  filterHeavyExcludedCategoryName,
  filterHeavyExcludedClassFullName,
  filterHeavyFullTransformFixture,
} from "../fixtures/recipes/filterHeavyFullTransform.js";
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

async function structuralIdentity(db: SnapshotDb): Promise<unknown> {
  const [aspects, elements, models, relationships] = await Promise.all([
    classDistribution(db, "bis.ElementAspect"),
    classDistribution(db, "bis.Element"),
    classDistribution(db, "bis.Model"),
    classDistribution(db, "bis.ElementRefersToElements"),
  ]);
  return { aspects, elements, models, relationships };
}

async function outputShapeDigest(targetDb: SnapshotDb): Promise<string> {
  return canonicalSha256(await structuralIdentity(targetDb));
}

export function filterHeavyFullTransformation(
  dataset: PreparedDataset
): BenchmarkScenario {
  const { sourceDb, targetDb } = requireStandaloneDataset(dataset);
  const editTxn = new EditTxn(
    targetDb,
    "Quick filter-heavy full transformation"
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
        "Failed to dispose filter-heavy full transformation"
      );
  };
  return {
    abort: dispose,
    async prepare() {
      const reader = sourceDb.createQueryReader(
        `SELECT ECInstanceId id FROM bis.SpatialCategory
         WHERE CodeValue = '${filterHeavyExcludedCategoryName}'`,
        undefined,
        { usePrimaryConn: true }
      );
      if (!(await reader.step()))
        throw new Error(
          `Filter-heavy scenario could not find category "${filterHeavyExcludedCategoryName}" in the source`
        );
      transformer.exporter.excludeElementsInCategory(
        reader.current.id as Id64String
      );
      transformer.exporter.excludeElementClass(
        filterHeavyExcludedClassFullName
      );
      await transformer.processSchemas();
    },
    async measure() {
      await transformer.process();
    },
    async finish() {
      editTxn.saveChanges("complete quick filter-heavy full transformation");
      dispose();
      return outputShapeDigest(targetDb);
    },
  };
}

export const filterHeavyFullTransformationScenario: BenchmarkScenarioDefinition =
  {
    id: "filter-heavy-full-transformation",
    defaultFixtureId: "filter-heavy-full-transform",
    capabilities: {
      topology: "standalone-source-and-empty-target",
      requiredClaims: ["filter-heavy transformation"],
    },
    factory: filterHeavyFullTransformation,
  };

export const filterHeavyFullTransformationBenchmark = defineBenchmark({
  scenario: filterHeavyFullTransformationScenario,
  fixtures: [filterHeavyFullTransformFixture],
});
