/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn, SnapshotDb } from "@itwin/core-backend";
import {
  IModelTransformer,
  IModelTransformOptions,
} from "@itwin/imodel-transformer";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  PreparedDataset,
  requireStandaloneDataset,
} from "../fixtures/FixtureProvider.js";
import { standaloneFullTransformFixture } from "../fixtures/recipes/standaloneFullTransform.js";
import { relationshipHeavyTransformFixture } from "../fixtures/recipes/relationshipHeavyTransform.js";
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

/**
 * Shared factory for full standalone transformations. Scenario variants (e.g. the prefetch
 * scenario) differ only in the extra transformer options they pass, so their measured pipelines
 * stay directly comparable.
 */
export function createStandaloneFullTransformation(
  dataset: PreparedDataset,
  extraTransformOptions: Partial<IModelTransformOptions> = {}
): BenchmarkScenario {
  const { sourceDb, targetDb } = requireStandaloneDataset(dataset);
  const editTxn = new EditTxn(targetDb, "Quick standalone full transformation");
  editTxn.start();
  const transformer = new IModelTransformer(
    { source: sourceDb, target: editTxn },
    {
      loadSourceGeometry: true,
      noProvenance: true,
      ...extraTransformOptions,
    }
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
        "Failed to dispose standalone full transformation"
      );
  };
  return {
    abort: dispose,
    async prepare() {
      await transformer.processSchemas();
    },
    async measure() {
      await transformer.process();
    },
    async finish() {
      editTxn.saveChanges("complete quick standalone full transformation");
      dispose();
      return outputShapeDigest(targetDb);
    },
  };
}

export function standaloneFullTransformation(
  dataset: PreparedDataset
): BenchmarkScenario {
  return createStandaloneFullTransformation(dataset);
}

export const standaloneFullTransformationScenario: BenchmarkScenarioDefinition =
  {
    id: "standalone-full-transformation",
    defaultFixtureId: "standalone-full-transform",
    capabilities: {
      topology: "standalone-source-and-empty-target",
      requiredClaims: ["full transformation"],
    },
    factory: standaloneFullTransformation,
  };

export const standaloneFullTransformationBenchmark = defineBenchmark({
  scenario: standaloneFullTransformationScenario,
  fixtures: [standaloneFullTransformFixture, relationshipHeavyTransformFixture],
});
