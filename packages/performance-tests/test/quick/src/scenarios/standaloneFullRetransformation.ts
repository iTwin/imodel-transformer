/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  EditTxn,
  ElementRefersToElements,
  IModelDb,
} from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import {
  PreparedDataset,
  requirePopulatedStandaloneDataset,
} from "../fixtures/FixtureProvider.js";
import { relationshipHeavyRetransformFixture } from "../fixtures/recipes/relationshipHeavyTransform.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";
import { outputShapeDigest } from "./outputShape.js";

const outputClassQueries = {
  aspects: "bis.ElementAspect",
  elements: "bis.Element",
  models: "bis.Model",
  relationships: "bis.ElementRefersToElements",
} as const;

async function countRelationships(db: IModelDb): Promise<number> {
  const reader = db.createQueryReader(
    `SELECT count(*) cnt FROM ${ElementRefersToElements.classFullName}`,
    undefined,
    { usePrimaryConn: true }
  );
  if (!(await reader.step()))
    throw new Error("Relationship count query returned no row");
  return reader.current.cnt as number;
}

/**
 * Re-runs a full transformation into a target that a full transformation populated when the
 * fixture artifact was built, so every exported relationship already exists in the target. The
 * build-time run recorded provenance, which lets the re-run map source elements without
 * FederationGuids onto the existing target elements instead of inserting duplicates.
 */
export function standaloneFullRetransformation(
  dataset: PreparedDataset
): BenchmarkScenario {
  const { sourceDb, targetDb } = requirePopulatedStandaloneDataset(dataset);
  const editTxn = new EditTxn(
    targetDb,
    "Quick standalone full retransformation"
  );
  editTxn.start();
  let transformer: IModelTransformer | undefined;
  let populatedDigest: string | undefined;
  let disposed = false;
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    try {
      transformer?.dispose();
    } finally {
      if (editTxn.isActive) editTxn.end();
    }
  };
  return {
    abort: dispose,
    async prepare() {
      const sourceRelationships = await countRelationships(sourceDb);
      const targetRelationships = await countRelationships(targetDb);
      if (targetRelationships !== sourceRelationships)
        throw new Error(
          `Populated target has ${targetRelationships} of ${sourceRelationships} source relationships`
        );
      populatedDigest = await outputShapeDigest(targetDb, outputClassQueries);
      transformer = new IModelTransformer(
        { source: sourceDb, target: editTxn },
        { loadSourceGeometry: true }
      );
    },
    async measure() {
      if (transformer === undefined)
        throw new Error("Standalone full retransformation was not prepared");
      await transformer.process();
    },
    async finish() {
      editTxn.saveChanges("complete quick standalone full retransformation");
      dispose();
      const digest = await outputShapeDigest(targetDb, outputClassQueries);
      // Any duplicate element, relationship, or provenance aspect changes the class counts.
      if (digest !== populatedDigest)
        throw new Error(
          `Full retransformation changed the populated target output shape: before=${populatedDigest}, after=${digest}`
        );
      return digest;
    },
  };
}

export const standaloneFullRetransformationScenario: BenchmarkScenarioDefinition =
  {
    id: "standalone-full-retransformation",
    defaultFixtureId: "relationship-heavy-retransform",
    capabilities: {
      topology: "standalone-source-and-populated-target",
      requiredClaims: ["full retransformation"],
    },
    factory: standaloneFullRetransformation,
  };

export const standaloneFullRetransformationBenchmark = defineBenchmark({
  scenario: standaloneFullRetransformationScenario,
  fixtures: [relationshipHeavyRetransformFixture],
});
