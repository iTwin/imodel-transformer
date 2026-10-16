/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { IModelDb } from "@itwin/core-backend";
import {
  BuiltFixture,
  requireStandaloneDataset,
} from "../../src/fixtures/FixtureProvider.js";
import { configureFixture } from "../../src/fixtures/FixtureRecipe.js";
import { standaloneFixtureProvider } from "../../src/fixtures/providers/standaloneProvider.js";
import { relationshipHeavyTransformRecipe } from "../../src/fixtures/recipes/relationshipHeavyTransform.js";
import { standaloneFullRetransformation } from "../../src/scenarios/standaloneFullRetransformation.js";
import {
  shutdownIsolatedHost,
  startIsolatedHost,
} from "../support/isolatedHost.js";

const elementCount = 200;
const relationshipsPerElement = 3;

// a small invocation of the registered recipe keeps this correctness test fast
const smallRelationshipHeavyFixture = configureFixture(
  relationshipHeavyTransformRecipe,
  {
    id: "relationship-heavy-retransform-test",
    version: 1,
    label: "small relationship-heavy retransformation test",
    scenarioClaims: ["full transformation"],
    topology: "standalone-source-and-empty-target",
    seed: 15485863,
    parameters: { elementCount, relationshipsPerElement },
  }
);

interface RelationshipRow {
  readonly id: string;
  readonly sourceLabel: string;
  readonly targetLabel: string;
  readonly priority: number;
}

async function queryRelationships(db: IModelDb): Promise<RelationshipRow[]> {
  const rows: RelationshipRow[] = [];
  const reader = db.createQueryReader(
    `SELECT r.ECInstanceId id, s.UserLabel sourceLabel, t.UserLabel targetLabel, r.MemberPriority priority
     FROM bis.ElementGroupsMembers r
     JOIN Generic.PhysicalObject s ON s.ECInstanceId = r.SourceECInstanceId
     JOIN Generic.PhysicalObject t ON t.ECInstanceId = r.TargetECInstanceId
     ORDER BY r.ECInstanceId`,
    undefined,
    { usePrimaryConn: true }
  );
  while (await reader.step())
    rows.push({
      id: reader.current.id,
      sourceLabel: reader.current.sourceLabel,
      targetLabel: reader.current.targetLabel,
      priority: reader.current.priority,
    });
  return rows;
}

async function queryPhysicalObjectIds(db: IModelDb): Promise<string[]> {
  const ids: string[] = [];
  const reader = db.createQueryReader(
    "SELECT ECInstanceId id FROM Generic.PhysicalObject ORDER BY ECInstanceId",
    undefined,
    { usePrimaryConn: true }
  );
  while (await reader.step()) ids.push(reader.current.id);
  return ids;
}

const endpointKey = ({ sourceLabel, targetLabel, priority }: RelationshipRow) =>
  `${sourceLabel}->${targetLabel}#${priority}`;

describe("standalone full retransformation scenario", () => {
  let built: BuiltFixture;
  let root = "";

  beforeAll(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-retransformation-"));
    await startIsolatedHost();
    built = await standaloneFixtureProvider.build(
      smallRelationshipHeavyFixture,
      path.join(root, "artifact")
    );
  });

  afterAll(async () => {
    if (built) await standaloneFixtureProvider.disposeBuild(built);
    await shutdownIsolatedHost();
    if (root !== "") fs.rmSync(root, { recursive: true, force: true });
  });

  it("re-imports every relationship onto the existing target relationships", async () => {
    const digests: string[] = [];
    for (let sample = 0; sample < 2; sample++) {
      const dataset = requireStandaloneDataset(
        await standaloneFixtureProvider.materialize(
          built,
          path.join(root, `sample-${sample}`),
          `retransformation-${sample}`
        )
      );
      const scenario = standaloneFullRetransformation(dataset);
      try {
        const sourceRelationships = await queryRelationships(dataset.sourceDb);
        expect(sourceRelationships).to.have.length(
          elementCount * relationshipsPerElement
        );

        await scenario.prepare?.();
        const populatedElementIds = await queryPhysicalObjectIds(
          dataset.targetDb
        );
        const populatedRelationships = await queryRelationships(
          dataset.targetDb
        );
        expect(populatedElementIds).to.have.length(elementCount);
        expect(populatedRelationships.map(endpointKey).sort()).to.deep.equal(
          sourceRelationships.map(endpointKey).sort()
        );

        await scenario.measure();
        // the re-run reuses the same target elements and relationship instances
        expect(await queryPhysicalObjectIds(dataset.targetDb)).to.deep.equal(
          populatedElementIds
        );
        expect(await queryRelationships(dataset.targetDb)).to.deep.equal(
          populatedRelationships
        );

        digests.push(await scenario.finish());
      } finally {
        scenario.abort();
        await standaloneFixtureProvider.disposeSample(dataset);
        fs.rmSync(dataset.directory, { recursive: true, force: true });
      }
    }
    expect(new Set(digests).size).to.equal(1);
  });

  it("rejects measurement before preparation and ends the edit transaction on abort", async () => {
    const dataset = requireStandaloneDataset(
      await standaloneFixtureProvider.materialize(
        built,
        path.join(root, "unprepared"),
        "retransformation-unprepared"
      )
    );
    try {
      const scenario = standaloneFullRetransformation(dataset);
      await expect(scenario.measure()).rejects.toThrow(/was not prepared/);
      scenario.abort();
      expect(() => scenario.abort()).not.to.throw();
      // a second scenario can start its own edit transaction only after the first one ended
      standaloneFullRetransformation(dataset).abort();
    } finally {
      await standaloneFixtureProvider.disposeSample(dataset);
      fs.rmSync(dataset.directory, { recursive: true, force: true });
    }
  });
});
