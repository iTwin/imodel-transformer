/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  BuiltFixture,
  PreparedStandaloneDataset,
  requireStandaloneDataset,
} from "../../src/fixtures/FixtureProvider.js";
import {
  queryRealisticBuildingCounts,
  realisticBuildingExpectedCounts,
  realisticBuildingTransformFixture,
} from "../../src/fixtures/recipes/realisticBuildingTransform.js";
import { standaloneFixtureProvider } from "../../src/fixtures/providers/standaloneProvider.js";
import { standaloneFullTransformation } from "../../src/scenarios/standaloneFullTransformation.js";
import {
  shutdownIsolatedHost,
  startIsolatedHost,
} from "../support/isolatedHost.js";

const expectedSemanticDigest =
  "d2e96eadb8f4c46a67ed9bef48d464fd4679e236896d6dc0587adb847ce77e20";

function sha256(fileName: string): string {
  return createHash("sha256").update(fs.readFileSync(fileName)).digest("hex");
}

async function disposeSamples(
  datasets: readonly (PreparedStandaloneDataset | undefined)[]
): Promise<unknown[]> {
  const errors: unknown[] = [];
  for (const dataset of datasets) {
    if (dataset === undefined) continue;
    try {
      await standaloneFixtureProvider.disposeSample(dataset);
    } catch (error) {
      errors.push(error);
    }
    fs.rmSync(dataset.directory, { recursive: true, force: true });
  }
  return errors;
}

describe("realistic synthetic building fixture", () => {
  let built: BuiltFixture;
  let root = "";

  beforeAll(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-realistic-building-"));
    await startIsolatedHost();
    built = await standaloneFixtureProvider.build(
      realisticBuildingTransformFixture,
      path.join(root, "artifact")
    );
  });

  afterAll(async () => {
    if (built) await standaloneFixtureProvider.disposeBuild(built);
    await shutdownIsolatedHost();
    if (root !== "") fs.rmSync(root, { recursive: true, force: true });
  });

  it("materializes deterministic immutable sources and fully transforms one fresh target", async () => {
    let first: PreparedStandaloneDataset | undefined;
    let second: PreparedStandaloneDataset | undefined;
    let testError: unknown;
    try {
      first = requireStandaloneDataset(
        await standaloneFixtureProvider.materialize(
          built,
          path.join(root, "sample-1"),
          "realistic-building-1"
        )
      );
      second = requireStandaloneDataset(
        await standaloneFixtureProvider.materialize(
          built,
          path.join(root, "sample-2"),
          "realistic-building-2"
        )
      );
      const firstSourceHash = sha256(first.sourceDb.pathName);
      expect(first.sourceDb.isReadonly).to.be.true;
      expect(second.sourceDb.isReadonly).to.be.true;
      expect(sha256(second.sourceDb.pathName)).to.equal(firstSourceHash);
      expect(await queryRealisticBuildingCounts(first.sourceDb)).to.deep.equal(
        realisticBuildingExpectedCounts
      );
      expect(await queryRealisticBuildingCounts(second.sourceDb)).to.deep.equal(
        realisticBuildingExpectedCounts
      );

      const scenario = standaloneFullTransformation(first);
      try {
        await scenario.prepare?.();
        await scenario.measure();
        expect(await scenario.finish()).to.equal(expectedSemanticDigest);
      } finally {
        scenario.abort();
      }

      expect(sha256(first.sourceDb.pathName)).to.equal(firstSourceHash);
      expect(await queryRealisticBuildingCounts(first.targetDb)).to.deep.equal(
        realisticBuildingExpectedCounts
      );
    } catch (error) {
      testError = error;
    }
    const errors = [
      ...(testError === undefined ? [] : [testError]),
      ...(await disposeSamples([first, second])),
    ];
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1)
      throw new AggregateError(
        errors,
        "Realistic-building fixture test and cleanup failed"
      );
  });
});
