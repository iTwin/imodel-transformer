/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  ConfiguredFixture,
  configureFixture,
} from "../../src/fixtures/FixtureRecipe.js";
import {
  balancedIncrementalDescriptor,
  balancedIncrementalRecipe,
} from "../../src/fixtures/recipes/balancedIncremental.js";
import {
  BuiltFixture,
  getFixtureProvider,
  requireFixtureArtifact,
} from "../../src/fixtures/FixtureProvider.js";
import { liveHubFixtureProvider } from "../../src/fixtures/providers/liveHubProvider.js";
import { quickTestHub } from "../../src/fixtures/QuickTestHub.js";
import { incrementalSynchronization } from "../../src/scenarios/incrementalSynchronization.js";
import {
  shutdownIsolatedHost,
  startIsolatedHost,
} from "../support/isolatedHost.js";

describe("live hub fixture artifact", () => {
  let buildCount = 0;
  let built: BuiltFixture;
  let fixture: ConfiguredFixture;
  let root: string;

  beforeAll(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-live-artifact-"));
    const configured = configureFixture(balancedIncrementalRecipe, {
      id: "balanced-incremental-live-artifact-test",
      version: 1,
      label: "balanced incremental live artifact test",
      scenarioClaims: balancedIncrementalDescriptor.scenarioClaims,
      topology: "source-and-empty-target",
      seed: 328,
      parameters: { scale: 1 },
    });
    fixture = {
      ...configured,
      async createSeed(fileName: string) {
        buildCount++;
        return configured.createSeed(fileName);
      },
    };
    await startIsolatedHost();
    built = await liveHubFixtureProvider.build(
      fixture,
      path.join(root, "fixture-artifact")
    );
  });

  afterAll(async () => {
    if (built) await liveHubFixtureProvider.disposeBuild(built);
    await shutdownIsolatedHost();
    if (root) fs.rmSync(root, { recursive: true, force: true });
  });

  it("selects the artifact-backed live provider", () => {
    expect(getFixtureProvider(fixture.descriptor)).to.equal(
      liveHubFixtureProvider
    );
    expect(requireFixtureArtifact(built).manifest.liveHub).not.to.be.undefined;
  });

  it("restores pristine incremental iModels without rebuilding the recipe", async () => {
    const digests: string[] = [];
    for (let sample = 0; sample < 2; sample++) {
      const sampleDir = path.join(root, `sample-${sample}`);
      const dataset = await liveHubFixtureProvider.materialize(
        built,
        sampleDir,
        `live-artifact-sample-${sample}`
      );
      const scenario = incrementalSynchronization(dataset);
      try {
        await scenario.measure();
        digests.push(await scenario.finish());
      } finally {
        scenario.abort();
        await liveHubFixtureProvider.disposeSample(dataset);
        fs.rmSync(sampleDir, { recursive: true, force: true });
      }
      expect(quickTestHub.isActive).to.equal(false);
    }

    expect(buildCount).to.equal(1);
    expect(new Set(digests).size).to.equal(1);
  });
});
