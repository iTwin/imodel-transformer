/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import {
  defaultQuickPerformanceScenarioId,
  getScenarioDefinition,
} from "../../src/catalogs/ScenarioCatalog.js";
import {
  listBenchmarkRegistrations,
  listRegisteredFixtures,
  listRegisteredScenarios,
} from "../../src/catalogs/BenchmarkRegistry.js";
import { validateFixtureDescriptor } from "../../src/fixtures/FixtureDescriptor.js";
import { resolveBenchmarkRun } from "../../src/framework/BenchmarkResolution.js";

describe("quick performance scenario catalog", () => {
  it("selects incremental synchronization by default", () => {
    expect(getScenarioDefinition().id).to.equal(
      defaultQuickPerformanceScenarioId
    );
    expect(defaultQuickPerformanceScenarioId).to.equal(
      "incremental-synchronization"
    );
  });

  it("rejects unknown scenarios", () => {
    expect(() => getScenarioDefinition("not-a-scenario")).to.throw(
      'Unknown quick performance scenario "not-a-scenario". Available scenarios: incremental-synchronization'
    );
  });

  it("validates every registered benchmark and configured fixture", () => {
    const registeredScenarioIds = new Set(
      listRegisteredScenarios().map((scenario) => scenario.id)
    );
    const registeredFixtureIds = new Set(
      listRegisteredFixtures().map((fixture) => fixture.descriptor.id)
    );

    for (const registration of listBenchmarkRegistrations()) {
      expect(registeredScenarioIds.has(registration.scenario.id)).to.be.true;
      const resolved = resolveBenchmarkRun(registration.scenario.id);
      expect(resolved.scenario).to.equal(registration.scenario);
      for (const fixture of registration.fixtures ?? []) {
        expect(registeredFixtureIds.has(fixture.descriptor.id)).to.be.true;
        expect(validateFixtureDescriptor(fixture.descriptor)).to.equal(
          fixture.descriptor
        );
        expect(fixture.recipeId).to.equal(fixture.descriptor.layout.recipe);
      }
    }
  });
});
