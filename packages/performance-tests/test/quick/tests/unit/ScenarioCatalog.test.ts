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
import {
  assertScenarioSupportsFixture,
  resolveBenchmarkRun,
} from "../../src/framework/BenchmarkResolution.js";
import { BenchmarkRegistration } from "../../src/framework/BenchmarkRegistration.js";

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
      'Unknown quick performance scenario "not-a-scenario". Available scenarios: incremental-synchronization, changeset-scanning, schema-processing, standalone-full-transformation, prefetch-full-transformation'
    );
  });

  it("registers the schema-processing scenario and its source fixture", () => {
    const resolved = resolveBenchmarkRun("schema-processing");
    expect(resolved.scenario.defaultFixtureId).to.equal(
      "schema-processing-large"
    );
    expect(resolved.descriptor.layout.topology).to.equal("source-only");
    expect(resolved.descriptor.scenarioClaims).to.include("schema processing");
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
        expect(() =>
          assertScenarioSupportsFixture(
            registration.scenario,
            fixture.descriptor
          )
        ).to.not.throw();
      }
    }
  });

  it("does not expose a mutable registration list", () => {
    const registrations = listBenchmarkRegistrations();
    expect(Object.isFrozen(registrations)).to.be.true;
    expect(() =>
      (registrations as BenchmarkRegistration[]).push(registrations[0])
    ).to.throw();
    expect(Object.isFrozen(registrations[0].scenario)).to.be.true;
    expect(Object.isFrozen(registrations[0].scenario.capabilities)).to.be.true;
    expect(() => {
      (registrations[0].scenario as { id: string }).id = "mutated";
    }).to.throw();
    expect(getScenarioDefinition().id).to.equal(
      defaultQuickPerformanceScenarioId
    );
  });
});
