/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import {
  assertScenarioSupportsFixture,
  resolveBenchmarkRun,
  resolveBenchmarkRunFromEnvironment,
} from "./BenchmarkResolution";
import { BenchmarkScenarioDefinition } from "./BenchmarkScenario";
import {
  balancedIncrementalDescriptor,
  balancedIncrementalSourceOnlyDescriptor,
} from "./FixtureCatalog";
import { incrementalSynchronizationScenario } from "./scenarios/incrementalSynchronization";

describe("benchmark resolution", () => {
  it("resolves the scenario's declared default fixture", () => {
    const resolved = resolveBenchmarkRun();
    expect(resolved.scenario.id).to.equal("incremental-synchronization");
    expect(resolved.descriptor.id).to.equal(
      incrementalSynchronizationScenario.defaultFixtureId
    );
  });

  it("lets an explicit fixture id override the default", () => {
    const scenario: BenchmarkScenarioDefinition = {
      ...incrementalSynchronizationScenario,
      capabilities: { topology: "source-only" },
    };
    expect(() =>
      assertScenarioSupportsFixture(
        scenario,
        balancedIncrementalSourceOnlyDescriptor
      )
    ).to.not.throw();
  });

  it("rejects a fixture whose topology the scenario cannot consume", () => {
    expect(() =>
      assertScenarioSupportsFixture(
        incrementalSynchronizationScenario,
        balancedIncrementalSourceOnlyDescriptor
      )
    ).to.throw(/requires a "source-and-empty-target" fixture/);
  });

  it("rejects a fixture that does not make a required claim", () => {
    const scenario: BenchmarkScenarioDefinition = {
      ...incrementalSynchronizationScenario,
      capabilities: {
        topology: "source-and-empty-target",
        requiredClaims: ["time travel"],
      },
    };
    expect(() =>
      assertScenarioSupportsFixture(scenario, balancedIncrementalDescriptor)
    ).to.throw(/does not claim \[time travel\]/);
  });

  it("reports unknown fixture ids with the available set", () => {
    expect(() =>
      resolveBenchmarkRun("incremental-synchronization", "no-such-fixture")
    ).to.throw(/Available fixtures: balanced-incremental/);
  });

  it("treats blank environment inputs as unspecified", () => {
    const resolved = resolveBenchmarkRunFromEnvironment({
      QUICK_PERF_SCENARIO: "",
      QUICK_PERF_FIXTURE: "  ",
    });
    expect(resolved.scenario.id).to.equal("incremental-synchronization");
    expect(resolved.descriptor.id).to.equal("balanced-incremental");
  });
});
