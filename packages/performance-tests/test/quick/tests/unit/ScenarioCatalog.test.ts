/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import {
  defaultQuickPerformanceScenarioId,
  getScenarioDefinition,
} from "../../src/catalogs/ScenarioCatalog.js";

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
      'Unknown quick performance scenario "not-a-scenario". Available scenarios: incremental-synchronization, dynamic-schema-union'
    );
  });

  it("resolves the dynamic schema union scenario", () => {
    const scenario = getScenarioDefinition("dynamic-schema-union");
    expect(scenario.id).to.equal("dynamic-schema-union");
    expect(scenario.defaultFixtureId).to.equal("dynamic-schema-union-medium");
    expect(scenario.capabilities).to.deep.equal({
      topology: "snapshot-schema-pair",
      requiredClaims: ["dynamic schema union"],
    });
  });
});
