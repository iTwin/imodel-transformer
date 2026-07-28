/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "chai";
import {
  defaultScenarioBudgetMilliseconds,
  scenarioBudgetMilliseconds,
} from "./BenchmarkScenario";
import {
  defaultQuickPerformanceScenarioId,
  getScenarioDefinition,
} from "./ScenarioCatalog";

describe("quick performance scenario catalog", () => {
  it("selects incremental synchronization by default", () => {
    expect(getScenarioDefinition().id).to.equal(
      defaultQuickPerformanceScenarioId
    );
    expect(defaultQuickPerformanceScenarioId).to.equal(
      "incremental-synchronization"
    );
  });

  it("defaults the budget when a scenario does not declare one", () => {
    expect(scenarioBudgetMilliseconds(getScenarioDefinition())).to.equal(
      defaultScenarioBudgetMilliseconds
    );
  });

  it("honours a declared budget", () => {
    expect(
      scenarioBudgetMilliseconds({
        ...getScenarioDefinition(),
        budgetMilliseconds: 1000,
      })
    ).to.equal(1000);
  });

  it("rejects a non-positive budget", () => {
    expect(() =>
      scenarioBudgetMilliseconds({
        ...getScenarioDefinition(),
        budgetMilliseconds: 0,
      })
    ).to.throw(/invalid budget/);
  });

  it("rejects unknown scenarios", () => {
    expect(() => getScenarioDefinition("not-a-scenario")).to.throw(
      'Unknown quick performance scenario "not-a-scenario". Available scenarios: incremental-synchronization'
    );
  });
});
