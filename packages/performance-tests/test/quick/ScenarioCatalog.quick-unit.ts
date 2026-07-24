/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "chai";
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

  it("rejects unknown scenarios", () => {
    expect(() => getScenarioDefinition("not-a-scenario")).to.throw(
      'Unknown quick performance scenario "not-a-scenario". Available scenarios: incremental-synchronization'
    );
  });
});
