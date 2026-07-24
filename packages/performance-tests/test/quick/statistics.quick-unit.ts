/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "chai";
import { classifyVariance } from "./BenchmarkReporter";
import {
  coefficientOfVariation,
  median,
  medianAbsoluteDeviation,
  percentile,
} from "./validation/statistics";

describe("quick performance statistics", () => {
  it("calculates robust summary statistics", () => {
    expect(median([4, 1, 3, 2])).to.equal(2.5);
    expect(percentile([1, 2, 3, 4, 5], 0.9)).to.equal(4.6);
    expect(medianAbsoluteDeviation([1, 2, 3, 4, 100])).to.equal(1);
    expect(coefficientOfVariation([10, 10, 10])).to.equal(0);
  });

  it("requires coefficient of variation at or below five percent", () => {
    expect(classifyVariance(8, 0.05, 0.05)).to.equal("stable");
    expect(classifyVariance(8, 0.050_001, 0.01)).to.equal("unstable");
    expect(classifyVariance(1, 0, 0)).to.equal("insufficient-samples");
  });
});
