/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "chai";
import * as path from "path";
import { BenchmarkReporter } from "./BenchmarkReporter";
import { resolveBenchmarkRunFromEnvironment } from "./BenchmarkResolution";
import { BenchmarkRunner } from "./BenchmarkRunner";
import { scenarioBudgetMilliseconds } from "./BenchmarkScenario";

/**
 * Headroom above the scenario budget so the budget assertion reports the overrun before Mocha
 * kills the test. Without it the two deadlines race and the timeout always wins, which reports
 * "timeout exceeded" instead of the far more useful measured elapsed time.
 */
const budgetHeadroomMilliseconds = 60 * 1000;

describe("quick performance", () => {
  const { descriptor, scenario } = resolveBenchmarkRunFromEnvironment();
  const budgetMilliseconds = scenarioBudgetMilliseconds(scenario);
  const measuredSamples = Number(process.env.QUICK_PERF_SAMPLES ?? "8");

  it(`${scenario.id} completes within budget on ${descriptor.id}`, async function () {
    this.timeout(budgetMilliseconds + budgetHeadroomMilliseconds);
    const outputDir =
      process.env.QUICK_PERF_OUTPUT ??
      path.join(__dirname, ".quick-output", descriptor.id);
    const started = process.hrtime.bigint();
    const samples = await new BenchmarkRunner(
      descriptor,
      outputDir,
      scenario
    ).run(measuredSamples);
    const elapsedMilliseconds =
      Number(process.hrtime.bigint() - started) / 1_000_000;
    const summary = BenchmarkReporter.write(outputDir, samples);

    expect(summary.measuredSamples).to.equal(measuredSamples);
    expect(
      new Set(samples.map((sample) => sample.semanticDigest)).size
    ).to.equal(1, "every sample must observe the same fixture");
    expect(new Set(samples.map((sample) => sample.scenarioId))).to.deep.equal(
      new Set([scenario.id])
    );
    expect(elapsedMilliseconds).to.be.lessThan(budgetMilliseconds);
  });
});
