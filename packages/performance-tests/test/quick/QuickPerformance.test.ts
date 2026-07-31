/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import { BenchmarkReporter } from "./src/reporting/BenchmarkReporter.js";
import {
  resolveBenchmarkRunFromEnvironment,
  resolveMeasuredSamplesFromEnvironment,
} from "./src/framework/BenchmarkResolution.js";
import { BenchmarkRunner } from "./src/framework/BenchmarkRunner.js";
import { scenarioBudgetMilliseconds } from "./src/framework/BenchmarkScenario.js";
import { quickPath } from "./src/support/paths.js";

describe("quick performance", () => {
  const { descriptor, fixture, scenario } =
    resolveBenchmarkRunFromEnvironment();
  const measuredSamples = resolveMeasuredSamplesFromEnvironment();
  const budgetMilliseconds = scenarioBudgetMilliseconds(scenario);

  it(`${scenario.id} completes within budget on ${descriptor.id}`, async () => {
    const outputDir =
      process.env.QUICK_PERF_OUTPUT ??
      quickPath(".quick-output", descriptor.id);
    const started = process.hrtime.bigint();
    const samples = await new BenchmarkRunner(fixture, outputDir, scenario).run(
      measuredSamples
    );
    const elapsedMilliseconds =
      Number(process.hrtime.bigint() - started) / 1_000_000;
    const summary = BenchmarkReporter.write(
      outputDir,
      samples,
      elapsedMilliseconds
    );

    expect(summary.measuredSamples).to.equal(measuredSamples);
    expect(
      new Set(samples.map((sample) => sample.semanticDigest)).size
    ).to.equal(1, "every sample must observe the same fixture");
    expect(elapsedMilliseconds).to.be.lessThan(budgetMilliseconds);
  });
});
