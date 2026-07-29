/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import { BenchmarkReporter } from "./BenchmarkReporter.js";
import { resolveBenchmarkRunFromEnvironment } from "./BenchmarkResolution.js";
import { BenchmarkRunner } from "./BenchmarkRunner.js";
import { scenarioBudgetMilliseconds } from "./BenchmarkScenario.js";
import { quickSourcePath } from "./quickPaths.js";

describe("quick performance", () => {
  const { descriptor, scenario } = resolveBenchmarkRunFromEnvironment();
  const budgetMilliseconds = scenarioBudgetMilliseconds(scenario);

  it(`${scenario.id} completes within budget on ${descriptor.id}`, async () => {
    const outputDir =
      process.env.QUICK_PERF_OUTPUT ??
      quickSourcePath(".quick-output", descriptor.id);
    const started = process.hrtime.bigint();
    const samples = await new BenchmarkRunner(
      descriptor,
      outputDir,
      scenario
    ).run();
    const elapsedMilliseconds =
      Number(process.hrtime.bigint() - started) / 1_000_000;
    const summary = BenchmarkReporter.write(
      outputDir,
      samples,
      elapsedMilliseconds
    );

    expect(summary.measuredSamples).to.equal(8);
    expect(
      new Set(samples.map((sample) => sample.semanticDigest)).size
    ).to.equal(1, "every sample must observe the same fixture");
    expect(elapsedMilliseconds).to.be.lessThan(budgetMilliseconds);
  });
});
