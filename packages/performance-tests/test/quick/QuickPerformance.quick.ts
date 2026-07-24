/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "path";
import { expect } from "chai";
import { BenchmarkReporter } from "./BenchmarkReporter";
import { BenchmarkRunner } from "./BenchmarkRunner";
import { getFixtureDescriptor } from "./FixtureCatalog";
import { getScenarioDefinition } from "./ScenarioCatalog";

describe("quick transformer performance", function () {
  this.timeout(15 * 60 * 1000);

  it("runs the balanced incremental synchronization fixture", async () => {
    const scenario = getScenarioDefinition(process.env.QUICK_PERF_SCENARIO);
    const measuredSamples = Number(process.env.QUICK_PERF_SAMPLES ?? "8");
    const outputDir =
      process.env.QUICK_PERF_OUTPUT ??
      path.join(__dirname, ".quick-output", scenario.id);
    const runner = new BenchmarkRunner(
      getFixtureDescriptor("balanced-incremental"),
      outputDir,
      scenario
    );
    const jobStart = process.hrtime.bigint();
    const samples = await runner.run(measuredSamples);
    const jobMilliseconds =
      Number(process.hrtime.bigint() - jobStart) / 1_000_000;
    BenchmarkReporter.write(outputDir, samples, jobMilliseconds);

    expect(samples.filter((sample) => sample.measured)).to.have.length(
      measuredSamples
    );
    expect(
      new Set(samples.map((sample) => sample.semanticDigest)).size
    ).to.equal(
      1,
      "fresh reconstructions must produce the same semantic digest"
    );
    expect(new Set(samples.map((sample) => sample.scenarioId))).to.deep.equal(
      new Set([scenario.id])
    );
    expect(jobMilliseconds).to.be.lessThan(15 * 60 * 1000);
  });
});
