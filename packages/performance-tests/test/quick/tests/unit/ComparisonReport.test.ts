/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  ComparisonReporter,
  createComparisonSummary,
  percentageDelta,
} from "../../src/comparison/ComparisonReport.js";
import { armSamples } from "./comparisonTestUtils.js";

describe("A/B comparison reporting", () => {
  const temporaryDirectories: string[] = [];

  afterEach(() => {
    for (const directory of temporaryDirectories)
      fs.rmSync(directory, { recursive: true, force: true });
    temporaryDirectories.length = 0;
  });

  function input() {
    return {
      baseline: {
        revision: "base-sha",
        samples: armSamples([90, 100, 110]),
      },
      candidate: {
        revision: "candidate-sha",
        samples: armSamples([99, 110, 121]),
      },
      fixtureAuthoring: {
        arm: "baseline" as const,
        revision: "base-sha",
        transformerVersion: "0.6.0",
      },
      informationalThresholdPercent: 5,
      measuredSamplesPerArm: 3,
      ordering: [
        "candidate",
        "baseline",
        "baseline",
        "candidate",
        "candidate",
        "baseline",
        "baseline",
        "candidate",
      ] as const,
    };
  }

  it("calculates the candidate percentage delta from arm medians", () => {
    expect(percentageDelta(100, 110)).to.equal(10);
    const summary = createComparisonSummary(input());
    expect(summary.baseline.medianMilliseconds).to.equal(100);
    expect(summary.candidate.medianMilliseconds).to.equal(110);
    expect(summary.percentageDelta).to.be.closeTo(10, 0.000_001);
    expect(summary.informationalStatus).to.equal(
      "candidate-slower-than-threshold"
    );
    expect(summary.informationalOnly).to.equal(true);
  });

  it("writes a simple JSON, Markdown, and raw-sample report", () => {
    const outputDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-ab-report-")
    );
    temporaryDirectories.push(outputDir);
    const summary = ComparisonReporter.write(outputDir, input());
    const report = JSON.parse(
      fs.readFileSync(path.join(outputDir, "comparison.json"), "utf8")
    ) as typeof summary;
    const markdown = fs.readFileSync(
      path.join(outputDir, "comparison.md"),
      "utf8"
    );
    const records = fs
      .readFileSync(path.join(outputDir, "comparison-samples.jsonl"), "utf8")
      .trim()
      .split("\n");

    expect(report).to.deep.equal(summary);
    expect(report.policy).to.deep.equal({
      warmupsPerArm: 1,
      measuredSamplesPerArm: 3,
      ordering: "alternating",
      informationalThresholdPercent: 5,
    });
    expect(markdown).to.contain("Informational only");
    expect(markdown).to.contain("does not establish statistical confidence");
    expect(markdown).to.contain(
      "Prepared target: baseline `base-sha` with transformer `0.6.0`"
    );
    expect(records).to.have.length(8);
  });

  it("rejects configuration and semantic mismatches", () => {
    const mismatchedFixture = input();
    mismatchedFixture.candidate.samples = armSamples([99, 110, 121]).map(
      (sample) => ({ ...sample, fixtureRecipeHash: "different-recipe" })
    );
    expect(() => createComparisonSummary(mismatchedFixture)).to.throw(
      /identical scenario and configured fixture/
    );

    const mismatchedSemantics = input();
    mismatchedSemantics.candidate.samples = armSamples([99, 110, 121]).map(
      (sample) => ({ ...sample, semanticDigest: "different-result" })
    );
    expect(() => createComparisonSummary(mismatchedSemantics)).to.throw(
      /different semantic results/
    );
  });

  it("rejects different fixture bytes even when semantic digests match", () => {
    const mismatchedContent = input();
    mismatchedContent.candidate.samples = armSamples([99, 110, 121]).map(
      (sample) => ({
        ...sample,
        fixtureContentHash: "different-fixture-content",
      })
    );
    expect(() => createComparisonSummary(mismatchedContent)).to.throw(
      /same immutable fixture artifact/
    );
  });

  it("keeps arm transformer provenance outside workload identity", () => {
    const versioned = input();
    versioned.baseline.samples = armSamples([90, 100, 110]).map((sample) => ({
      ...sample,
      fixtureGenerator: {
        ...sample.fixtureGenerator,
        transformer: "baseline-author-version",
      },
      transformerProvenance: {
        contentHash: "baseline-transformer-hash",
        entryPoint: "baseline/transformer.js",
        version: "baseline-version",
      },
    }));
    versioned.candidate.samples = armSamples([99, 110, 121]).map((sample) => ({
      ...sample,
      fixtureGenerator: {
        ...sample.fixtureGenerator,
        transformer: "candidate-author-version",
      },
      transformerProvenance: {
        contentHash: "candidate-transformer-hash",
        entryPoint: "candidate/transformer.js",
        version: "candidate-version",
      },
    }));

    const summary = createComparisonSummary(versioned);
    expect(summary.percentageDelta).to.be.closeTo(10, 0.000_001);
    expect(summary.baseline.transformerProvenance.version).to.equal(
      "baseline-version"
    );
    expect(summary.candidate.transformerProvenance.version).to.equal(
      "candidate-version"
    );
  });

  it("rejects incomplete arm samples and invalid baseline timing", () => {
    const incomplete = input();
    incomplete.baseline.samples = armSamples([90, 100]);
    expect(() => createComparisonSummary(incomplete)).to.throw(
      /one warm-up and 3 measured samples/
    );
    expect(() => percentageDelta(0, 10)).to.throw(/Baseline median/);
  });
});
