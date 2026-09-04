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
    expect(summary.baseline).to.include({
      medianMilliseconds: 100,
      p90Milliseconds: 108,
      minimumMilliseconds: 90,
      maximumMilliseconds: 110,
    });
    expect(summary.candidate).to.include({
      medianMilliseconds: 110,
      p90Milliseconds: 118.8,
      minimumMilliseconds: 99,
      maximumMilliseconds: 121,
    });
    expect(summary.percentageDelta).to.be.closeTo(10, 0.000_001);
    expect(summary.informationalStatus).to.equal(
      "candidate-slower-than-threshold"
    );
    expect(summary.informationalOnly).to.equal(true);
  });

  it("summarizes worker-reported peak RSS", () => {
    const withMemory = input();
    withMemory.baseline.samples = withMemory.baseline.samples.map(
      (sample, index) => ({
        ...sample,
        ...(sample.measured
          ? { workerPeakRssBytes: [90, 100, 110][index - 1] * 1048576 }
          : {}),
      })
    );
    withMemory.candidate.samples = withMemory.candidate.samples.map(
      (sample, index) => ({
        ...sample,
        ...(sample.measured
          ? { workerPeakRssBytes: [99, 110, 121][index - 1] * 1048576 }
          : {}),
      })
    );

    const summary = createComparisonSummary(withMemory);
    expect(summary.baseline.medianPeakRssBytes).to.equal(100 * 1048576);
    expect(summary.candidate.medianPeakRssBytes).to.equal(110 * 1048576);
  });

  it("writes a simple JSON, Markdown, and raw-sample report", () => {
    const outputDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-ab-report-")
    );
    temporaryDirectories.push(outputDir);
    const reportInput = input();
    reportInput.candidate.samples = reportInput.candidate.samples.map(
      (sample) => {
        if (sample.transformerProvenance === undefined)
          throw new Error("Test sample is missing transformer provenance");
        return {
          ...sample,
          transformerProvenance: {
            ...sample.transformerProvenance,
            coreBackendVersion: "5.13.0",
          },
        };
      }
    );
    const summary = ComparisonReporter.write(outputDir, reportInput);
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
    expect(markdown).to.contain("## Run configuration");
    expect(markdown).to.contain(
      "| Scenario | Mode | Fixture | Source iModel scale | Samples |"
    );
    expect(markdown).to.contain("1.00 MiB · 10 schemas · 100 classes");
    expect(markdown).to.contain("3 + 1 warm-up/arm");
    expect(markdown).to.contain(
      "Prepared target: baseline <code>base-sha</code> with transformer <code>0.6.0</code>."
    );
    expect(markdown).to.contain("## Result");
    expect(markdown).to.contain(
      "Core backend: baseline <code>5.10.3</code>, candidate <code>5.13.0</code>."
    );
    expect(markdown).to.contain("| Arm | Revision | Transformer | Median |");
    expect(markdown).to.contain(
      "How to interpret <code>candidate-slower-than-threshold</code>"
    );
    expect(markdown).to.contain("Where are the individual measurements?");
    expect(markdown).not.to.contain("90.00 ms, 100.00 ms, 110.00 ms");
    expect(records).to.have.length(8);
  });

  it("escapes scenario configuration in Markdown tables", () => {
    const outputDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-ab-report-escaped-")
    );
    temporaryDirectories.push(outputDir);
    const escaped = input();
    escaped.baseline.samples = escaped.baseline.samples.map((sample) => ({
      ...sample,
      scenarioConfiguration: { mode: "scan|next\nline" },
    }));
    escaped.candidate.samples = escaped.candidate.samples.map((sample) => ({
      ...sample,
      scenarioConfiguration: { mode: "scan|next\nline" },
    }));

    ComparisonReporter.write(outputDir, escaped);
    const markdown = fs.readFileSync(
      path.join(outputDir, "comparison.md"),
      "utf8"
    );
    expect(markdown).to.contain("<code>scan&#124;next line</code>");
  });

  it("rejects configuration and semantic mismatches", () => {
    const mismatchedFixture = input();
    mismatchedFixture.candidate.samples = armSamples([99, 110, 121]).map(
      (sample) => ({ ...sample, fixtureRecipeHash: "different-recipe" })
    );
    expect(() => createComparisonSummary(mismatchedFixture)).to.throw(
      /identical scenario and configured fixture/
    );

    const mismatchedConfiguration = input();
    mismatchedConfiguration.candidate.samples = armSamples([99, 110, 121]).map(
      (sample) => ({
        ...sample,
        scenarioConfiguration: { mode: "different" },
      })
    );
    expect(() => createComparisonSummary(mismatchedConfiguration)).to.throw(
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

  it("rejects invalid peak RSS values", () => {
    const invalid = input();
    invalid.baseline.samples = invalid.baseline.samples.map((sample, index) =>
      index === 1 ? { ...sample, workerPeakRssBytes: 0 } : sample
    );
    expect(() => createComparisonSummary(invalid)).to.throw(
      /baseline peak RSS samples must be positive finite numbers/
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

  it("reports different transformer and Core runtime provenance per arm", () => {
    const versioned = input();
    versioned.baseline.samples = armSamples([90, 100, 110]).map((sample) => ({
      ...sample,
      transformerProvenance: {
        coreBackendVersion: "5.10.3",
        contentHash: "baseline-transformer-hash",
        entryPoint: "baseline/transformer.js",
        version: "baseline-version",
      },
    }));
    versioned.candidate.samples = armSamples([99, 110, 121]).map((sample) => ({
      ...sample,
      transformerProvenance: {
        coreBackendVersion: "5.13.0",
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
    expect(summary.baseline.transformerProvenance.coreBackendVersion).to.equal(
      "5.10.3"
    );
    expect(summary.candidate.transformerProvenance.coreBackendVersion).to.equal(
      "5.13.0"
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
