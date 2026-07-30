/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { BenchmarkSample } from "../framework/BenchmarkRunner.js";
import {
  coefficientOfVariation,
  median,
  medianAbsoluteDeviation,
  percentile,
} from "./statistics.js";
import { resolvedVersions } from "../support/versions.js";

export const maximumCoefficientOfVariation = 0.05;
export const maximumNormalizedMad = 0.05;
export const minimumMeasuredSamplesForReliability = 8;

function reportIdentity(sample: BenchmarkSample): string {
  return JSON.stringify([
    sample.reportSchemaVersion,
    sample.fixtureId,
    sample.fixtureVersion,
    sample.fixtureRecipeHash,
    sample.fixtureGenerator.coreBackend,
    sample.fixtureGenerator.node,
    sample.fixtureGenerator.transformer,
  ]);
}

export function classifyVariance(
  measuredSamples: number,
  observedCoefficientOfVariation: number,
  normalizedMad: number
): "insufficient-samples" | "stable" | "unstable" {
  if (measuredSamples < minimumMeasuredSamplesForReliability)
    return "insufficient-samples";
  return observedCoefficientOfVariation <= maximumCoefficientOfVariation &&
    normalizedMad <= maximumNormalizedMad
    ? "stable"
    : "unstable";
}

export class BenchmarkReporter {
  public static write(
    outputDir: string,
    samples: readonly BenchmarkSample[],
    jobMilliseconds?: number
  ) {
    if (samples.length === 0)
      throw new Error("Cannot report an empty quick performance sample set");
    const scenarioIds = new Set(samples.map((sample) => sample.scenarioId));
    if (scenarioIds.size !== 1)
      throw new Error(
        `Cannot mix quick performance scenarios in one report: ${[
          ...scenarioIds,
        ].join(", ")}`
      );
    const reportIdentities = new Set(samples.map(reportIdentity));
    if (reportIdentities.size !== 1)
      throw new Error(
        "Cannot mix quick performance fixture identities in one report"
      );
    const identity = samples[0];
    const measured = samples.filter((sample) => sample.measured);
    const walls = measured.map((sample) => sample.wallMilliseconds);
    const wallMedian = median(walls);
    const wallCoefficientOfVariation = coefficientOfVariation(walls);
    const wallMad = medianAbsoluteDeviation(walls);
    const reconstruction = samples.map(
      (sample) => sample.reconstructionMilliseconds
    );
    const phaseSummary = (values: readonly number[]) => ({
      median: median(values),
      maximum: Math.max(...values),
      total: values.reduce((sum, value) => sum + value, 0),
    });
    const summary = {
      fixtureId: measured[0]?.fixtureId,
      fixtureVersion: identity.fixtureVersion,
      fixtureRecipeHash: identity.fixtureRecipeHash,
      fixtureGenerator: identity.fixtureGenerator,
      topology: measured[0]?.topology,
      /** Stage 1 runs once per job, so this is a scalar, not a per-sample distribution. */
      fixtureBuildMilliseconds: samples[0].fixtureBuildMilliseconds,
      jobMilliseconds,
      measuredSamples: measured.length,
      reportSchemaVersion: identity.reportSchemaVersion,
      scenarioId: samples[0].scenarioId,
      versions: resolvedVersions(),
      varianceStatus: classifyVariance(
        measured.length,
        wallCoefficientOfVariation,
        wallMad / wallMedian
      ),
      varianceThresholds: {
        coefficientOfVariation: maximumCoefficientOfVariation,
        normalizedMad: maximumNormalizedMad,
      },
      unstableSamples: measured
        .filter(
          (sample) =>
            Math.abs(sample.wallMilliseconds - wallMedian) / wallMedian > 0.15
        )
        .map((sample) => sample.sample),
      wallMilliseconds: {
        median: wallMedian,
        p90: percentile(walls, 0.9),
        p95: percentile(walls, 0.95),
        mad: wallMad,
        normalizedMad: wallMad / wallMedian,
        coefficientOfVariation: wallCoefficientOfVariation,
        minimum: Math.min(...walls),
        maximum: Math.max(...walls),
      },
      reconstructionMilliseconds: phaseSummary(reconstruction),
      verificationMilliseconds: phaseSummary(
        samples.map((sample) => sample.verificationMilliseconds)
      ),
      teardownMilliseconds: phaseSummary(
        samples.map((sample) => sample.teardownMilliseconds)
      ),
    };
    fs.mkdirSync(outputDir, { recursive: true });
    fs.writeFileSync(
      path.join(outputDir, "samples.jsonl"),
      `${samples.map((sample) => JSON.stringify(sample)).join("\n")}\n`
    );
    fs.writeFileSync(
      path.join(outputDir, "summary.json"),
      `${JSON.stringify(summary, undefined, 2)}\n`
    );
    fs.writeFileSync(
      path.join(outputDir, "summary.csv"),
      [
        "reportSchemaVersion,scenario,fixture,fixtureVersion,fixtureRecipeHash,fixtureNodeVersion,fixtureCoreBackendVersion,fixtureTransformerVersion,measuringNodeVersion,measuringCoreBackendVersion,measuringTransformerVersion,measuredSamples,jobMs,fixtureBuildMs,medianMs,p90Ms,p95Ms,madMs,cv,reconstructionTotalMs,verificationTotalMs,teardownTotalMs",
        [
          summary.reportSchemaVersion,
          summary.scenarioId,
          summary.fixtureId,
          summary.fixtureVersion,
          summary.fixtureRecipeHash,
          summary.fixtureGenerator.node,
          summary.fixtureGenerator.coreBackend,
          summary.fixtureGenerator.transformer,
          summary.versions.node,
          summary.versions.coreBackend,
          summary.versions.transformer,
          summary.measuredSamples,
          summary.jobMilliseconds ?? "",
          summary.fixtureBuildMilliseconds,
          summary.wallMilliseconds.median,
          summary.wallMilliseconds.p90,
          summary.wallMilliseconds.p95,
          summary.wallMilliseconds.mad,
          summary.wallMilliseconds.coefficientOfVariation,
          summary.reconstructionMilliseconds.total,
          summary.verificationMilliseconds.total,
          summary.teardownMilliseconds.total,
        ].join(","),
      ].join("\n")
    );
    return summary;
  }
}
