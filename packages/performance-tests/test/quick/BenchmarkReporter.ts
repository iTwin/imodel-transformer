/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { BenchmarkSample } from "./BenchmarkRunner";
import {
  coefficientOfVariation,
  median,
  medianAbsoluteDeviation,
  percentile,
} from "./validation/statistics";

export const maximumCoefficientOfVariation = 0.05;
export const maximumNormalizedMad = 0.05;
export const minimumMeasuredSamplesForReliability = 8;

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
  ): void {
    if (samples.length === 0)
      throw new Error("Cannot report an empty quick performance sample set");
    const scenarioIds = new Set(samples.map((sample) => sample.scenarioId));
    if (scenarioIds.size !== 1)
      throw new Error(
        `Cannot mix quick performance scenarios in one report: ${[
          ...scenarioIds,
        ].join(", ")}`
      );
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
      jobMilliseconds,
      measuredSamples: measured.length,
      scenarioId: samples[0].scenarioId,
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
        "scenario,fixture,measuredSamples,jobMs,medianMs,p90Ms,p95Ms,madMs,cv,reconstructionTotalMs,verificationTotalMs,teardownTotalMs",
        [
          summary.scenarioId,
          summary.fixtureId,
          summary.measuredSamples,
          summary.jobMilliseconds ?? "",
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
  }
}
