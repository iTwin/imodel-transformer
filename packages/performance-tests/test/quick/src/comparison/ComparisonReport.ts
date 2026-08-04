/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { BenchmarkSample } from "../framework/BenchmarkRunner.js";
import { median } from "../reporting/statistics.js";
import { TransformerProvenance } from "./TransformerProvenance.js";

export type ComparisonArm = "baseline" | "candidate";

export interface ComparisonArmResult {
  readonly revision: string;
  readonly samples: readonly BenchmarkSample[];
}

export interface ComparisonReportInput {
  readonly baseline: ComparisonArmResult;
  readonly candidate: ComparisonArmResult;
  readonly informationalThresholdPercent: number;
  readonly measuredSamplesPerArm: number;
  readonly ordering: readonly ComparisonArm[];
}

export type InformationalComparisonStatus =
  | "candidate-faster-than-threshold"
  | "candidate-slower-than-threshold"
  | "within-informational-threshold";

export interface ComparisonSummary {
  readonly reportSchemaVersion: 1;
  readonly scenarioId: string;
  readonly fixtureId: string;
  readonly fixtureVersion: number;
  readonly fixtureRecipeHash: string;
  readonly fixtureContentHash: string;
  readonly semanticDigest: string;
  readonly policy: {
    readonly warmupsPerArm: 1;
    readonly measuredSamplesPerArm: number;
    readonly ordering: "alternating";
    readonly informationalThresholdPercent: number;
  };
  readonly baseline: {
    readonly revision: string;
    readonly transformerProvenance: TransformerProvenance;
    readonly medianMilliseconds: number;
    readonly measuredMilliseconds: readonly number[];
  };
  readonly candidate: {
    readonly revision: string;
    readonly transformerProvenance: TransformerProvenance;
    readonly medianMilliseconds: number;
    readonly measuredMilliseconds: readonly number[];
  };
  readonly percentageDelta: number;
  readonly informationalStatus: InformationalComparisonStatus;
  readonly informationalOnly: true;
  readonly executionOrder: readonly ComparisonArm[];
}

function configurationIdentity(sample: BenchmarkSample): string {
  return JSON.stringify([
    sample.reportSchemaVersion,
    sample.scenarioId,
    sample.fixtureId,
    sample.fixtureVersion,
    sample.fixtureRecipeHash,
    sample.fixtureContentHash,
    {
      coreBackend: sample.fixtureGenerator.coreBackend,
      node: sample.fixtureGenerator.node,
    },
    sample.topology,
    sample.operations,
  ]);
}

function validateArm(
  arm: ComparisonArm,
  result: ComparisonArmResult,
  measuredSamplesPerArm: number
): TransformerProvenance {
  const warmups = result.samples.filter((sample) => !sample.measured);
  const measured = result.samples.filter((sample) => sample.measured);
  if (
    warmups.length !== 1 ||
    warmups[0].sample !== 0 ||
    measured.length !== measuredSamplesPerArm
  )
    throw new Error(
      `${arm} must contain one warm-up and ${measuredSamplesPerArm} measured samples`
    );
  const measuredIds = measured
    .map((sample) => sample.sample)
    .sort((a, b) => a - b);
  const expectedIds = Array.from(
    { length: measuredSamplesPerArm },
    (_, index) => index + 1
  );
  if (JSON.stringify(measuredIds) !== JSON.stringify(expectedIds))
    throw new Error(`${arm} measured sample identifiers are incomplete`);
  const transformerProvenances = new Set(
    result.samples.map((sample) => JSON.stringify(sample.transformerProvenance))
  );
  const transformerProvenance = result.samples[0].transformerProvenance;
  if (transformerProvenances.size !== 1 || transformerProvenance === undefined)
    throw new Error(`${arm} workers did not resolve one transformer build`);
  return transformerProvenance;
}

export function percentageDelta(
  baselineMilliseconds: number,
  candidateMilliseconds: number
): number {
  if (!Number.isFinite(baselineMilliseconds) || baselineMilliseconds <= 0)
    throw new Error("Baseline median must be a positive finite number");
  if (!Number.isFinite(candidateMilliseconds) || candidateMilliseconds < 0)
    throw new Error("Candidate median must be a non-negative finite number");
  return (
    ((candidateMilliseconds - baselineMilliseconds) / baselineMilliseconds) *
    100
  );
}

export function createComparisonSummary(
  input: ComparisonReportInput
): ComparisonSummary {
  if (
    !Number.isSafeInteger(input.measuredSamplesPerArm) ||
    input.measuredSamplesPerArm < 1
  )
    throw new Error(
      "A/B comparison requires at least one measured sample per arm"
    );
  if (
    !Number.isFinite(input.informationalThresholdPercent) ||
    input.informationalThresholdPercent < 0
  )
    throw new Error(
      "The informational threshold must be a non-negative number"
    );

  const baselineTransformer = validateArm(
    "baseline",
    input.baseline,
    input.measuredSamplesPerArm
  );
  const candidateTransformer = validateArm(
    "candidate",
    input.candidate,
    input.measuredSamplesPerArm
  );
  const allSamples = [...input.baseline.samples, ...input.candidate.samples];
  const fixtureContentHashes = new Set(
    allSamples.map((sample) => sample.fixtureContentHash)
  );
  const fixtureContentHash = [...fixtureContentHashes][0];
  if (fixtureContentHashes.size !== 1 || fixtureContentHash === undefined)
    throw new Error(
      "Baseline and candidate must use the same immutable fixture artifact"
    );
  if (new Set(allSamples.map(configurationIdentity)).size !== 1)
    throw new Error(
      "Baseline and candidate must use the identical scenario and configured fixture"
    );
  const semanticDigests = new Set(
    allSamples.map((sample) => sample.semanticDigest)
  );
  if (semanticDigests.size !== 1)
    throw new Error(
      "Baseline and candidate produced different semantic results"
    );

  const baselineMeasured = input.baseline.samples
    .filter((sample) => sample.measured)
    .map((sample) => sample.wallMilliseconds);
  const candidateMeasured = input.candidate.samples
    .filter((sample) => sample.measured)
    .map((sample) => sample.wallMilliseconds);
  const baselineMedian = median(baselineMeasured);
  const candidateMedian = median(candidateMeasured);
  const delta = percentageDelta(baselineMedian, candidateMedian);
  const informationalStatus: InformationalComparisonStatus =
    delta > input.informationalThresholdPercent
      ? "candidate-slower-than-threshold"
      : delta < -input.informationalThresholdPercent
        ? "candidate-faster-than-threshold"
        : "within-informational-threshold";
  const identity = allSamples[0];

  return {
    reportSchemaVersion: 1,
    scenarioId: identity.scenarioId,
    fixtureId: identity.fixtureId,
    fixtureVersion: identity.fixtureVersion,
    fixtureRecipeHash: identity.fixtureRecipeHash,
    fixtureContentHash,
    semanticDigest: identity.semanticDigest,
    policy: {
      warmupsPerArm: 1,
      measuredSamplesPerArm: input.measuredSamplesPerArm,
      ordering: "alternating",
      informationalThresholdPercent: input.informationalThresholdPercent,
    },
    baseline: {
      revision: input.baseline.revision,
      transformerProvenance: baselineTransformer,
      medianMilliseconds: baselineMedian,
      measuredMilliseconds: baselineMeasured,
    },
    candidate: {
      revision: input.candidate.revision,
      transformerProvenance: candidateTransformer,
      medianMilliseconds: candidateMedian,
      measuredMilliseconds: candidateMeasured,
    },
    percentageDelta: delta,
    informationalStatus,
    informationalOnly: true,
    executionOrder: input.ordering,
  };
}

function formatMilliseconds(value: number): string {
  return `${value.toFixed(2)} ms`;
}

function markdown(summary: ComparisonSummary): string {
  const signedDelta =
    summary.percentageDelta > 0
      ? `+${summary.percentageDelta.toFixed(2)}%`
      : `${summary.percentageDelta.toFixed(2)}%`;
  return [
    "# Quick performance A/B comparison",
    "",
    "> Informational only. This small sample does not establish statistical confidence and is not a merge-blocking result.",
    "",
    `Scenario: \`${summary.scenarioId}\`  `,
    `Fixture: \`${summary.fixtureId}\` (version ${summary.fixtureVersion})`,
    "",
    "| Arm | Revision | Transformer | Median | Measured samples |",
    "| --- | --- | --- | ---: | --- |",
    `| Baseline | \`${summary.baseline.revision}\` | \`${summary.baseline.transformerProvenance.version}\` | ${formatMilliseconds(summary.baseline.medianMilliseconds)} | ${summary.baseline.measuredMilliseconds.map(formatMilliseconds).join(", ")} |`,
    `| Candidate | \`${summary.candidate.revision}\` | \`${summary.candidate.transformerProvenance.version}\` | ${formatMilliseconds(summary.candidate.medianMilliseconds)} | ${summary.candidate.measuredMilliseconds.map(formatMilliseconds).join(", ")} |`,
    "",
    `**Candidate delta:** ${signedDelta}  `,
    `**Informational status (${summary.policy.informationalThresholdPercent}% threshold):** \`${summary.informationalStatus}\``,
    "",
  ].join("\n");
}

export class ComparisonReporter {
  public static write(
    outputDir: string,
    input: ComparisonReportInput
  ): ComparisonSummary {
    const summary = createComparisonSummary(input);
    fs.mkdirSync(outputDir, { recursive: true });
    fs.writeFileSync(
      path.join(outputDir, "comparison.json"),
      `${JSON.stringify(summary, undefined, 2)}\n`
    );
    fs.writeFileSync(path.join(outputDir, "comparison.md"), markdown(summary));
    const records = [
      ...input.baseline.samples.map((sample) => ({
        arm: "baseline" as const,
        revision: input.baseline.revision,
        ...sample,
      })),
      ...input.candidate.samples.map((sample) => ({
        arm: "candidate" as const,
        revision: input.candidate.revision,
        ...sample,
      })),
    ];
    fs.writeFileSync(
      path.join(outputDir, "comparison-samples.jsonl"),
      `${records.map((record) => JSON.stringify(record)).join("\n")}\n`
    );
    return summary;
  }
}
