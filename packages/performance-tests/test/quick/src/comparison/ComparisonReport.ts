/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import type { BenchmarkSample } from "../framework/BenchmarkRunner.js";
import { ScenarioConfiguration } from "../framework/BenchmarkScenario.js";
import { IModelInventory } from "../fixtures/IModelInventory.js";
import { median, percentile } from "../reporting/statistics.js";
import { TransformerProvenance } from "./TransformerProvenance.js";
import { ExternalFixtureSourceIdentity } from "../fixtures/FixtureDescriptor.js";

export type ComparisonArm = "baseline" | "candidate";

export type ComparisonSample = BenchmarkSample & {
  /** Peak RSS reported by the isolated worker's process resource usage. */
  readonly workerPeakRssBytes: number;
};

export interface ComparisonArmResult {
  readonly revision: string;
  readonly samples: readonly ComparisonSample[];
}

export interface ComparisonReportInput {
  readonly baseline: ComparisonArmResult;
  readonly candidate: ComparisonArmResult;
  readonly fixtureAuthoring: {
    readonly arm: "baseline";
    readonly revision: string;
    readonly transformerVersion: string;
  };
  readonly informationalThresholdPercent: number;
  readonly measuredSamplesPerArm: number;
  readonly ordering: readonly ComparisonArm[];
}

export type InformationalComparisonStatus =
  | "candidate-faster-than-threshold"
  | "candidate-slower-than-threshold"
  | "within-informational-threshold";

export interface ComparisonSummary {
  readonly reportSchemaVersion: 3;
  readonly scenarioId: string;
  readonly fixtureId: string;
  readonly fixtureVersion: number;
  readonly fixtureRecipeHash: string;
  readonly fixtureContentHash: string;
  readonly fixtureInventory?: IModelInventory;
  readonly scenarioConfiguration?: ScenarioConfiguration;
  readonly fixtureAuthoring: ComparisonReportInput["fixtureAuthoring"];
  readonly fixtureSource?: ExternalFixtureSourceIdentity;
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
    readonly p90Milliseconds: number;
    readonly minimumMilliseconds: number;
    readonly maximumMilliseconds: number;
    readonly measuredMilliseconds: readonly number[];
    readonly medianPeakRssBytes: number;
    readonly measuredPeakRssBytes: readonly number[];
  };
  readonly candidate: {
    readonly revision: string;
    readonly transformerProvenance: TransformerProvenance;
    readonly medianMilliseconds: number;
    readonly p90Milliseconds: number;
    readonly minimumMilliseconds: number;
    readonly maximumMilliseconds: number;
    readonly measuredMilliseconds: readonly number[];
    readonly medianPeakRssBytes: number;
    readonly measuredPeakRssBytes: readonly number[];
  };
  readonly percentageDelta: number;
  readonly informationalStatus: InformationalComparisonStatus;
  readonly informationalOnly: true;
  readonly executionOrder: readonly ComparisonArm[];
}

function configurationIdentity(sample: ComparisonSample): string {
  return JSON.stringify([
    sample.reportSchemaVersion,
    sample.scenarioId,
    sample.fixtureId,
    sample.fixtureVersion,
    sample.fixtureRecipeHash,
    sample.fixtureContentHash,
    sample.fixtureInventory,
    sample.scenarioConfiguration,
    {
      coreBackend: sample.fixtureGenerator.coreBackend,
      node: sample.fixtureGenerator.node,
    },
    sample.topology,
    sample.operations,
    ...(sample.fixtureSource === undefined ? [] : [sample.fixtureSource]),
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

function validatePeakRss(
  arm: ComparisonArm,
  samples: readonly ComparisonSample[]
): void {
  if (
    samples.some(
      (sample) =>
        !Number.isFinite(sample.workerPeakRssBytes) ||
        sample.workerPeakRssBytes <= 0
    )
  )
    throw new Error(`${arm} peak RSS samples must be positive finite numbers`);
}

function measuredPeakRss(
  samples: readonly ComparisonSample[]
): readonly number[] {
  return samples
    .filter((sample) => sample.measured)
    .map((sample) => sample.workerPeakRssBytes);
}

function timingStatistics(measuredMilliseconds: readonly number[]) {
  return {
    medianMilliseconds: median(measuredMilliseconds),
    p90Milliseconds: percentile(measuredMilliseconds, 0.9),
    minimumMilliseconds: Math.min(...measuredMilliseconds),
    maximumMilliseconds: Math.max(...measuredMilliseconds),
    measuredMilliseconds,
  };
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
  validatePeakRss("baseline", input.baseline.samples);
  validatePeakRss("candidate", input.candidate.samples);
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
  const baselineTiming = timingStatistics(baselineMeasured);
  const candidateTiming = timingStatistics(candidateMeasured);
  const baselinePeakRss = measuredPeakRss(input.baseline.samples);
  const candidatePeakRss = measuredPeakRss(input.candidate.samples);
  const delta = percentageDelta(
    baselineTiming.medianMilliseconds,
    candidateTiming.medianMilliseconds
  );
  const informationalStatus: InformationalComparisonStatus =
    delta > input.informationalThresholdPercent
      ? "candidate-slower-than-threshold"
      : delta < -input.informationalThresholdPercent
        ? "candidate-faster-than-threshold"
        : "within-informational-threshold";
  const identity = allSamples[0];

  return {
    reportSchemaVersion: 3,
    scenarioId: identity.scenarioId,
    fixtureId: identity.fixtureId,
    fixtureVersion: identity.fixtureVersion,
    fixtureRecipeHash: identity.fixtureRecipeHash,
    fixtureContentHash,
    fixtureInventory: identity.fixtureInventory,
    scenarioConfiguration: identity.scenarioConfiguration,
    fixtureAuthoring: input.fixtureAuthoring,
    ...(identity.fixtureSource === undefined
      ? {}
      : { fixtureSource: identity.fixtureSource }),
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
      ...baselineTiming,
      medianPeakRssBytes: median(baselinePeakRss),
      measuredPeakRssBytes: baselinePeakRss,
    },
    candidate: {
      revision: input.candidate.revision,
      transformerProvenance: candidateTransformer,
      ...candidateTiming,
      medianPeakRssBytes: median(candidatePeakRss),
      measuredPeakRssBytes: candidatePeakRss,
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

function formatRevision(revision: string): string {
  return /^[a-f0-9]{12,}$/i.test(revision) ? revision.slice(0, 8) : revision;
}

function formatBytes(byteLength: number): string {
  const units = ["B", "KiB", "MiB", "GiB"];
  let value = byteLength;
  let unit = units[0];
  for (const candidate of units.slice(1)) {
    if (value < 1024) break;
    value /= 1024;
    unit = candidate;
  }
  return `${value.toFixed(unit === "B" ? 0 : 2)} ${unit}`;
}

function formatInventory(inventory: IModelInventory | undefined): string {
  if (inventory === undefined) return "Not available for this fixture topology";
  return [
    formatBytes(inventory.byteLength),
    `${inventory.schemaCount.toLocaleString("en-US")} schemas`,
    `${inventory.classCount.toLocaleString("en-US")} classes`,
    `${inventory.propertyCount.toLocaleString("en-US")} properties`,
    `${inventory.modelCount.toLocaleString("en-US")} models`,
    `${inventory.elementCount.toLocaleString("en-US")} elements`,
  ].join(" · ");
}

function markdownCode(value: string): string {
  const escaped = value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("|", "&#124;")
    .replaceAll(/\r?\n/g, " ");
  return `<code>${escaped}</code>`;
}

function configurationLabel(key: string): string {
  const words = key
    .replaceAll(/([a-z])([A-Z])/g, "$1 $2")
    .replaceAll(/[-_]/g, " ");
  return words.charAt(0).toUpperCase() + words.slice(1);
}

function currentStatusMeaning(summary: ComparisonSummary): string {
  const threshold = summary.policy.informationalThresholdPercent;
  switch (summary.informationalStatus) {
    case "candidate-slower-than-threshold":
      return `The candidate median is more than ${threshold}% slower. Inspect variance and repeat the run before calling this a regression.`;
    case "candidate-faster-than-threshold":
      return `The candidate median is more than ${threshold}% faster. Treat this as a promising signal and repeat if it will influence a decision.`;
    case "within-informational-threshold":
      return `The candidate median is within ±${threshold}% of baseline, so this run does not flag a material difference. This does not prove equal performance.`;
  }
}

function markdown(summary: ComparisonSummary): string {
  const signedDelta = `${summary.percentageDelta >= 0 ? "+" : ""}${summary.percentageDelta.toFixed(2)}%`;
  const configuration = Object.entries(summary.scenarioConfiguration ?? {});
  const configurationHeaders = configuration.map(([key]) =>
    configurationLabel(key)
  );
  const configurationValues = configuration.map(([, value]) =>
    markdownCode(value)
  );
  const threshold = summary.policy.informationalThresholdPercent;
  const baselineCore =
    summary.baseline.transformerProvenance.coreBackendVersion;
  const candidateCore =
    summary.candidate.transformerProvenance.coreBackendVersion;
  const coreBackendSummary =
    baselineCore === candidateCore
      ? `Core backend: both arms use ${markdownCode(baselineCore)}.`
      : `Core backend: baseline ${markdownCode(baselineCore)}, candidate ${markdownCode(candidateCore)}.`;
  return [
    "# Quick performance A/B comparison",
    "",
    `> Informational only. This report flags median differences outside ±${threshold}%; it does not establish statistical confidence or block merging.`,
    "",
    "## Run configuration",
    "",
    `| Scenario | ${configurationHeaders.length > 0 ? `${configurationHeaders.join(" | ")} | ` : ""}Fixture | Source iModel scale | Samples |`,
    `| --- | ${configurationHeaders.map(() => "--- | ").join("")}--- | --- | ---: |`,
    `| ${markdownCode(summary.scenarioId)} | ${configurationValues.length > 0 ? `${configurationValues.join(" | ")} | ` : ""}${markdownCode(summary.fixtureId)} v${summary.fixtureVersion} | ${formatInventory(summary.fixtureInventory)} | ${summary.policy.measuredSamplesPerArm} + ${summary.policy.warmupsPerArm} warm-up/arm |`,
    "",
    `Execution order: ${summary.policy.ordering} baseline/candidate samples in isolated processes.`,
    `Prepared target: baseline ${markdownCode(formatRevision(summary.fixtureAuthoring.revision))} with transformer ${markdownCode(summary.fixtureAuthoring.transformerVersion)}.`,
    "",
    "## Result",
    "",
    coreBackendSummary,
    "",
    "| Arm | Revision | Transformer | Median | P90 | Range | Peak worker RSS |",
    "| --- | --- | --- | ---: | ---: | ---: | ---: |",
    `| Baseline | ${markdownCode(formatRevision(summary.baseline.revision))} | ${markdownCode(summary.baseline.transformerProvenance.version)} | ${formatMilliseconds(summary.baseline.medianMilliseconds)} | ${formatMilliseconds(summary.baseline.p90Milliseconds)} | ${formatMilliseconds(summary.baseline.minimumMilliseconds)}–${formatMilliseconds(summary.baseline.maximumMilliseconds)} | ${formatBytes(summary.baseline.medianPeakRssBytes)} |`,
    `| Candidate | ${markdownCode(formatRevision(summary.candidate.revision))} | ${markdownCode(summary.candidate.transformerProvenance.version)} | ${formatMilliseconds(summary.candidate.medianMilliseconds)} | ${formatMilliseconds(summary.candidate.p90Milliseconds)} | ${formatMilliseconds(summary.candidate.minimumMilliseconds)}–${formatMilliseconds(summary.candidate.maximumMilliseconds)} | ${formatBytes(summary.candidate.medianPeakRssBytes)} |`,
    "",
    "Peak worker RSS is reported by the isolated worker's process resource usage across its complete lifetime, including setup and teardown.",
    "",
    `**Candidate delta:** ${signedDelta}  `,
    `**Status:** \`${summary.informationalStatus}\``,
    "",
    "<details>",
    `<summary>How to interpret <code>${summary.informationalStatus}</code></summary>`,
    "",
    currentStatusMeaning(summary),
    "",
    "| Status | Meaning |",
    "| --- | --- |",
    `| \`candidate-slower-than-threshold\` | Candidate median is more than ${threshold}% slower. Investigate and repeat before treating it as a regression. |`,
    `| \`within-informational-threshold\` | Difference is within ±${threshold}%. No material difference is flagged; this is not proof of equivalence. |`,
    `| \`candidate-faster-than-threshold\` | Candidate median is more than ${threshold}% faster. Repeat if the improvement will influence a decision. |`,
    "",
    "The threshold is an investigation trigger, not a confidence interval or pass/fail gate.",
    "",
    "</details>",
    "",
    "<details>",
    "<summary>Where are the individual measurements?</summary>",
    "",
    `The artifact retains all ${summary.policy.measuredSamplesPerArm} measured timings per arm in \`comparison-samples.jsonl\`, plus each isolated execution record under \`executions/\`. They are intentionally omitted from this summary.`,
    "",
    "</details>",
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
