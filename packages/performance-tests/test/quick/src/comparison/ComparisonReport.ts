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
  /**
   * Scenario this arm was configured to run. When the two arms declare different scenarios, the
   * report treats the run as an intentional scenario A/B over one shared fixture and build instead
   * of rejecting the scenario mismatch.
   */
  readonly scenarioId?: string;
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
  readonly reportSchemaVersion: 4;
  /** Baseline arm's scenario; identical to the candidate's except in a scenario A/B comparison. */
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
    readonly scenarioId: string;
    readonly scenarioConfiguration?: ScenarioConfiguration;
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
    readonly scenarioId: string;
    readonly scenarioConfiguration?: ScenarioConfiguration;
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

function configurationIdentity(
  sample: ComparisonSample,
  includeScenario: boolean
): string {
  return JSON.stringify([
    sample.reportSchemaVersion,
    ...(includeScenario
      ? [sample.scenarioId, sample.scenarioConfiguration]
      : []),
    sample.fixtureId,
    sample.fixtureVersion,
    sample.fixtureRecipeHash,
    sample.fixtureContentHash,
    sample.fixtureInventory,
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

interface ArmScenario {
  readonly id: string;
  readonly configuration?: ScenarioConfiguration;
}

function armScenario(
  arm: ComparisonArm,
  result: ComparisonArmResult
): ArmScenario {
  const scenarioIds = new Set(
    result.samples.map((sample) => sample.scenarioId)
  );
  const id = [...scenarioIds][0];
  if (scenarioIds.size !== 1 || id === undefined)
    throw new Error(`${arm} samples must run one scenario`);
  const configurations = new Set(
    result.samples.map((sample) =>
      JSON.stringify(sample.scenarioConfiguration ?? null)
    )
  );
  if (configurations.size !== 1)
    throw new Error(`${arm} samples must use one scenario configuration`);
  if (result.scenarioId !== undefined && result.scenarioId !== id)
    throw new Error(
      `${arm} samples ran scenario "${id}" instead of the declared "${result.scenarioId}"`
    );
  return { id, configuration: result.samples[0].scenarioConfiguration };
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
  const baselineScenario = armScenario("baseline", input.baseline);
  const candidateScenario = armScenario("candidate", input.candidate);
  const scenarioComparison =
    input.baseline.scenarioId !== undefined &&
    input.candidate.scenarioId !== undefined &&
    input.baseline.scenarioId !== input.candidate.scenarioId;
  if (
    scenarioComparison &&
    baselineTransformer.contentHash !== candidateTransformer.contentHash
  )
    throw new Error(
      "A scenario A/B comparison requires both arms to use the same transformer build"
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
  if (
    new Set(
      allSamples.map((sample) =>
        configurationIdentity(sample, !scenarioComparison)
      )
    ).size !== 1
  )
    throw new Error(
      scenarioComparison
        ? "A scenario A/B comparison requires the identical configured fixture"
        : "Baseline and candidate must use the identical scenario and configured fixture"
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
    reportSchemaVersion: 4,
    scenarioId: baselineScenario.id,
    fixtureId: identity.fixtureId,
    fixtureVersion: identity.fixtureVersion,
    fixtureRecipeHash: identity.fixtureRecipeHash,
    fixtureContentHash,
    fixtureInventory: identity.fixtureInventory,
    scenarioConfiguration: baselineScenario.configuration,
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
      scenarioId: baselineScenario.id,
      ...(baselineScenario.configuration === undefined
        ? {}
        : { scenarioConfiguration: baselineScenario.configuration }),
      transformerProvenance: baselineTransformer,
      ...baselineTiming,
      medianPeakRssBytes: median(baselinePeakRss),
      measuredPeakRssBytes: baselinePeakRss,
    },
    candidate: {
      revision: input.candidate.revision,
      scenarioId: candidateScenario.id,
      ...(candidateScenario.configuration === undefined
        ? {}
        : { scenarioConfiguration: candidateScenario.configuration }),
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

function relativePerformance(summary: ComparisonSummary): string {
  const baseline = summary.baseline.medianMilliseconds;
  const candidate = summary.candidate.medianMilliseconds;
  if (candidate < baseline)
    return `Candidate is ${(baseline / candidate).toFixed(2)}× faster than baseline.`;
  if (candidate > baseline)
    return `Candidate is ${(candidate / baseline).toFixed(2)}× slower than baseline.`;
  return "Candidate and baseline have equal median duration.";
}

function armConfigurationCell(
  configuration: ScenarioConfiguration | undefined
): string {
  const entries = Object.entries(configuration ?? {});
  if (entries.length === 0) return "—";
  return entries
    .map(([key, value]) => `${configurationLabel(key)}: ${markdownCode(value)}`)
    .join(", ");
}

function runConfigurationRows(summary: ComparisonSummary): string[] {
  const fixtureCell = `${markdownCode(summary.fixtureId)} v${summary.fixtureVersion}`;
  const scaleCell = formatInventory(summary.fixtureInventory);
  const samplesCell = `${summary.policy.measuredSamplesPerArm} + ${summary.policy.warmupsPerArm} warm-up/arm`;
  if (summary.baseline.scenarioId !== summary.candidate.scenarioId)
    return [
      "| Arm | Scenario | Configuration | Fixture | Source iModel scale | Samples |",
      "| --- | --- | --- | --- | --- | ---: |",
      `| Baseline | ${markdownCode(summary.baseline.scenarioId)} | ${armConfigurationCell(summary.baseline.scenarioConfiguration)} | ${fixtureCell} | ${scaleCell} | ${samplesCell} |`,
      `| Candidate | ${markdownCode(summary.candidate.scenarioId)} | ${armConfigurationCell(summary.candidate.scenarioConfiguration)} | ${fixtureCell} | ${scaleCell} | ${samplesCell} |`,
      "",
      "Both arms use one transformer build and one immutable fixture; the candidate delta measures the scenario difference.",
    ];
  const configuration = Object.entries(summary.scenarioConfiguration ?? {});
  const configurationHeaders = configuration.map(([key]) =>
    configurationLabel(key)
  );
  const configurationValues = configuration.map(([, value]) =>
    markdownCode(value)
  );
  return [
    `| Scenario | ${configurationHeaders.length > 0 ? `${configurationHeaders.join(" | ")} | ` : ""}Fixture | Source iModel scale | Samples |`,
    `| --- | ${configurationHeaders.map(() => "--- | ").join("")}--- | --- | ---: |`,
    `| ${markdownCode(summary.scenarioId)} | ${configurationValues.length > 0 ? `${configurationValues.join(" | ")} | ` : ""}${fixtureCell} | ${scaleCell} | ${samplesCell} |`,
  ];
}

function markdown(summary: ComparisonSummary): string {
  const signedDelta = `${summary.percentageDelta >= 0 ? "+" : ""}${summary.percentageDelta.toFixed(2)}%`;
  const scenarioComparison =
    summary.baseline.scenarioId !== summary.candidate.scenarioId;
  const scenarioColumn = (scenarioId: string) =>
    scenarioComparison ? ` ${markdownCode(scenarioId)} |` : "";
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
    ...runConfigurationRows(summary),
    "",
    `Execution order: ${summary.policy.ordering} baseline/candidate samples in isolated processes.`,
    `Prepared target: baseline ${markdownCode(formatRevision(summary.fixtureAuthoring.revision))} with transformer ${markdownCode(summary.fixtureAuthoring.transformerVersion)}.`,
    "",
    "## Result",
    "",
    coreBackendSummary,
    "",
    `| Arm |${scenarioComparison ? " Scenario |" : ""} Revision | Transformer | Median | P90 | Range | Peak worker RSS |`,
    `| --- |${scenarioComparison ? " --- |" : ""} --- | --- | ---: | ---: | ---: | ---: |`,
    `| Baseline |${scenarioColumn(summary.baseline.scenarioId)} ${markdownCode(formatRevision(summary.baseline.revision))} | ${markdownCode(summary.baseline.transformerProvenance.version)} | ${formatMilliseconds(summary.baseline.medianMilliseconds)} | ${formatMilliseconds(summary.baseline.p90Milliseconds)} | ${formatMilliseconds(summary.baseline.minimumMilliseconds)}–${formatMilliseconds(summary.baseline.maximumMilliseconds)} | ${formatBytes(summary.baseline.medianPeakRssBytes)} |`,
    `| Candidate |${scenarioColumn(summary.candidate.scenarioId)} ${markdownCode(formatRevision(summary.candidate.revision))} | ${markdownCode(summary.candidate.transformerProvenance.version)} | ${formatMilliseconds(summary.candidate.medianMilliseconds)} | ${formatMilliseconds(summary.candidate.p90Milliseconds)} | ${formatMilliseconds(summary.candidate.minimumMilliseconds)}–${formatMilliseconds(summary.candidate.maximumMilliseconds)} | ${formatBytes(summary.candidate.medianPeakRssBytes)} |`,
    "",
    "Peak worker RSS is reported by the isolated worker's process resource usage across its complete lifetime, including setup and teardown.",
    "",
    `**Candidate delta:** ${signedDelta}  `,
    `**Relative performance:** ${relativePerformance(summary)}  `,
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
