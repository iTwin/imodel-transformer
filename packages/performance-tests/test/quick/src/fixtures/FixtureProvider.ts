/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
import { BriefcaseDb } from "@itwin/core-backend";
import { ChangesetFileProps } from "@itwin/core-common";
import { FixtureArtifact, FixtureArtifactManifest } from "./FixtureArtifact.js";
import { FixtureDescriptor } from "./FixtureDescriptor.js";
import { ReconstructedHub } from "./LocalHubFixture.js";
import { detachedBriefcaseFixtureProvider } from "./providers/detachedBriefcaseProvider.js";
import { liveHubFixtureProvider } from "./providers/liveHubProvider.js";

interface PreparedDatasetBase {
  readonly descriptor: FixtureDescriptor;
  /** Stage-2 cost for this sample: what it took to hand the scenario a pristine working copy. */
  readonly reconstructionMilliseconds: number;
}

/** A live local test hub with an open source briefcase and an already-transformed-into target. */
export interface PreparedLiveHubDataset extends PreparedDatasetBase {
  readonly topology: "source-and-empty-target";
  readonly hub: ReconstructedHub;
}

/** A working copy of a stage-1 artifact: an open readonly source and its changeset files. */
export interface PreparedDetachedDataset extends PreparedDatasetBase {
  readonly topology: "source-only";
  /** Root of this sample's working copy. */
  readonly directory: string;
  readonly sourceDb: BriefcaseDb;
  /** Changeset props whose `pathname` values are absolute and point into `directory`. */
  readonly csFileProps: ChangesetFileProps[];
  readonly manifest: FixtureArtifactManifest;
  /**
   * Whatever the recipe returned from `applySourceChangesets`, or `undefined` if it returned
   * nothing. Deliberately `unknown`: the framework never interprets it, and it is byte-identical
   * across samples and across A/B arms because it is captured once at stage 1.
   */
  readonly recipe?: unknown;
}

export type PreparedDataset = PreparedLiveHubDataset | PreparedDetachedDataset;

export function requireLiveHubDataset(
  dataset: PreparedDataset
): PreparedLiveHubDataset {
  if (dataset.topology !== "source-and-empty-target")
    throw new Error(
      `Scenario requires a "source-and-empty-target" fixture but received "${dataset.topology}"`
    );
  return dataset;
}

export function requireDetachedDataset(
  dataset: PreparedDataset
): PreparedDetachedDataset {
  if (dataset.topology !== "source-only")
    throw new Error(
      `Scenario requires a "source-only" fixture but received "${dataset.topology}"`
    );
  return dataset;
}

export function fixtureWorkingDirectory(
  root: string,
  sampleName: string
): string {
  return path.join(root, sampleName);
}

/**
 * The result of stage 1.
 *
 * `artifact` is present only for topologies that can be captured as relocatable bytes. A
 * live-hub topology cannot: its measured region re-enters the hub, so there is nothing to
 * capture and stage 1 is structural only.
 */
export interface BuiltFixture {
  readonly descriptor: FixtureDescriptor;
  readonly directory: string;
  readonly buildMilliseconds: number;
  readonly artifact?: FixtureArtifact;
}

export function requireFixtureArtifact(built: BuiltFixture): FixtureArtifact {
  if (!built.artifact)
    throw new Error(
      `Fixture "${built.descriptor.id}" did not produce an on-disk artifact`
    );
  return built.artifact;
}

/**
 * Two-stage fixture lifecycle.
 *
 * Stage 1 ({@link FixtureProvider.build}) runs once per benchmark run. Nothing measures against
 * its output directly. Stage 2 ({@link FixtureProvider.materialize}) hands each sample its own
 * pristine working copy, so a scenario may mutate what it is given without the framework caring.
 * The default is always a fresh copy per sample; the split only moves the expensive part out of
 * the loop.
 */
export interface FixtureProvider {
  /** Build once, outside the sample loop. */
  build(
    descriptor: FixtureDescriptor,
    artifactDir: string
  ): Promise<BuiltFixture>;
  /** Produce a fresh working copy for one sample. */
  materialize(
    built: BuiltFixture,
    sampleDir: string,
    sampleName: string
  ): Promise<PreparedDataset>;
  /** Release one sample's working copy. */
  disposeSample(dataset: PreparedDataset): Promise<void>;
  /** Release stage-1 state, after every sample has been disposed. */
  disposeBuild(built: BuiltFixture): Promise<void>;
}

export function getFixtureProvider(
  descriptor: FixtureDescriptor
): FixtureProvider {
  switch (descriptor.layout.topology) {
    case "source-and-empty-target":
      return liveHubFixtureProvider;
    case "source-only":
      return detachedBriefcaseFixtureProvider;
    default: {
      const unreachable: never = descriptor.layout.topology;
      throw new Error(`Unknown fixture topology: ${String(unreachable)}`);
    }
  }
}
