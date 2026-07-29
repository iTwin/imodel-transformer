/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { DatasetDescriptor } from "./DatasetDescriptor.js";
import { FixtureArtifact } from "./FixtureArtifact.js";
import { PreparedDataset } from "./FixtureMaterializer.js";
import { detachedBriefcaseFixtureProvider } from "./providers/detachedBriefcaseProvider.js";
import { liveHubFixtureProvider } from "./providers/liveHubProvider.js";

/**
 * The result of stage 1.
 *
 * `artifact` is present only for topologies that can be captured as relocatable bytes. A
 * live-hub topology cannot: its measured region re-enters the hub, so there is nothing to
 * capture and stage 1 is structural only.
 */
export interface BuiltFixture {
  readonly descriptor: DatasetDescriptor;
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
    descriptor: DatasetDescriptor,
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
  descriptor: DatasetDescriptor
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
