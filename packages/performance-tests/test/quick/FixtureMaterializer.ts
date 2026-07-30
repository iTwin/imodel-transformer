/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
import { BriefcaseDb } from "@itwin/core-backend";
import { ChangesetFileProps } from "@itwin/core-common";
import { DatasetDescriptor } from "./DatasetDescriptor.js";
import { FixtureArtifactManifest } from "./FixtureArtifact.js";
import { ReconstructedHub } from "./LocalHubFixture.js";

interface PreparedDatasetBase {
  readonly descriptor: DatasetDescriptor;
  /** Stage-2 cost for this sample: what it took to hand the scenario a pristine working copy. */
  readonly reconstructionMilliseconds: number;
}

/** A live HubMock with an open source briefcase and an already-transformed-into target. */
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
