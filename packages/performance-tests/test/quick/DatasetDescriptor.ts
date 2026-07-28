/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

export interface FixtureOperationCounts {
  readonly aspects: Readonly<Record<"deletes" | "inserts" | "updates", number>>;
  readonly elements: Readonly<
    Record<"deletes" | "inserts" | "updates", number>
  >;
  readonly relationships: Readonly<
    Record<"deletes" | "inserts" | "updates", number>
  >;
  readonly geometryUpdates: number;
  readonly sourceChangesets: number;
}

export interface FixtureDistribution {
  readonly base: {
    readonly aspects: number;
    readonly elements: number;
    readonly geometricElements: number;
    readonly relationships: number;
  };
  readonly operations: FixtureOperationCounts;
}

/**
 * The *shape* of a fixture, independent of the change mix a recipe applies to it.
 *
 * - `source-and-empty-target`: a source briefcase plus an empty target that has been transformed
 *   into. Requires a live hub at measure time, so its working copy is a full per-sample rebuild.
 * - `source-only`: a source briefcase and its pushed changeset files, with no target and no hub at
 *   measure time. Built once into an immutable artifact and copied per sample.
 */
export type FixtureTopology = "source-and-empty-target" | "source-only";

export const fixtureTopologies: readonly FixtureTopology[] = [
  "source-and-empty-target",
  "source-only",
];

export interface DatasetDescriptor {
  readonly id: string;
  readonly version: number;
  readonly label: string;
  readonly scenarioClaims: readonly string[];
  readonly layout: {
    readonly kind: "reconstructed";
    readonly topology: FixtureTopology;
    readonly recipe: string;
    readonly seed: number;
  };
  readonly distribution: FixtureDistribution;
  readonly generator: {
    readonly coreBackend: string;
    readonly node: string;
    readonly transformer: string;
  };
  readonly recipeHash: string;
}
