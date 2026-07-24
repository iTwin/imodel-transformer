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

export interface DatasetDescriptor {
  readonly id: string;
  readonly version: number;
  readonly label: string;
  readonly scenarioClaims: readonly string[];
  readonly layout: {
    readonly kind: "reconstructed";
    readonly recipe: "balanced-incremental";
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
