/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { createHash } from "node:crypto";

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

export interface FixtureDescriptor {
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

function canonicalize(value: unknown): string {
  if (Array.isArray(value))
    return `[${value.map((entry) => canonicalize(entry)).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalize(record[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

export function canonicalSha256(value: unknown): string {
  return createHash("sha256").update(canonicalize(value)).digest("hex");
}

export function validateFixtureDescriptor(value: unknown): FixtureDescriptor {
  if (value === null || typeof value !== "object")
    throw new Error("Fixture descriptor must be an object");
  const descriptor = value as Partial<FixtureDescriptor>;
  if (
    typeof descriptor.id !== "string" ||
    typeof descriptor.version !== "number" ||
    typeof descriptor.label !== "string" ||
    !Array.isArray(descriptor.scenarioClaims) ||
    descriptor.layout?.kind !== "reconstructed" ||
    !fixtureTopologies.includes(descriptor.layout.topology) ||
    typeof descriptor.layout.recipe !== "string" ||
    descriptor.layout.recipe.length === 0 ||
    typeof descriptor.layout.seed !== "number" ||
    descriptor.distribution === undefined ||
    typeof descriptor.generator?.coreBackend !== "string" ||
    typeof descriptor.generator.node !== "string" ||
    typeof descriptor.generator.transformer !== "string" ||
    typeof descriptor.recipeHash !== "string"
  )
    throw new Error("Fixture descriptor has an invalid shape");
  return descriptor as FixtureDescriptor;
}
