/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { createHash } from "crypto";
import { DatasetDescriptor } from "./DatasetDescriptor";

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

export function validateDescriptor(value: unknown): DatasetDescriptor {
  if (value === null || typeof value !== "object")
    throw new Error("Fixture descriptor must be an object");
  const descriptor = value as Partial<DatasetDescriptor>;
  if (
    typeof descriptor.id !== "string" ||
    typeof descriptor.version !== "number" ||
    typeof descriptor.label !== "string" ||
    !Array.isArray(descriptor.scenarioClaims) ||
    descriptor.layout?.kind !== "reconstructed" ||
    descriptor.layout.recipe !== "balanced-incremental" ||
    typeof descriptor.layout.seed !== "number" ||
    descriptor.distribution === undefined ||
    typeof descriptor.generator?.coreBackend !== "string" ||
    typeof descriptor.generator.node !== "string" ||
    typeof descriptor.generator.transformer !== "string" ||
    typeof descriptor.recipeHash !== "string"
  )
    throw new Error("Fixture descriptor has an invalid shape");
  return descriptor as DatasetDescriptor;
}
