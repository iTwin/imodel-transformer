/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import {
  BriefcaseDb,
  ChangeInstance,
  ChangesetReader,
  PartialChangeUnifier,
  PropertyFilter,
} from "@itwin/core-backend";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  readChangesetFileProps,
  readFixtureArtifact,
  readFixtureRecipeData,
} from "../fixtures/FixtureArtifact.js";

export const comparisonFixtureIdentityVersion = 2;
export const comparisonFixtureIdentityFileName = "comparison-identity.json";

export interface ComparisonFixtureIdentity {
  readonly artifactVersion: number;
  readonly contentDigest: string;
  readonly baseSemanticDigest: string;
  readonly changesetSemanticDigest: string;
}

function assertSha256(value: string, label: string): void {
  if (!/^[a-f0-9]{64}$/i.test(value))
    throw new Error(`${label} must be a SHA-256 hex digest`);
}

function normalizeChangesetValue(
  value: unknown,
  propertyName?: string
): unknown {
  const normalizedPropertyName = propertyName?.toLowerCase();
  if (normalizedPropertyName === "lastmod" && typeof value === "string")
    return { volatileTimestampBytes: Buffer.byteLength(value) };
  if (
    (normalizedPropertyName === "federationguid" ||
      normalizedPropertyName === "geometryguid") &&
    typeof value === "string"
  )
    return { generatedGuidBytes: Buffer.byteLength(value) };
  if (normalizedPropertyName === "changeindexes" && Array.isArray(value))
    return { count: value.length };
  if (
    (normalizedPropertyName === "tables" ||
      normalizedPropertyName === "changefetchedpropnames") &&
    Array.isArray(value)
  )
    return [...value].sort();
  if (normalizedPropertyName === "isindirectchange")
    return { representedByRowShapes: true };
  if (value instanceof Uint8Array)
    return { base64: Buffer.from(value).toString("base64") };
  if (typeof value === "bigint") return { bigint: value.toString() };
  if (Array.isArray(value))
    return value.map((entry) => normalizeChangesetValue(entry));
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
        key,
        normalizeChangesetValue(entry, key),
      ])
    );
  return value;
}

function normalizeChangeInstance(
  instance: ChangeInstance | undefined
): unknown {
  return instance === undefined ? undefined : normalizeChangesetValue(instance);
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value))
    return `[${value.map((entry) => canonicalJson(entry)).join(",")}]`;
  return `{${Object.entries(value as Record<string, unknown>)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
    .join(",")}}`;
}

export function changesetSemanticDigest(
  directory: string,
  db: BriefcaseDb
): string {
  const changesets = readChangesetFileProps(directory).map((changeset) => {
    const reader = ChangesetReader.openFile({
      fileName: changeset.pathname,
      db,
      propFilter: PropertyFilter.BisCoreElement,
      rowOptions: { abbreviateBlobs: false },
    });
    const unifier = new PartialChangeUnifier();
    const rowShapes = new Map<string, number>();
    try {
      while (reader.step()) {
        const shape = canonicalJson({
          ecTable: reader.isECTable,
          indirect: reader.isIndirectChange,
          operation: reader.op,
          table: reader.tableName,
        });
        rowShapes.set(shape, (rowShapes.get(shape) ?? 0) + 1);
        unifier.appendFrom(reader);
      }
      const instances = [...unifier.instances]
        .map(normalizeChangeInstance)
        .sort((left, right) =>
          canonicalJson(left).localeCompare(canonicalJson(right))
        );
      return {
        index: changeset.index,
        instances,
        rowShapes: [...rowShapes.entries()].sort(([left], [right]) =>
          left.localeCompare(right)
        ),
      };
    } finally {
      try {
        unifier[Symbol.dispose]();
      } finally {
        reader[Symbol.dispose]();
      }
    }
  });
  return canonicalSha256(changesets);
}

export function createComparisonFixtureIdentity(
  directory: string,
  db: BriefcaseDb,
  baseSemanticDigest: string
): ComparisonFixtureIdentity {
  assertSha256(baseSemanticDigest, "Fixture base semantic digest");
  const artifact = readFixtureArtifact(directory);
  const changesetDigest = changesetSemanticDigest(directory, db);
  const changesets = readChangesetFileProps(directory).map((changeset) => ({
    index: changeset.index,
  }));
  const contentDigest = canonicalSha256({
    artifactVersion: artifact.manifest.artifactVersion,
    baseSemanticDigest,
    changesetSemanticDigest: changesetDigest,
    briefcase: {
      briefcaseId: artifact.manifest.briefcase.briefcaseId,
      changesetIndex: artifact.manifest.briefcase.changeset.index,
    },
    changesets: {
      baseChangesetIndex: artifact.manifest.changesets.baseChangesetIndex,
      firstIndex: artifact.manifest.changesets.firstIndex,
      lastIndex: artifact.manifest.changesets.lastIndex,
      values: changesets,
    },
    descriptor: artifact.manifest.descriptor,
    recipeData: readFixtureRecipeData(directory, artifact.manifest),
  });
  return {
    artifactVersion: comparisonFixtureIdentityVersion,
    contentDigest,
    baseSemanticDigest,
    changesetSemanticDigest: changesetDigest,
  };
}

export function writeComparisonFixtureIdentity(
  directory: string,
  identity: ComparisonFixtureIdentity
): void {
  validateComparisonFixtureIdentity(identity);
  fs.writeFileSync(
    path.join(directory, comparisonFixtureIdentityFileName),
    `${JSON.stringify(identity, undefined, 2)}\n`
  );
}

export function validateComparisonFixtureIdentity(
  value: unknown
): ComparisonFixtureIdentity {
  if (!value || typeof value !== "object")
    throw new Error("Comparison fixture identity must be an object");
  const identity = value as Partial<ComparisonFixtureIdentity>;
  if (identity.artifactVersion !== comparisonFixtureIdentityVersion)
    throw new Error(
      `Unsupported comparison fixture identity version ${String(
        identity.artifactVersion
      )}; expected ${comparisonFixtureIdentityVersion}`
    );
  assertSha256(identity.contentDigest as string, "Fixture content digest");
  assertSha256(
    identity.baseSemanticDigest as string,
    "Fixture base semantic digest"
  );
  assertSha256(
    identity.changesetSemanticDigest as string,
    "Fixture changeset semantic digest"
  );
  return identity as ComparisonFixtureIdentity;
}

export function readComparisonFixtureIdentity(
  directory: string
): ComparisonFixtureIdentity {
  const identityFile = path.join(directory, comparisonFixtureIdentityFileName);
  if (!fs.existsSync(identityFile))
    throw new Error(
      `Fixture artifact is missing ${comparisonFixtureIdentityFileName}; rebuild it with the current comparison harness`
    );
  return validateComparisonFixtureIdentity(
    JSON.parse(fs.readFileSync(identityFile, "utf8"))
  );
}
