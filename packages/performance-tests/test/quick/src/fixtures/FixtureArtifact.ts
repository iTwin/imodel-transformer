/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { ChangesetFileProps } from "@itwin/core-common";
import {
  canonicalSha256,
  FixtureDescriptor,
  validateFixtureDescriptor,
} from "./FixtureDescriptor.js";
import { IModelInventory, isIModelInventory } from "./IModelInventory.js";

/**
 * Version of the on-disk artifact layout. Bump when the directory contract changes in a way that
 * an older reader cannot interpret.
 */
export const fixtureArtifactVersion = 2;

export const artifactBriefcaseFileName = "briefcase.bim";
export const artifactChangesetDirectoryName = "changesets";
export const artifactChangesetPropsFileName = "csFileProps.json";
export const artifactManifestFileName = "manifest.json";
export const artifactRecipeDataFileName = "recipe.json";
export const artifactSourceSeedFileName = "source-seed.bim";
export const artifactTargetBriefcaseFileName = "target-briefcase.bim";
export const artifactTargetChangesetDirectoryName = "target-changesets";
export const artifactTargetChangesetPropsFileName = "targetCsFileProps.json";
export const artifactTargetSeedFileName = "target-seed.bim";

export interface FixtureArtifactBriefcaseManifest {
  readonly fileName: string;
  readonly briefcaseId: number;
  readonly changeset: { readonly id: string; readonly index?: number };
  readonly byteLength: number;
}

export interface FixtureArtifactChangesetManifest {
  readonly directory: string;
  readonly propsFile: string;
  readonly count: number;
  readonly baseChangesetIndex: number;
  readonly firstIndex?: number;
  readonly lastIndex?: number;
}

export interface LiveHubFixtureArtifactManifest {
  readonly iTwinId: string;
  readonly source: {
    readonly iModelId: string;
    readonly iModelName: string;
    readonly version0File: string;
  };
  readonly target: {
    readonly briefcase: FixtureArtifactBriefcaseManifest;
    readonly changesets: FixtureArtifactChangesetManifest;
    readonly iModelId: string;
    readonly iModelName: string;
    readonly version0File: string;
  };
}

export interface FixtureArtifactManifest {
  readonly artifactVersion: number;
  /** SHA-256 identity of every immutable workload file in the artifact. */
  readonly contentHash: string;
  readonly descriptor: FixtureDescriptor;
  /** Scale derived from the captured source iModel, outside the measured region. */
  readonly iModelInventory?: IModelInventory;
  readonly briefcase: FixtureArtifactBriefcaseManifest;
  readonly changesets: FixtureArtifactChangesetManifest;
  /** Present when the artifact restores a live source and target hub for incremental sync. */
  readonly liveHub?: LiveHubFixtureArtifactManifest;
  /**
   * Present only when the recipe returned data to carry across the stage boundary. Absent means
   * the recipe emitted nothing — stage 2 reads this key rather than probing the filesystem.
   */
  readonly recipeDataFile?: string;
  readonly buildMilliseconds: number;
  readonly builtAt: string;
}

/** A stage-1 fixture artifact: immutable bytes that stage 2 copies from. */
export interface FixtureArtifact {
  readonly directory: string;
  readonly manifest: FixtureArtifactManifest;
}

function artifactContentFiles(directory: string, relative = ""): string[] {
  return fs
    .readdirSync(path.join(directory, relative), { withFileTypes: true })
    .flatMap((entry) => {
      const entryRelative = path.join(relative, entry.name);
      if (entryRelative === artifactManifestFileName) return [];
      return entry.isDirectory()
        ? artifactContentFiles(directory, entryRelative)
        : [entryRelative];
    });
}

/** Hash the relative path, length, and bytes of every immutable workload file. */
export function fixtureArtifactContentHash(directory: string): string {
  const entries = artifactContentFiles(directory)
    .map((relative) => {
      const contents = fs.readFileSync(path.join(directory, relative));
      return {
        path: relative.replaceAll(path.sep, "/"),
        byteLength: contents.byteLength,
        sha256: createHash("sha256").update(contents).digest("hex"),
      };
    })
    .sort((left, right) => left.path.localeCompare(right.path));
  return canonicalSha256(entries);
}

function describeValue(value: unknown): string {
  if (value === null) return "null";
  if (typeof value !== "object") return typeof value;
  const name = value.constructor?.name;
  return name && name !== "Object" ? name : "object";
}

/**
 * Reject anything that would not survive `JSON.stringify`/`parse` intact.
 *
 * A plain round-trip comparison is not enough: `JSON.stringify(new Set())` yields `{}`, and both
 * sides then have zero enumerable keys, so a lossy `Set` would compare equal. Walking the original
 * and requiring JSON-native values at every node catches that, plus `Map`, `Date`, `BigInt`, `NaN`,
 * `undefined`, functions and class instances — and names the path that is wrong.
 */
function assertJsonNative(value: unknown, at: string, seen: Set<object>): void {
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return;
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new Error(
        `Recipe data at ${at} is ${String(
          value
        )}, which JSON serializes as null`
      );
    return;
  }
  if (typeof value !== "object")
    throw new Error(
      `Recipe data at ${at} is a ${describeValue(
        value
      )}, which JSON cannot represent`
    );
  if (seen.has(value)) throw new Error(`Recipe data at ${at} is circular`);
  seen.add(value);
  if (Array.isArray(value)) {
    value.forEach((entry, index) =>
      assertJsonNative(entry, `${at}[${index}]`, seen)
    );
  } else {
    const prototype = Object.getPrototypeOf(value) as object | null;
    if (prototype !== Object.prototype && prototype !== null)
      throw new Error(
        `Recipe data at ${at} is a ${describeValue(
          value
        )}; only plain objects and arrays survive JSON`
      );
    for (const [key, entry] of Object.entries(value))
      assertJsonNative(entry, `${at}.${key}`, seen);
  }
  seen.delete(value);
}

/**
 * Serialize a recipe's returned data into the artifact, failing at build time rather than
 * producing a silently lossy artifact a scenario would misread much later.
 */
export function writeFixtureRecipeData(
  directory: string,
  data: unknown
): string {
  assertJsonNative(data, "<root>", new Set());
  fs.writeFileSync(
    path.join(directory, artifactRecipeDataFileName),
    `${JSON.stringify(data, undefined, 2)}\n`
  );
  return artifactRecipeDataFileName;
}

/** Read recipe data from an artifact or working copy; `undefined` when the recipe emitted none. */
export function readFixtureRecipeData(
  directory: string,
  manifest: FixtureArtifactManifest
): unknown {
  if (manifest.recipeDataFile === undefined) return undefined;
  const file = path.join(directory, manifest.recipeDataFile);
  if (!fs.existsSync(file))
    throw new Error(
      `Fixture artifact manifest declares recipe data at ${manifest.recipeDataFile} but the file is missing`
    );
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

export function changesetArtifactFileName(
  changeset: Pick<ChangesetFileProps, "id" | "index">
): string {
  return `${String(changeset.index).padStart(6, "0")}-${changeset.id}.cs`;
}

/** Rewrite `pathname` to a POSIX path relative to the artifact root. */
export function toRelativeChangesetProps(
  changesets: readonly ChangesetFileProps[],
  directory = artifactChangesetDirectoryName
): ChangesetFileProps[] {
  return changesets.map((changeset) => ({
    ...changeset,
    pathname: `${directory}/${changesetArtifactFileName(changeset)}`,
  }));
}

function assertRelativeChangesetPath(pathname: unknown, index: number): string {
  if (typeof pathname !== "string" || pathname.length === 0)
    throw new Error(`Changeset ${index} is missing a pathname`);
  if (path.isAbsolute(pathname) || pathname.includes(".."))
    throw new Error(
      `Changeset ${index} pathname must be relative to the artifact root: ${pathname}`
    );
  return pathname;
}

/**
 * Read `csFileProps.json` from an artifact or working copy, rebasing every `pathname` to an
 * absolute path below `directory`. This is the only supported reader: `ChangedInstanceIds` opens
 * changesets by `pathname` verbatim, so relative paths must never reach it.
 */
export function readChangesetFileProps(
  directory: string,
  propsFileName = artifactChangesetPropsFileName
): ChangesetFileProps[] {
  const propsFile = path.join(directory, propsFileName);
  const parsed: unknown = JSON.parse(fs.readFileSync(propsFile, "utf8"));
  if (!Array.isArray(parsed))
    throw new Error(`Fixture artifact ${propsFile} must contain an array`);
  return parsed.map((entry, index) => {
    const changeset = entry as ChangesetFileProps;
    const relative = assertRelativeChangesetPath(changeset.pathname, index);
    const pathname = path.join(directory, ...relative.split("/"));
    if (!fs.existsSync(pathname))
      throw new Error(
        `Fixture artifact is missing changeset file: ${pathname}`
      );
    return { ...changeset, pathname };
  });
}

export function artifactBriefcasePath(directory: string): string {
  return path.join(directory, artifactBriefcaseFileName);
}

function validateBriefcaseManifest(
  value: unknown
): value is FixtureArtifactBriefcaseManifest {
  if (value === null || typeof value !== "object") return false;
  const briefcase = value as Partial<FixtureArtifactBriefcaseManifest>;
  return (
    typeof briefcase.fileName === "string" &&
    typeof briefcase.briefcaseId === "number" &&
    typeof briefcase.changeset?.id === "string" &&
    typeof briefcase.byteLength === "number"
  );
}

function validateChangesetManifest(
  value: unknown
): value is FixtureArtifactChangesetManifest {
  if (value === null || typeof value !== "object") return false;
  const changesets = value as Partial<FixtureArtifactChangesetManifest>;
  return (
    typeof changesets.directory === "string" &&
    typeof changesets.propsFile === "string" &&
    typeof changesets.count === "number" &&
    typeof changesets.baseChangesetIndex === "number"
  );
}

function validateLiveHubManifest(
  value: unknown
): value is LiveHubFixtureArtifactManifest {
  if (value === null || typeof value !== "object") return false;
  const liveHub = value as Partial<LiveHubFixtureArtifactManifest>;
  return (
    typeof liveHub.iTwinId === "string" &&
    typeof liveHub.source?.iModelId === "string" &&
    typeof liveHub.source.iModelName === "string" &&
    typeof liveHub.source.version0File === "string" &&
    typeof liveHub.target?.iModelId === "string" &&
    typeof liveHub.target.iModelName === "string" &&
    typeof liveHub.target.version0File === "string" &&
    validateBriefcaseManifest(liveHub.target.briefcase) &&
    validateChangesetManifest(liveHub.target.changesets)
  );
}

export function writeFixtureArtifactManifest(
  directory: string,
  manifest: FixtureArtifactManifest
): void {
  fs.writeFileSync(
    path.join(directory, artifactManifestFileName),
    `${JSON.stringify(manifest, undefined, 2)}\n`
  );
}

export function validateFixtureArtifactManifest(
  value: unknown
): FixtureArtifactManifest {
  if (value === null || typeof value !== "object")
    throw new Error("Fixture artifact manifest must be an object");
  const manifest = value as Partial<FixtureArtifactManifest>;
  if (manifest.artifactVersion !== fixtureArtifactVersion)
    throw new Error(
      `Unsupported fixture artifact version ${String(
        manifest.artifactVersion
      )}; expected ${fixtureArtifactVersion}`
    );
  if (
    typeof manifest.contentHash !== "string" ||
    !/^[a-f0-9]{64}$/.test(manifest.contentHash) ||
    (manifest.iModelInventory !== undefined &&
      !isIModelInventory(manifest.iModelInventory)) ||
    !validateBriefcaseManifest(manifest.briefcase) ||
    !validateChangesetManifest(manifest.changesets) ||
    typeof manifest.buildMilliseconds !== "number" ||
    typeof manifest.builtAt !== "string"
  )
    throw new Error("Fixture artifact manifest has an invalid shape");
  if (
    manifest.recipeDataFile !== undefined &&
    typeof manifest.recipeDataFile !== "string"
  )
    throw new Error(
      "Fixture artifact manifest has an invalid recipeDataFile entry"
    );
  const descriptor = validateFixtureDescriptor(manifest.descriptor);
  if (
    descriptor.layout.topology === "source-and-empty-target" &&
    !validateLiveHubManifest(manifest.liveHub)
  )
    throw new Error(
      "Live-hub fixture artifact manifest is missing source and target restoration data"
    );
  if (
    descriptor.layout.topology === "source-only" &&
    manifest.liveHub !== undefined
  )
    throw new Error("Source-only fixture artifact cannot declare a live hub");
  return manifest as FixtureArtifactManifest;
}

/**
 * Read and validate an artifact directory. Verifies the manifest against what is actually on disk
 * so a truncated or partially-copied artifact fails here rather than deep inside a benchmark.
 */
export function readFixtureArtifact(directory: string): FixtureArtifact {
  const manifest = validateFixtureArtifactManifest(
    JSON.parse(
      fs.readFileSync(path.join(directory, artifactManifestFileName), "utf8")
    )
  );
  const validateBriefcase = (
    briefcaseManifest: FixtureArtifactBriefcaseManifest,
    label: string
  ) => {
    const briefcase = path.join(directory, briefcaseManifest.fileName);
    if (!fs.existsSync(briefcase))
      throw new Error(`Fixture artifact is missing its ${label}: ${briefcase}`);
    const byteLength = fs.statSync(briefcase).size;
    if (byteLength !== briefcaseManifest.byteLength)
      throw new Error(
        `Fixture artifact ${label} is ${byteLength} bytes but its manifest declares ${briefcaseManifest.byteLength}`
      );
    return byteLength;
  };
  const validateChangesets = (
    changesetManifest: FixtureArtifactChangesetManifest,
    label: string
  ) => {
    const changesets = readChangesetFileProps(
      directory,
      changesetManifest.propsFile
    );
    if (changesets.length !== changesetManifest.count)
      throw new Error(
        `Fixture artifact has ${changesets.length} ${label} but its manifest declares ${changesetManifest.count}`
      );
  };
  const sourceByteLength = validateBriefcase(
    manifest.briefcase,
    "source briefcase"
  );
  if (
    manifest.iModelInventory !== undefined &&
    sourceByteLength !== manifest.iModelInventory.byteLength
  )
    throw new Error(
      `Fixture artifact source briefcase is ${sourceByteLength} bytes but its inventory declares ${manifest.iModelInventory.byteLength}`
    );
  validateChangesets(manifest.changesets, "source changesets");
  if (manifest.liveHub) {
    validateBriefcase(manifest.liveHub.target.briefcase, "target briefcase");
    validateChangesets(manifest.liveHub.target.changesets, "target changesets");
    for (const seed of [
      manifest.liveHub.source.version0File,
      manifest.liveHub.target.version0File,
    ]) {
      if (!fs.existsSync(path.join(directory, seed)))
        throw new Error(
          `Fixture artifact is missing its version-zero seed: ${seed}`
        );
    }
  }
  if (
    manifest.recipeDataFile !== undefined &&
    !fs.existsSync(path.join(directory, manifest.recipeDataFile))
  )
    throw new Error(
      `Fixture artifact is missing the recipe data its manifest declares: ${manifest.recipeDataFile}`
    );
  const contentHash = fixtureArtifactContentHash(directory);
  if (contentHash !== manifest.contentHash)
    throw new Error(
      `Fixture artifact content hash is ${contentHash} but its manifest declares ${manifest.contentHash}`
    );
  return { directory, manifest };
}
