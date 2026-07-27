/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { ChangesetFileProps } from "@itwin/core-common";
import { DatasetDescriptor } from "./DatasetDescriptor";
import { validateDescriptor } from "./FixtureManifest";

/**
 * Version of the on-disk artifact layout. Bump when the directory contract changes in a way that
 * an older reader cannot interpret.
 */
export const fixtureArtifactVersion = 1;

export const artifactBriefcaseFileName = "briefcase.bim";
export const artifactChangesetDirectoryName = "changesets";
export const artifactChangesetPropsFileName = "csFileProps.json";
export const artifactManifestFileName = "manifest.json";

export interface FixtureArtifactManifest {
  readonly artifactVersion: number;
  readonly descriptor: DatasetDescriptor;
  readonly briefcase: {
    readonly fileName: string;
    readonly briefcaseId: number;
    readonly changeset: { readonly id: string; readonly index?: number };
    readonly byteLength: number;
  };
  readonly changesets: {
    readonly directory: string;
    readonly propsFile: string;
    readonly count: number;
    /** Index of the changeset immediately before the first captured changeset. */
    readonly baseChangesetIndex: number;
    readonly firstIndex?: number;
    readonly lastIndex?: number;
  };
  readonly buildMilliseconds: number;
  readonly builtAt: string;
}

/** A stage-1 fixture artifact: immutable bytes that stage 2 copies from. */
export interface FixtureArtifact {
  readonly directory: string;
  readonly manifest: FixtureArtifactManifest;
}

export function changesetArtifactFileName(
  changeset: Pick<ChangesetFileProps, "id" | "index">
): string {
  return `${String(changeset.index).padStart(6, "0")}-${changeset.id}.cs`;
}

/** Rewrite `pathname` to a POSIX path relative to the artifact root. */
export function toRelativeChangesetProps(
  changesets: readonly ChangesetFileProps[]
): ChangesetFileProps[] {
  return changesets.map((changeset) => ({
    ...changeset,
    pathname: `${artifactChangesetDirectoryName}/${changesetArtifactFileName(
      changeset
    )}`,
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
  directory: string
): ChangesetFileProps[] {
  const propsFile = path.join(directory, artifactChangesetPropsFileName);
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
    typeof manifest.briefcase?.fileName !== "string" ||
    typeof manifest.briefcase.briefcaseId !== "number" ||
    typeof manifest.briefcase.changeset?.id !== "string" ||
    typeof manifest.briefcase.byteLength !== "number" ||
    typeof manifest.changesets?.directory !== "string" ||
    typeof manifest.changesets.propsFile !== "string" ||
    typeof manifest.changesets.count !== "number" ||
    typeof manifest.changesets.baseChangesetIndex !== "number" ||
    typeof manifest.buildMilliseconds !== "number" ||
    typeof manifest.builtAt !== "string"
  )
    throw new Error("Fixture artifact manifest has an invalid shape");
  validateDescriptor(manifest.descriptor);
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
  const briefcase = path.join(directory, manifest.briefcase.fileName);
  if (!fs.existsSync(briefcase))
    throw new Error(`Fixture artifact is missing its briefcase: ${briefcase}`);
  const byteLength = fs.statSync(briefcase).size;
  if (byteLength !== manifest.briefcase.byteLength)
    throw new Error(
      `Fixture artifact briefcase is ${byteLength} bytes but its manifest declares ${manifest.briefcase.byteLength}`
    );
  const changesets = readChangesetFileProps(directory);
  if (changesets.length !== manifest.changesets.count)
    throw new Error(
      `Fixture artifact has ${changesets.length} changesets but its manifest declares ${manifest.changesets.count}`
    );
  return { directory, manifest };
}
