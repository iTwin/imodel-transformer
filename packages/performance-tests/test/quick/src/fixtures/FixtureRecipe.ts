/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";

import { createRequire } from "node:module";
import * as path from "node:path";
import { AccessToken } from "@itwin/core-bentley";
import { BriefcaseDb, IModelDb } from "@itwin/core-backend";
import {
  canonicalSha256,
  ExternalFixtureSourceIdentity,
  FixtureDescriptor,
  FixtureDistribution,
  FixtureTopology,
  fixtureWorkloadGeneratorIdentity,
} from "./FixtureDescriptor.js";
import { sha256File } from "./FixtureArtifact.js";
import { quickRootDirectory } from "../support/paths.js";

const localRequire = createRequire(import.meta.url);
const repositoryRoot = path.resolve(quickRootDirectory, "..", "..", "..", "..");
const lockfileName = path.join(repositoryRoot, "pnpm-lock.yaml");

function packageVersion(packageName: string): string {
  const packageJson = JSON.parse(
    fs.readFileSync(localRequire.resolve(`${packageName}/package.json`), "utf8")
  ) as { version: string };
  return packageJson.version;
}

const generator = Object.freeze({
  coreBackend: packageVersion("@itwin/core-backend"),
  node: process.version,
  transformer: packageVersion("@itwin/imodel-transformer"),
});

function identityFile(fileName: string) {
  return {
    path: path.relative(repositoryRoot, fileName).replaceAll(path.sep, "/"),
    contents: fs.readFileSync(fileName, "utf8").replace(/\r\n?/g, "\n"),
  };
}

function deepFreeze<T>(value: T): Readonly<T> {
  if (value === null || typeof value !== "object") return value;
  for (const entry of Object.values(value as Record<string, unknown>))
    deepFreeze(entry);
  return Object.freeze(value);
}

function assertIdentityInput(
  value: unknown,
  at: string,
  seen = new Set<object>()
): void {
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return;
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new Error(`${at} contains a non-finite number`);
    return;
  }
  if (typeof value !== "object")
    throw new Error(`${at} must contain only JSON-native values`);
  if (seen.has(value)) throw new Error(`${at} contains a circular reference`);
  seen.add(value);
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index++) {
      if (!Object.hasOwn(value, index))
        throw new Error(`${at}[${index}] is an array hole`);
      assertIdentityInput(value[index], `${at}[${index}]`, seen);
    }
  } else {
    const prototype = Object.getPrototypeOf(value) as object | null;
    if (prototype !== Object.prototype && prototype !== null)
      throw new Error(`${at} must contain only plain objects and arrays`);
    if (Object.getOwnPropertySymbols(value).length > 0)
      throw new Error(`${at} contains a symbol key`);
    for (const [key, entry] of Object.entries(value))
      assertIdentityInput(entry, `${at}.${key}`, seen);
  }
  seen.delete(value);
}

export interface FixtureRecipeIdentity {
  /** Source files containing the imperative construction and validation logic. */
  readonly implementationFiles: readonly string[];
  /** EC schema files imported by the recipe. */
  readonly schemaFiles?: readonly string[];
  /** Additional stable, serializable inputs that affect generated contents. */
  readonly values?: unknown;
}

export interface FixtureRecipeContext<TParameters> {
  readonly descriptor: FixtureDescriptor;
  readonly parameters: Readonly<TParameters>;
  readonly schemaFiles: readonly string[];
}

/**
 * The complete author-facing specification for generating an iModel workload.
 *
 * Recipes own construction, declared identity inputs, distribution, and optional validation.
 * Providers continue to own local test hub, database, artifact, and cleanup lifecycles.
 */
export interface FixtureRecipe<
  TParameters,
  TState = unknown,
  TArtifactData = unknown,
> {
  readonly id: string;
  readonly identity: FixtureRecipeIdentity;
  readonly distribution: (
    parameters: Readonly<TParameters>
  ) => FixtureDistribution;
  createSeed(
    fileName: string,
    context: FixtureRecipeContext<TParameters>
  ): Promise<TState>;
  applySourceChangesets(
    db: BriefcaseDb,
    accessToken: AccessToken,
    context: FixtureRecipeContext<TParameters>,
    state: TState
  ): Promise<TArtifactData | void>;
  /** Optional post-construction validation, run before a fixture is consumed or captured. */
  validate?(
    db: IModelDb,
    context: FixtureRecipeContext<TParameters>
  ): Promise<void>;
}

export function defineFixtureRecipe<
  TParameters,
  TState = unknown,
  TArtifactData = unknown,
>(
  recipe: FixtureRecipe<TParameters, TState, TArtifactData>
): FixtureRecipe<TParameters, TState, TArtifactData> {
  if (recipe.identity.values !== undefined)
    assertIdentityInput(recipe.identity.values, "Recipe identity values");
  const identity = deepFreeze({
    implementationFiles: [...recipe.identity.implementationFiles],
    schemaFiles:
      recipe.identity.schemaFiles === undefined
        ? undefined
        : [...recipe.identity.schemaFiles],
    values:
      recipe.identity.values === undefined
        ? undefined
        : structuredClone(recipe.identity.values),
  });
  return Object.freeze({ ...recipe, identity });
}

export interface FixtureConfiguration<TParameters> {
  readonly id: string;
  readonly version: number;
  readonly label: string;
  readonly scenarioClaims: readonly string[];
  readonly topology: FixtureTopology;
  readonly seed: number;
  readonly parameters: Readonly<TParameters>;
}

/**
 * A named immutable invocation of a recipe. Infrastructure derives its serializable descriptor;
 * authors never duplicate hashes, generator versions, or distributions in a catalog.
 */
export interface ConfiguredFixture {
  readonly descriptor: FixtureDescriptor;
  /** Filesystem path used only while authoring an external-source artifact. Never reported. */
  readonly externalSourceFileName?: string;
  readonly recipeId: string;
  createSeed(fileName: string): Promise<unknown>;
  applySourceChangesets(
    db: BriefcaseDb,
    accessToken: AccessToken,
    state: unknown
  ): Promise<unknown>;
  validate?(db: IModelDb): Promise<void>;
}

function resolveThroughExistingAncestor(fileName: string): string {
  let ancestor = path.resolve(fileName);
  const suffix: string[] = [];
  while (!fs.existsSync(ancestor)) {
    const parent = path.dirname(ancestor);
    if (parent === ancestor)
      throw new Error(`Cannot resolve fixture path: ${fileName}`);
    suffix.unshift(path.basename(ancestor));
    ancestor = parent;
  }
  return path.join(fs.realpathSync(ancestor), ...suffix);
}

export function assertExternalFixtureSourceOutsideDirectory(
  fixture: ConfiguredFixture,
  managedDirectory: string
): void {
  if (fixture.externalSourceFileName === undefined) return;
  const isInside = (candidate: string, root: string) => {
    const relative = path.relative(root, candidate);
    return (
      relative === "" ||
      (relative !== ".." &&
        !relative.startsWith(`..${path.sep}`) &&
        !path.isAbsolute(relative))
    );
  };
  const lexicalSource = path.resolve(fixture.externalSourceFileName);
  const lexicalManaged = path.resolve(managedDirectory);
  const canonicalSource = fs.realpathSync(fixture.externalSourceFileName);
  const canonicalManaged = resolveThroughExistingAncestor(managedDirectory);
  if (
    isInside(lexicalSource, lexicalManaged) ||
    isInside(canonicalSource, canonicalManaged)
  )
    throw new Error(
      `QUICK_PERF_STANDALONE_BIM must be outside benchmark-managed directories: ${fixture.externalSourceFileName}`
    );
}

export function withExternalFixtureSourceIdentity(
  fixture: ConfiguredFixture,
  source: ExternalFixtureSourceIdentity
): ConfiguredFixture {
  const descriptor: FixtureDescriptor = Object.freeze({
    ...fixture.descriptor,
    source,
    recipeHash: canonicalSha256({
      recipeHash: fixture.descriptor.recipeHash,
      source,
    }),
  });
  return Object.freeze({ ...fixture, descriptor });
}

/**
 * Bind external standalone bytes to a configured fixture without changing the catalog entry.
 * The recipe still identifies the fixture contract; the derived hash adds the external identity.
 */
export function withExternalFixtureSource(
  fixture: ConfiguredFixture,
  fileName: string
): ConfiguredFixture {
  if (!path.isAbsolute(fileName))
    throw new Error(
      `QUICK_PERF_STANDALONE_BIM must be an absolute path: ${fileName}`
    );
  const resolved = path.resolve(fileName);
  if (!fs.existsSync(resolved))
    throw new Error(`QUICK_PERF_STANDALONE_BIM does not exist: ${resolved}`);
  const stat = fs.statSync(resolved);
  if (!stat.isFile())
    throw new Error(
      `QUICK_PERF_STANDALONE_BIM must identify a file: ${resolved}`
    );
  if (path.extname(resolved).toLowerCase() !== ".bim")
    throw new Error(
      `QUICK_PERF_STANDALONE_BIM must have a .bim extension: ${resolved}`
    );
  if (!Number.isSafeInteger(stat.size))
    throw new Error(
      `QUICK_PERF_STANDALONE_BIM size must be a safe integer: ${resolved}`
    );
  const source: ExternalFixtureSourceIdentity = Object.freeze({
    kind: "external-bim",
    fileName: path.basename(resolved),
    byteLength: stat.size,
    sha256: sha256File(resolved),
  });
  const identified = withExternalFixtureSourceIdentity(fixture, source);
  return Object.freeze({
    ...identified,
    externalSourceFileName: resolved,
  });
}

export function configureFixture<TParameters, TState, TArtifactData>(
  recipe: FixtureRecipe<TParameters, TState, TArtifactData>,
  configuration: FixtureConfiguration<TParameters>
): ConfiguredFixture {
  if (!Number.isSafeInteger(configuration.seed))
    throw new Error("Fixture seed must be a safe integer");
  if (!Number.isSafeInteger(configuration.version) || configuration.version < 1)
    throw new Error("Fixture version must be a positive safe integer");
  assertIdentityInput(configuration.parameters, "Fixture parameters");
  const parameters = deepFreeze(structuredClone(configuration.parameters));
  const distribution = deepFreeze(recipe.distribution(parameters));
  assertIdentityInput(distribution, "Fixture distribution");
  const schemaFiles = Object.freeze([...(recipe.identity.schemaFiles ?? [])]);
  const descriptor: FixtureDescriptor = Object.freeze({
    id: configuration.id,
    version: configuration.version,
    label: configuration.label,
    scenarioClaims: Object.freeze([...configuration.scenarioClaims]),
    layout: Object.freeze({
      kind: "reconstructed",
      topology: configuration.topology,
      recipe: recipe.id,
      seed: configuration.seed,
    }),
    distribution,
    generator,
    recipeHash: canonicalSha256({
      fixture: {
        id: configuration.id,
        version: configuration.version,
        label: configuration.label,
        scenarioClaims: configuration.scenarioClaims,
        topology: configuration.topology,
        recipe: recipe.id,
        seed: configuration.seed,
      },
      parameters,
      distribution,
      identity: recipe.identity.values,
      implementationFiles:
        recipe.identity.implementationFiles.map(identityFile),
      schemaFiles: schemaFiles.map(identityFile),
      lockfile: identityFile(lockfileName),
      versions: fixtureWorkloadGeneratorIdentity(generator),
    }),
  });
  const context: FixtureRecipeContext<TParameters> = Object.freeze({
    descriptor,
    parameters,
    schemaFiles,
  });
  const configured = {
    descriptor,
    recipeId: recipe.id,
    createSeed: async (fileName: string) =>
      recipe.createSeed(fileName, context),
    applySourceChangesets: async (
      db: BriefcaseDb,
      accessToken: AccessToken,
      state: unknown
    ) =>
      recipe.applySourceChangesets(db, accessToken, context, state as TState),
  };
  if (recipe.validate === undefined) return Object.freeze(configured);
  return Object.freeze({
    ...configured,
    validate: async (db: IModelDb) => {
      if (recipe.validate === undefined)
        throw new Error(`Fixture recipe "${recipe.id}" lost its validator`);
      await recipe.validate(db, context);
    },
  });
}
