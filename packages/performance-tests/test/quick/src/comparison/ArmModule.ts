/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as path from "node:path";
import type { ChangedInstanceIdsDependency } from "../scenarios/changesetScanning.js";

const harnessRequire = createRequire(import.meta.url);

export type ArmOperation = "identity" | "fork-init" | "change-processing";

/**
 * Serializable arm input for a future isolated-process runner.
 *
 * This module validates paths and manifests only. It deliberately never imports an arm or links
 * native-module peers in the analysis process.
 */
export interface ArmSpec {
  readonly id: string;
  readonly packageRoot: string;
  readonly modulePath?: string;
  readonly operation: ArmOperation;
  readonly label?: string;
}

export interface ResolvedArmSpec {
  readonly spec: ArmSpec;
  readonly packageRoot: string;
  readonly modulePath: string;
  readonly transformerVersion: string;
  readonly coreBackendPeerRange: string;
}

interface PackageManifest {
  readonly main?: string;
  readonly version?: string;
  readonly peerDependencies?: Readonly<Record<string, string>>;
}

function hashRuntimeFiles(
  packageRoot: string,
  roots: readonly string[]
): string {
  const files: string[] = [];
  const visit = (current: string): void => {
    const stat = fs.statSync(current);
    if (stat.isFile()) {
      files.push(current);
      return;
    }
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      if (entry.name === "node_modules") continue;
      visit(path.join(current, entry.name));
    }
  };
  roots.forEach(visit);
  const hash = crypto.createHash("sha256");
  for (const file of [...new Set(files)].sort()) {
    hash.update(path.relative(packageRoot, file).split(path.sep).join("/"));
    hash.update("\0");
    hash.update(fs.readFileSync(file));
    hash.update("\0");
  }
  return hash.digest("hex");
}

function readManifest(packageRoot: string): PackageManifest {
  const manifestPath = path.join(packageRoot, "package.json");
  if (!fs.existsSync(manifestPath))
    throw new Error(`Arm package has no package.json: ${manifestPath}`);
  return JSON.parse(fs.readFileSync(manifestPath, "utf8")) as PackageManifest;
}

export function resolveArmSpec(spec: ArmSpec): ResolvedArmSpec {
  if (spec.id.trim().length === 0) throw new Error("Arm id cannot be empty");
  const packageRoot = fs.realpathSync(path.resolve(spec.packageRoot));
  const manifest = readManifest(packageRoot);
  const modulePath = fs.realpathSync(
    path.resolve(packageRoot, spec.modulePath ?? manifest.main ?? "index.js")
  );
  const relativeModulePath = path.relative(packageRoot, modulePath);
  if (
    relativeModulePath === ".." ||
    relativeModulePath.startsWith(`..${path.sep}`) ||
    path.isAbsolute(relativeModulePath)
  )
    throw new Error(
      `Arm "${spec.id}" module must remain inside its package root`
    );
  if (!fs.statSync(modulePath).isFile())
    throw new Error(`Arm module is not a file: ${modulePath}`);
  const coreBackendPeerRange =
    manifest.peerDependencies?.["@itwin/core-backend"];
  if (!coreBackendPeerRange)
    throw new Error(
      `Arm "${spec.id}" must declare @itwin/core-backend as a peer dependency`
    );
  return {
    spec,
    packageRoot,
    modulePath,
    transformerVersion: manifest.version ?? "unknown",
    coreBackendPeerRange,
  };
}

export interface ArmRuntimeIdentity {
  readonly armId: string;
  readonly transformerVersion: string;
  readonly transformerPackageHash: string;
  readonly coreBackendVersion: string;
  /** SHA-256 identity of the resolved core-backend package produced by the child process. */
  readonly coreBackendPackageHash: string;
}

export interface LoadedArmModule {
  readonly changedInstanceIds: ChangedInstanceIdsDependency;
  readonly runtime: ArmRuntimeIdentity;
}

/**
 * Make the child harness and selected transformer share the arm checkout's core-backend instance.
 *
 * This must run before importing any module that imports core-backend.
 */
export function aliasHarnessCoreBackendToArm(arm: ResolvedArmSpec): void {
  const armRequire = createRequire(arm.modulePath);
  const armEntry = armRequire.resolve("@itwin/core-backend");
  const harnessEntry = harnessRequire.resolve("@itwin/core-backend");
  armRequire("@itwin/core-backend");
  const armModule = armRequire.cache[armEntry];
  if (!armModule)
    throw new Error(
      `Arm "${arm.spec.id}" core-backend module could not be initialized`
    );
  harnessRequire.cache[harnessEntry] = armModule;
}

/**
 * Load the selected transformer only inside an arm child. The comparison parent must use
 * {@link resolveArmSpec} instead so native dependencies from two checkouts cannot share a process.
 */
export function loadArmModule(arm: ResolvedArmSpec): LoadedArmModule {
  const armRequire = createRequire(arm.modulePath);
  const exports = armRequire(arm.modulePath) as {
    readonly ChangedInstanceIds?: ChangedInstanceIdsDependency;
  };
  if (!exports.ChangedInstanceIds?.initialize)
    throw new Error(
      `Arm "${arm.spec.id}" does not export ChangedInstanceIds.initialize`
    );
  const coreManifestPath = armRequire.resolve(
    "@itwin/core-backend/package.json"
  );
  const coreManifestBytes = fs.readFileSync(coreManifestPath);
  const coreManifest = JSON.parse(coreManifestBytes.toString("utf8")) as {
    readonly version?: string;
  };
  const corePackageRoot = path.dirname(coreManifestPath);
  return {
    changedInstanceIds: exports.ChangedInstanceIds,
    runtime: {
      armId: arm.spec.id,
      transformerVersion: arm.transformerVersion,
      transformerPackageHash: hashRuntimeFiles(arm.packageRoot, [
        path.join(arm.packageRoot, "package.json"),
        path.dirname(arm.modulePath),
      ]),
      coreBackendVersion: coreManifest.version ?? "unknown",
      coreBackendPackageHash: hashRuntimeFiles(corePackageRoot, [
        corePackageRoot,
      ]),
    },
  };
}

/**
 * Validate child-process identities without loading either native module in this process.
 */
export function assertArmRuntimeComparable(
  armA: ArmRuntimeIdentity,
  armB: ArmRuntimeIdentity
): void {
  if (
    armA.coreBackendVersion !== armB.coreBackendVersion ||
    armA.coreBackendPackageHash !== armB.coreBackendPackageHash
  )
    throw new Error(
      `Arms "${armA.armId}" and "${armB.armId}" used different @itwin/core-backend packages`
    );
}

export function assertArmSpecsComparable(
  armA: ResolvedArmSpec,
  armB: ResolvedArmSpec
): void {
  if (armA.spec.operation !== armB.spec.operation)
    throw new Error(
      `Arms request different operations: ${armA.spec.operation} != ${armB.spec.operation}`
    );
}
