/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";

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
  readonly coreBackendVersion: string;
  /** SHA-256 identity of the resolved core-backend package produced by the child process. */
  readonly coreBackendPackageHash: string;
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
