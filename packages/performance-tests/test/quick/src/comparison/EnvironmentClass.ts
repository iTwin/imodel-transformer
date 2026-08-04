/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as crypto from "node:crypto";
import * as os from "os";

/**
 * Environment classification for band and baseline lookup.
 *
 * A band captured on one machine class is noise on another: hosted Ubuntu measured a CV of 1.03%,
 * while six local macOS runs measured 6.32, 3.71, 3.00, 4.80, 2.17 and 6.43 percent. A lookup miss
 * yields `uncalibrated`, never a silent fall-back to another environment's band.
 */

export interface EnvironmentDescriptor {
  readonly platform: string;
  readonly arch: string;
  readonly cpuModel: string;
  readonly cpuCount: number;
  /** Total memory rounded down to a power of two, in GiB, so trivial differences do not re-key. */
  readonly memoryGibBucket: number;
  readonly nodeMajor: number;
  readonly runner: string;
  /** Immutable hosted image identity when the runner exposes one. */
  readonly runnerImage?: string;
}

export interface EnvironmentClass {
  readonly id: string;
  readonly descriptor: EnvironmentDescriptor;
}

function assertNonEmpty(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0)
    throw new Error(`${label} cannot be empty`);
}

export function validateEnvironmentClass(environment: EnvironmentClass): void {
  if (!environment || typeof environment !== "object")
    throw new Error("Environment class must be an object");
  if (!environment.descriptor || typeof environment.descriptor !== "object")
    throw new Error("Environment descriptor must be an object");
  assertNonEmpty(environment.id, "Environment id");
  const descriptor = environment.descriptor;
  assertNonEmpty(descriptor.platform, "Environment platform");
  assertNonEmpty(descriptor.arch, "Environment architecture");
  assertNonEmpty(descriptor.cpuModel, "Environment CPU model");
  assertNonEmpty(descriptor.runner, "Environment runner");
  for (const [label, value, minimum] of [
    ["CPU count", descriptor.cpuCount, 1],
    ["Memory bucket", descriptor.memoryGibBucket, 0],
    ["Node major version", descriptor.nodeMajor, 1],
  ] as const)
    if (!Number.isSafeInteger(value) || value < minimum)
      throw new Error(`Environment ${label} must be an integer >= ${minimum}`);
}

function memoryBucketGib(totalBytes: number): number {
  const gib = totalBytes / 1024 ** 3;
  return gib < 1 ? 0 : 2 ** Math.floor(Math.log2(gib));
}

export function describeEnvironment(): EnvironmentDescriptor {
  const cpus = os.cpus();
  return {
    platform: process.platform,
    arch: process.arch,
    cpuModel: cpus[0]?.model.trim() ?? "unknown",
    cpuCount: cpus.length,
    memoryGibBucket: memoryBucketGib(os.totalmem()),
    nodeMajor: Number(process.versions.node.split(".")[0]),
    runner: process.env.GITHUB_ACTIONS
      ? process.env.RUNNER_ENVIRONMENT === "github-hosted"
        ? `github-hosted:${process.env.RUNNER_OS ?? process.platform}:${
            process.env.RUNNER_ARCH ?? process.arch
          }`
        : `self-hosted:${
            process.env.RUNNER_NAME ?? process.env.RUNNER_OS ?? "unknown"
          }`
      : "local",
    runnerImage: process.env.GITHUB_ACTIONS
      ? `${process.env.ImageOS ?? process.env.RUNNER_OS ?? "unknown"}:${
          process.env.ImageVersion ?? "unknown"
        }`
      : undefined,
  };
}

/**
 * Stable hash over the descriptor.
 *
 * The readable components travel with the hash so a mismatch can be explained rather than merely
 * detected -- "no band for environment 3f2a1c" is not an actionable message on its own.
 */
export function classifyEnvironment(
  descriptor: EnvironmentDescriptor = describeEnvironment()
): EnvironmentClass {
  const canonical = [
    descriptor.platform,
    descriptor.arch,
    descriptor.cpuModel,
    descriptor.cpuCount,
    descriptor.memoryGibBucket,
    descriptor.nodeMajor,
    descriptor.runner,
    descriptor.runnerImage ?? "none",
  ].join("|");
  const environment = {
    id: crypto
      .createHash("sha256")
      .update(canonical)
      .digest("hex")
      .slice(0, 16),
    descriptor,
  };
  validateEnvironmentClass(environment);
  return environment;
}
