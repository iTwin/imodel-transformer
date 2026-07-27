/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as crypto from "crypto";
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
}

export interface EnvironmentClass {
  readonly id: string;
  readonly descriptor: EnvironmentDescriptor;
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
      ? (process.env.RUNNER_NAME ?? process.env.RUNNER_OS ?? "github-hosted")
      : "local",
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
  ].join("|");
  return {
    id: crypto
      .createHash("sha256")
      .update(canonical)
      .digest("hex")
      .slice(0, 16),
    descriptor,
  };
}
