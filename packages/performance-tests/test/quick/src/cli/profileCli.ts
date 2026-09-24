/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
import { BenchmarkRunner } from "../framework/BenchmarkRunner.js";
import { resolveBenchmarkRunFromEnvironment } from "../framework/BenchmarkResolution.js";
import {
  createQuickProfileMeasurement,
  resolveQuickProfileMode,
} from "../profiling/QuickProfiler.js";
import { quickPath } from "../support/paths.js";

async function main(): Promise<void> {
  const { descriptor, fixture, scenario } =
    resolveBenchmarkRunFromEnvironment();
  const outputDirectory =
    process.env.QUICK_PERF_OUTPUT ??
    quickPath(".quick-output", "profile", descriptor.id);
  const profileDirectory =
    process.env.QUICK_PERF_PROFILE_OUTPUT ??
    path.join(outputDirectory, "profiles");
  const mode = resolveQuickProfileMode(process.env.QUICK_PERF_PROFILE_MODE);
  const measurement = createQuickProfileMeasurement({
    mode,
    profileDirectory,
  });

  process.stdout.write(
    `Preparing quick profile: scenario=${scenario.id}, fixture=${descriptor.id}, mode=${mode}\n`
  );
  const semanticDigest = await new BenchmarkRunner(
    fixture,
    outputDirectory,
    scenario
  ).runProfile(measurement);
  process.stdout.write(
    `Quick profile completed and validated; semanticDigest=${semanticDigest}\n`
  );
}

void main().catch((error) => {
  process.stderr.write(
    `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`
  );
  process.exitCode = 1;
});
