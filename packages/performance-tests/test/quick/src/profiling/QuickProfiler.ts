/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { runWithCpuProfiler } from "@bentley/hook-profiler/js-cpu";
import { runWithPause } from "@bentley/hook-profiler/pause";
import {
  BenchmarkMeasurement,
  BenchmarkMeasurementContext,
} from "../framework/BenchmarkRunner.js";

export type QuickProfileMode = "external" | "js-cpu";

interface QuickProfilerDependencies {
  runWithCpuProfiler: typeof runWithCpuProfiler;
  runWithPause: typeof runWithPause;
  write(message: string): void;
}

export interface QuickProfilerOptions {
  mode: QuickProfileMode;
  profileDirectory: string;
}

const defaultDependencies: QuickProfilerDependencies = {
  runWithCpuProfiler,
  runWithPause,
  write: (message) => process.stdout.write(message),
};

export function resolveQuickProfileMode(
  configured: string | undefined
): QuickProfileMode {
  const mode = configured?.trim() || "external";
  if (mode === "external" || mode === "js-cpu") return mode;
  throw new Error(
    `QUICK_PERF_PROFILE_MODE must be "external" or "js-cpu"; received "${configured}"`
  );
}

function profileLabel(context: BenchmarkMeasurementContext): string {
  return `${context.scenarioId} on ${context.fixtureId}`;
}

export function createQuickProfileMeasurement(
  options: QuickProfilerOptions,
  dependencies: QuickProfilerDependencies = defaultDependencies
): BenchmarkMeasurement {
  return async (measure, context) => {
    const label = profileLabel(context);
    if (options.mode === "external") {
      await dependencies.runWithPause(measure, {
        label,
        write: (message) => dependencies.write(message),
      });
      return;
    }

    const profileName = `${context.scenarioId}-${context.fixtureId}`;
    dependencies.write(
      `PROFILE START ${label} PID=${process.pid}; output-directory=${options.profileDirectory}\n`
    );
    let profilePath: string | undefined;
    try {
      ({ profilePath } = await dependencies.runWithCpuProfiler(measure, {
        profileDir: options.profileDirectory,
        profileName,
      }));
    } finally {
      dependencies.write(
        `PROFILE END ${label} PID=${process.pid}${
          profilePath === undefined ? "" : `; output=${profilePath}`
        }\n`
      );
    }
  };
}
