/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { createInterface } from "node:readline/promises";
import { runWithCleanup } from "../../../Cleanup.js";
import {
  BenchmarkMeasurement,
  BenchmarkMeasurementContext,
} from "../framework/BenchmarkRunner.js";

export type QuickProfileMode = "external" | "js-cpu";

interface CpuProfileOptions {
  profileDir: string;
  profileName: string;
}

type CpuProfiler = (
  measure: () => Promise<void>,
  options: CpuProfileOptions
) => Promise<void>;

interface CpuProfilerModule {
  runWithCpuProfiler: CpuProfiler;
}

interface QuickProfilerDependencies {
  loadCpuProfiler(): Promise<CpuProfiler>;
  waitForInput(message: string): Promise<void>;
  write(message: string): void;
}

export interface QuickProfilerOptions {
  mode: QuickProfileMode;
  profileDirectory: string;
}

function isCpuProfilerModule(value: unknown): value is CpuProfilerModule {
  return (
    typeof value === "object" &&
    value !== null &&
    "runWithCpuProfiler" in value &&
    typeof value.runWithCpuProfiler === "function"
  );
}

async function loadCpuProfiler(): Promise<CpuProfiler> {
  const moduleName = "@bentley/hook-profiler/js-cpu";
  const loaded: unknown = await import(moduleName);
  if (!isCpuProfilerModule(loaded))
    throw new Error(`${moduleName} does not export runWithCpuProfiler`);
  return loaded.runWithCpuProfiler;
}

async function waitForInput(message: string): Promise<void> {
  if (!process.stdin.isTTY || !process.stdout.isTTY)
    throw new Error("External profiling requires an interactive terminal");
  const readline = createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  try {
    await readline.question(`${message}\nPress Enter to continue.`);
  } finally {
    readline.close();
  }
}

const defaultDependencies: QuickProfilerDependencies = {
  loadCpuProfiler,
  waitForInput,
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
      await dependencies.waitForInput(
        `Ready to profile ${label}; PID=${process.pid}. Attach and start the profiler now.`
      );
      dependencies.write(`PROFILE START ${label} PID=${process.pid}\n`);
      await runWithCleanup(measure, [
        {
          name: "finish external profiling interval",
          run: async () => {
            dependencies.write(`PROFILE END ${label} PID=${process.pid}\n`);
            await dependencies.waitForInput("Stop or detach the profiler now.");
          },
        },
      ]);
      return;
    }

    fs.mkdirSync(options.profileDirectory, { recursive: true });
    const profileName = `${context.scenarioId}-${context.fixtureId}`;
    const cpuProfiler = await dependencies.loadCpuProfiler();
    dependencies.write(
      `PROFILE START ${label} PID=${process.pid}; output=${path.join(
        options.profileDirectory,
        `${profileName}_<timestamp>.js.cpuprofile`
      )}\n`
    );
    try {
      await cpuProfiler(measure, {
        profileDir: options.profileDirectory,
        profileName,
      });
    } finally {
      dependencies.write(`PROFILE END ${label} PID=${process.pid}\n`);
    }
  };
}
