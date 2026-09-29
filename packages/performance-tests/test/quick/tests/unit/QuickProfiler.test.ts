/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, expect, it } from "vitest";
import {
  createQuickProfileMeasurement,
  resolveQuickProfileMode,
} from "../../src/profiling/QuickProfiler.js";
import { BenchmarkMeasurementContext } from "../../src/framework/BenchmarkRunner.js";
import { runWithCpuProfiler } from "@bentley/hook-profiler/js-cpu";
import { runWithPause } from "@bentley/hook-profiler/pause";

const context: BenchmarkMeasurementContext = {
  fixtureId: "fixture",
  measured: true,
  sample: 1,
  scenarioId: "scenario",
};

describe("quick profiler", () => {
  it("resolves supported modes and defaults to external", () => {
    expect(resolveQuickProfileMode(undefined)).to.equal("external");
    expect(resolveQuickProfileMode(" external ")).to.equal("external");
    expect(resolveQuickProfileMode("js-cpu")).to.equal("js-cpu");
    expect(() => resolveQuickProfileMode("sqlite")).to.throw(
      /must be "external" or "js-cpu"/
    );
  });

  it("places external profiler prompts around the measured operation", async () => {
    const events: string[] = [];
    const measurement = createQuickProfileMeasurement(
      { mode: "external", profileDirectory: "unused" },
      {
        runWithCpuProfiler: async () => {
          throw new Error("CPU profiler must not load in external mode");
        },
        runWithPause: async (measure, options) => {
          return runWithPause(measure, {
            ...options,
            waitForInput: async (message) => {
              events.push(`wait:${message}`);
            },
          });
        },
        write: (message) => {
          events.push(message.trim());
        },
      }
    );

    await measurement(async () => {
      events.push("measure");
    }, context);

    expect(events).to.have.length(5);
    expect(events[0]).to.match(/^wait:Ready to profile/);
    expect(events[1]).to.match(/^PROFILE START/);
    expect(events[2]).to.equal("measure");
    expect(events[3]).to.match(/^PROFILE END/);
    expect(events[4]).to.equal("wait:Stop or detach the profiler now.");
  });

  it("ends an external profiling interval when measurement fails", async () => {
    const events: string[] = [];
    const expected = new Error("measurement failed");
    const measurement = createQuickProfileMeasurement(
      { mode: "external", profileDirectory: "unused" },
      {
        runWithCpuProfiler: async () => {
          throw new Error("CPU profiler must not load in external mode");
        },
        runWithPause: async (measure, options) =>
          runWithPause(measure, {
            ...options,
            waitForInput: async () => {},
          }),
        write: (message) => {
          events.push(message.trim());
        },
      }
    );

    await expect(
      measurement(async () => {
        throw expected;
      }, context)
    ).rejects.toBe(expected);
    expect(events.some((event) => event.startsWith("PROFILE END"))).to.equal(
      true
    );
  });

  it("writes one automatic V8 profile around measurement", async () => {
    const directory = path.join(os.tmpdir(), "quick-profiler-output");
    const events: string[] = [];
    const measurement = createQuickProfileMeasurement(
      { mode: "js-cpu", profileDirectory: directory },
      {
        runWithCpuProfiler: async (measure, options) => {
          if (
            options?.profileDir === undefined ||
            options.profileName === undefined
          )
            throw new Error("Expected quick CPU profile options");
          events.push(`cpu:${options.profileName}:${options.profileDir}`);
          return {
            profilePath: path.join(
              options.profileDir ?? "",
              `${options.profileName}.js.cpuprofile`
            ),
            result: await measure(),
          };
        },
        runWithPause: async () => {
          throw new Error("Automatic profiling must not wait for input");
        },
        write: (message) => {
          events.push(message.trim());
        },
      }
    );
    await measurement(async () => {
      events.push("measure");
    }, context);
    expect(events[0]).to.match(/^PROFILE START/);
    expect(events[1]).to.equal(`cpu:scenario-fixture:${directory}`);
    expect(events[2]).to.equal("measure");
    expect(events[3]).to.equal(
      `PROFILE END scenario on fixture PID=${process.pid}; output=${path.join(
        directory,
        "scenario-fixture.js.cpuprofile"
      )}`
    );
  });

  it("ends an automatic V8 interval when measurement fails", async () => {
    const directory = path.join(os.tmpdir(), "quick-profiler-output");
    const events: string[] = [];
    const expected = new Error("measurement failed");
    const measurement = createQuickProfileMeasurement(
      { mode: "js-cpu", profileDirectory: directory },
      {
        runWithCpuProfiler: async (measure) => {
          return {
            profilePath: "unreachable",
            result: await measure(),
          };
        },
        runWithPause: async () => {
          throw new Error("Automatic profiling must not wait for input");
        },
        write: (message) => {
          events.push(message.trim());
        },
      }
    );
    await expect(
      measurement(async () => {
        throw expected;
      }, context)
    ).rejects.toBe(expected);
    expect(events.some((event) => event.startsWith("PROFILE END"))).to.equal(
      true
    );
  });

  it("creates the profile directory and returns the exact written path", async () => {
    const parent = fs.mkdtempSync(path.join(os.tmpdir(), "cpu-profiler-unit-"));
    const directory = path.join(parent, "profiles");
    try {
      const profiled = await runWithCpuProfiler(async () => 42, {
        profileDir: directory,
        profileName: "exact",
        timestamp: false,
      });
      expect(profiled.result).to.equal(42);
      expect(profiled.profilePath).to.equal(
        path.join(directory, "exact.js.cpuprofile")
      );
      expect(fs.statSync(profiled.profilePath).size).to.be.greaterThan(0);
    } finally {
      fs.rmSync(parent, { recursive: true, force: true });
    }
  });

  it("rejects instead of hanging when the profile cannot be written", async () => {
    const directory = fs.mkdtempSync(
      path.join(os.tmpdir(), "cpu-profiler-write-failure-")
    );
    const profilePath = path.join(directory, "blocked.js.cpuprofile");
    fs.mkdirSync(profilePath);
    try {
      await expect(
        Promise.race([
          runWithCpuProfiler(async () => {}, {
            profileDir: directory,
            profileName: "blocked",
            timestamp: false,
          }),
          new Promise((_, reject) =>
            setTimeout(
              () => reject(new Error("CPU profiler write timed out")),
              2_000
            )
          ),
        ])
      ).rejects.not.toThrow("CPU profiler write timed out");
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it("preserves operation and post-pause failures", async () => {
    const operationError = new Error("operation failed");
    const pauseError = new Error("post-pause failed");
    let pauseCount = 0;
    const error = await runWithPause(
      async () => {
        throw operationError;
      },
      {
        waitForInput: async () => {
          if (++pauseCount === 2) throw pauseError;
        },
        write: () => {},
      }
    ).catch((caught: unknown) => caught);

    expect(error).to.be.instanceOf(AggregateError);
    expect((error as AggregateError).errors).to.deep.equal([
      operationError,
      new Error("Cleanup failed: finish pause profiling interval", {
        cause: pauseError,
      }),
    ]);
  });
});
