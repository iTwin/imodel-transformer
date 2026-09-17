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
        loadCpuProfiler: async () => {
          throw new Error("CPU profiler must not load in external mode");
        },
        waitForInput: async (message) => {
          events.push(`wait:${message}`);
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
        loadCpuProfiler: async () => {
          throw new Error("CPU profiler must not load in external mode");
        },
        waitForInput: async () => {},
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
    const directory = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-profiler-unit-")
    );
    const events: string[] = [];
    const measurement = createQuickProfileMeasurement(
      { mode: "js-cpu", profileDirectory: directory },
      {
        loadCpuProfiler: async () => async (measure, options) => {
          events.push(`cpu:${options.profileName}:${options.profileDir}`);
          await measure();
        },
        waitForInput: async () => {
          throw new Error("Automatic profiling must not wait for input");
        },
        write: (message) => {
          events.push(message.trim());
        },
      }
    );
    try {
      await measurement(async () => {
        events.push("measure");
      }, context);
      expect(events[0]).to.match(/^PROFILE START/);
      expect(events[1]).to.equal(`cpu:scenario-fixture:${directory}`);
      expect(events[2]).to.equal("measure");
      expect(events[3]).to.match(/^PROFILE END/);
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it("ends an automatic V8 interval when measurement fails", async () => {
    const directory = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-profiler-unit-")
    );
    const events: string[] = [];
    const expected = new Error("measurement failed");
    const measurement = createQuickProfileMeasurement(
      { mode: "js-cpu", profileDirectory: directory },
      {
        loadCpuProfiler: async () => async (measure) => {
          await measure();
        },
        waitForInput: async () => {
          throw new Error("Automatic profiling must not wait for input");
        },
        write: (message) => {
          events.push(message.trim());
        },
      }
    );
    try {
      await expect(
        measurement(async () => {
          throw expected;
        }, context)
      ).rejects.toBe(expected);
      expect(events.some((event) => event.startsWith("PROFILE END"))).to.equal(
        true
      );
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });
});
