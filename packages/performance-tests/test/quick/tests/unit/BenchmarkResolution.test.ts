/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import {
  assertScenarioSupportsFixture,
  defaultQuickPerformanceMeasuredSamples,
  resolveBenchmarkRun,
  resolveBenchmarkRunFromEnvironment,
  resolveMeasuredSamples,
  resolveMeasuredSamplesFromEnvironment,
} from "../../src/framework/BenchmarkResolution.js";
import { BenchmarkScenarioDefinition } from "../../src/framework/BenchmarkScenario.js";
import {
  balancedIncrementalDescriptor,
  balancedIncrementalSourceOnlyDescriptor,
} from "../../src/fixtures/recipes/balancedIncremental.js";
import { incrementalSynchronizationScenario } from "../../src/scenarios/incrementalSynchronization.js";
import { assertExternalFixtureSourceOutsideDirectory } from "../../src/fixtures/FixtureRecipe.js";

describe("benchmark resolution", () => {
  it("resolves the scenario's declared default fixture", () => {
    const resolved = resolveBenchmarkRun();
    expect(resolved.scenario.id).to.equal("incremental-synchronization");
    expect(resolved.descriptor.id).to.equal(
      incrementalSynchronizationScenario.defaultFixtureId
    );
  });

  it("lets an explicit fixture id override the default", () => {
    const scenario: BenchmarkScenarioDefinition = {
      ...incrementalSynchronizationScenario,
      capabilities: { topology: "source-only" },
    };
    expect(() =>
      assertScenarioSupportsFixture(
        scenario,
        balancedIncrementalSourceOnlyDescriptor
      )
    ).to.not.throw();
  });

  it("rejects a fixture whose topology the scenario cannot consume", () => {
    expect(() =>
      assertScenarioSupportsFixture(
        incrementalSynchronizationScenario,
        balancedIncrementalSourceOnlyDescriptor
      )
    ).to.throw(/requires a "source-and-empty-target" fixture/);
  });

  it("rejects a fixture that does not make a required claim", () => {
    const scenario: BenchmarkScenarioDefinition = {
      ...incrementalSynchronizationScenario,
      capabilities: {
        topology: "source-and-empty-target",
        requiredClaims: ["time travel"],
      },
    };
    expect(() =>
      assertScenarioSupportsFixture(scenario, balancedIncrementalDescriptor)
    ).to.throw(/does not claim \[time travel\]/);
  });

  it("reports unknown fixture ids with the available set", () => {
    expect(() =>
      resolveBenchmarkRun("incremental-synchronization", "no-such-fixture")
    ).to.throw(/Available fixtures: balanced-incremental/);
  });

  it("treats blank environment inputs as unspecified", () => {
    const resolved = resolveBenchmarkRunFromEnvironment({
      QUICK_PERF_SCENARIO: "",
      QUICK_PERF_FIXTURE: "  ",
    });
    expect(resolved.scenario.id).to.equal("incremental-synchronization");
    expect(resolved.descriptor.id).to.equal("balanced-incremental");
  });

  it("binds external BIM identity only to the standalone topology", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-external-unit-"));
    const external = path.join(root, "input.bim");
    fs.writeFileSync(external, "external fixture bytes");
    try {
      const resolved = resolveBenchmarkRun(
        "standalone-full-transformation",
        undefined,
        external
      );
      expect(resolved.descriptor.source).to.deep.include({
        kind: "external-bim",
        fileName: "input.bim",
        byteLength: fs.statSync(external).size,
      });
      expect(resolved.fixture.externalSourceFileName).to.equal(
        path.resolve(external)
      );
      expect(() =>
        assertExternalFixtureSourceOutsideDirectory(resolved.fixture, root)
      ).to.throw(/outside benchmark-managed directories/);
      expect(resolved.descriptor.recipeHash).to.not.equal(
        resolveBenchmarkRun("standalone-full-transformation").descriptor
          .recipeHash
      );
      expect(() =>
        resolveBenchmarkRun("incremental-synchronization", undefined, external)
      ).to.throw(/requires a "standalone-source-and-empty-target" fixture/);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects missing, non-file, and non-BIM external inputs", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-external-unit-"));
    const text = path.join(root, "input.txt");
    fs.writeFileSync(text, "not a BIM");
    try {
      for (const input of [path.join(root, "missing.bim"), root, text])
        expect(() =>
          resolveBenchmarkRun(
            "standalone-full-transformation",
            undefined,
            input
          )
        ).to.throw(/QUICK_PERF_STANDALONE_BIM/);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("canonicalizes symlinked external inputs before managed-path checks", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-external-link-"));
    const managed = path.join(root, "managed");
    const alias = path.join(root, "alias");
    fs.mkdirSync(managed);
    const external = path.join(managed, "input.bim");
    fs.writeFileSync(external, "external fixture bytes");
    fs.symlinkSync(
      managed,
      alias,
      process.platform === "win32" ? "junction" : "dir"
    );
    try {
      const resolved = resolveBenchmarkRun(
        "standalone-full-transformation",
        undefined,
        path.join(alias, "input.bim")
      );
      expect(() =>
        assertExternalFixtureSourceOutsideDirectory(resolved.fixture, managed)
      ).to.throw(/outside benchmark-managed directories/);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects a managed-path alias to an external BIM before cleanup", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-managed-link-"));
    const managed = path.join(root, "managed");
    const externalDir = path.join(root, "external");
    const alias = path.join(managed, "alias");
    fs.mkdirSync(managed);
    fs.mkdirSync(externalDir);
    const external = path.join(externalDir, "input.bim");
    fs.writeFileSync(external, "external fixture bytes");
    fs.symlinkSync(
      externalDir,
      alias,
      process.platform === "win32" ? "junction" : "dir"
    );
    try {
      const resolved = resolveBenchmarkRun(
        "standalone-full-transformation",
        undefined,
        path.join(alias, "input.bim")
      );
      expect(() =>
        assertExternalFixtureSourceOutsideDirectory(resolved.fixture, managed)
      ).to.throw(/outside benchmark-managed directories/);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("resolves an explicit measured-sample count from the environment", () => {
    expect(
      resolveMeasuredSamplesFromEnvironment({
        QUICK_PERF_SAMPLES: " 8 ",
      })
    ).to.equal(8);
  });

  it.each(["0", "-1", "1.5", "eight", "01", "9007199254740992"])(
    "rejects invalid measured-sample count %s",
    (value) => {
      expect(() => resolveMeasuredSamples(value)).to.throw(
        /QUICK_PERF_SAMPLES must be/
      );
    }
  );

  it("defaults blank measured-sample counts to one", () => {
    expect(resolveMeasuredSamples("  ")).to.equal(
      defaultQuickPerformanceMeasuredSamples
    );
  });
});
