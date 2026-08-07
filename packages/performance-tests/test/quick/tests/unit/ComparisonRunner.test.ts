/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { ChildProcess } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ArmExecutionRequest,
  comparisonArmWorkerPath,
  createExecutionSchedule,
  executeArmProcess,
  FixtureArtifactBuildRequest,
  runComparison,
} from "../../src/comparison/ComparisonRunner.js";
import { resolveTransformerProvenance } from "../../src/comparison/TransformerProvenance.js";
import {
  benchmarkSample,
  fixtureArtifactManifest,
} from "./comparisonTestUtils.js";

describe("A/B comparison orchestration", () => {
  const temporaryDirectories: string[] = [];

  afterEach(() => {
    vi.restoreAllMocks();
    for (const directory of temporaryDirectories)
      fs.rmSync(directory, { recursive: true, force: true });
    temporaryDirectories.length = 0;
  });

  function temporaryDirectory(prefix: string): string {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
    temporaryDirectories.push(directory);
    return directory;
  }

  async function waitForFile(fileName: string): Promise<void> {
    const deadline = Date.now() + 2_000;
    while (!fs.existsSync(fileName)) {
      if (Date.now() >= deadline)
        throw new Error(`Worker did not signal readiness: ${fileName}`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  it("schedules one warm-up and three measured executions per arm in alternating order", () => {
    const schedule = createExecutionSchedule(3);
    expect(schedule).to.deep.equal([
      { arm: "candidate", measured: false, sample: 0 },
      { arm: "baseline", measured: false, sample: 0 },
      { arm: "baseline", measured: true, sample: 1 },
      { arm: "candidate", measured: true, sample: 1 },
      { arm: "candidate", measured: true, sample: 2 },
      { arm: "baseline", measured: true, sample: 2 },
      { arm: "baseline", measured: true, sample: 3 },
      { arm: "candidate", measured: true, sample: 3 },
    ]);
    expect(
      schedule.filter((execution) => execution.arm === "baseline")
    ).to.have.length(4);
    expect(
      schedule.filter((execution) => execution.arm === "candidate")
    ).to.have.length(4);
  });

  it("authors one baseline fixture artifact reused by every isolated arm", async () => {
    const outputDir = temporaryDirectory("quick-ab-runner-");
    const requests: ArmExecutionRequest[] = [];
    const buildRequests: FixtureArtifactBuildRequest[] = [];
    const summary = await runComparison(
      {
        baseline: {
          revision: "base-sha",
          rootDirectory: "baseline-root",
        },
        candidate: {
          revision: "candidate-sha",
          rootDirectory: "candidate-root",
        },
        fixtureId: "update-heavy-scan",
        measuredSamplesPerArm: 3,
        outputDir,
        scenarioId: "changeset-scanning",
        workerTimeoutMilliseconds: 1_234,
      },
      async (request) => {
        requests.push(request);
        return benchmarkSample({
          measured: request.measured,
          sample: request.sample,
          wallMilliseconds:
            (request.arm === "baseline" ? 100 : 105) + request.sample,
        });
      },
      async (request) => {
        buildRequests.push(request);
        return fixtureArtifactManifest();
      }
    );

    expect(buildRequests).to.have.length(1);
    expect(buildRequests[0].rootDirectory).to.equal("baseline-root");
    expect(buildRequests[0].artifactDirectory).to.equal(
      path.join(outputDir, "fixture-artifact")
    );
    expect(buildRequests[0].workerTimeoutMilliseconds).to.equal(1_234);
    expect(
      new Set(requests.map((request) => request.fixtureArtifactDirectory))
    ).to.deep.equal(new Set([buildRequests[0].artifactDirectory]));
    expect(
      new Set(requests.map((request) => request.scenarioId))
    ).to.deep.equal(new Set(["changeset-scanning"]));
    expect(new Set(requests.map((request) => request.fixtureId))).to.deep.equal(
      new Set(["update-heavy-scan"])
    );
    expect(
      new Set(requests.map((request) => request.harnessRootDirectory))
    ).to.deep.equal(
      new Set([
        path.join(
          "candidate-root",
          "packages",
          "performance-tests",
          "test",
          "quick"
        ),
      ])
    );
    expect(
      new Set(requests.map((request) => request.fixtureArtifactDirectory))
    ).to.deep.equal(new Set([buildRequests[0].artifactDirectory]));
    expect(
      requests.filter((request) => request.rootDirectory === "baseline-root")
    ).to.have.length(4);
    expect(
      requests.filter((request) => request.rootDirectory === "candidate-root")
    ).to.have.length(4);
    expect(
      new Set(requests.map((request) => request.workerTimeoutMilliseconds))
    ).to.deep.equal(new Set([1_234]));
    expect(summary.baseline.measuredMilliseconds).to.deep.equal([
      101, 102, 103,
    ]);
    expect(summary.candidate.measuredMilliseconds).to.deep.equal([
      106, 107, 108,
    ]);
    expect(summary.fixtureAuthoring).to.deep.equal({
      arm: "baseline",
      revision: "base-sha",
      transformerVersion: "0.6.0",
    });
  });

  it("runs the arm worker as a child process and reads its result", async () => {
    const rootDirectory = temporaryDirectory("quick-ab-process-");
    const workerPath = comparisonArmWorkerPath(rootDirectory);
    fs.mkdirSync(path.dirname(workerPath), { recursive: true });
    const expected = benchmarkSample();
    fs.writeFileSync(
      workerPath,
      [
        'const fs = require("node:fs");',
        "const request = JSON.parse(process.env.QUICK_PERF_ARM_REQUEST);",
        "if (process.env.QUICK_PERF_HARNESS_ROOT !== request.harnessRootDirectory) process.exit(8);",
        `fs.writeFileSync(request.resultFile, ${JSON.stringify(
          `${JSON.stringify(expected)}\n`
        )});`,
      ].join("\n")
    );
    const sample = await executeArmProcess({
      arm: "baseline",
      fixtureArtifactDirectory: path.join(rootDirectory, "fixture-artifact"),
      harnessRootDirectory: path.join(rootDirectory, "harness"),
      measured: true,
      outputDir: path.join(rootDirectory, "output"),
      revision: "base-sha",
      rootDirectory,
      sample: 1,
      scenarioId: "changeset-scanning",
    });
    expect(sample).toMatchObject(expected);
    expect(sample.workerRssSamplingIntervalMilliseconds).to.equal(0);
  });

  it.skipIf(process.platform === "win32")(
    "samples peak worker RSS from outside the worker process",
    async () => {
      const rootDirectory = temporaryDirectory("quick-ab-memory-");
      const workerPath = comparisonArmWorkerPath(rootDirectory);
      fs.mkdirSync(path.dirname(workerPath), { recursive: true });
      const expected = benchmarkSample();
      fs.writeFileSync(
        workerPath,
        [
          'const fs = require("node:fs");',
          "const request = JSON.parse(process.env.QUICK_PERF_ARM_REQUEST);",
          "const allocation = Buffer.alloc(16 * 1024 * 1024, 1);",
          "setTimeout(() => {",
          `  fs.writeFileSync(request.resultFile, ${JSON.stringify(
            `${JSON.stringify(expected)}\n`
          )});`,
          "  if (allocation[0] !== 1) process.exit(9);",
          "}, 250);",
        ].join("\n")
      );
      const sample = await executeArmProcess({
        arm: "baseline",
        fixtureArtifactDirectory: path.join(rootDirectory, "fixture-artifact"),
        harnessRootDirectory: path.join(rootDirectory, "harness"),
        measured: true,
        outputDir: path.join(rootDirectory, "output"),
        revision: "base-sha",
        rootDirectory,
        sample: 1,
        scenarioId: "changeset-scanning",
        workerRssSamplingIntervalMilliseconds: 20,
      });
      expect(sample.workerPeakRssBytes).to.be.greaterThan(16 * 1024 * 1024);
      expect(sample.workerRssSampleCount).to.be.greaterThan(1);
      expect(sample.workerRssSamplingIntervalMilliseconds).to.equal(20);
    }
  );

  it("surfaces child-process failures with arm and sample context", async () => {
    const rootDirectory = temporaryDirectory("quick-ab-failure-");
    const workerPath = comparisonArmWorkerPath(rootDirectory);
    fs.mkdirSync(path.dirname(workerPath), { recursive: true });
    fs.writeFileSync(
      workerPath,
      'process.stderr.write("worker failed"); process.exitCode = 7;\n'
    );
    await expect(
      executeArmProcess({
        arm: "candidate",
        fixtureArtifactDirectory: path.join(rootDirectory, "fixture-artifact"),
        harnessRootDirectory: path.join(rootDirectory, "harness"),
        measured: true,
        outputDir: path.join(rootDirectory, "output"),
        revision: "candidate-sha",
        rootDirectory,
        sample: 2,
      })
    ).rejects.toThrow(
      /candidate sample 2 process failed with exit code 7: worker failed/
    );
  });

  it("waits for a timed-out worker to terminate before rejecting", async () => {
    const rootDirectory = temporaryDirectory("quick-ab-timeout-");
    const workerPath = comparisonArmWorkerPath(rootDirectory);
    const pidFile = path.join(rootDirectory, "worker.pid");
    fs.mkdirSync(path.dirname(workerPath), { recursive: true });
    fs.writeFileSync(
      workerPath,
      [
        'const fs = require("node:fs");',
        'process.on("SIGTERM", () => {});',
        `fs.writeFileSync(${JSON.stringify(pidFile)}, String(process.pid));`,
        "setInterval(() => {}, 1000);",
      ].join("\n")
    );
    const kill = vi.spyOn(ChildProcess.prototype, "kill");
    const started = Date.now();
    const execution = executeArmProcess(
      {
        arm: "candidate",
        fixtureArtifactDirectory: path.join(rootDirectory, "fixture-artifact"),
        harnessRootDirectory: path.join(rootDirectory, "harness"),
        measured: true,
        outputDir: path.join(rootDirectory, "output"),
        revision: "candidate-sha",
        rootDirectory,
        sample: 2,
        workerTimeoutMilliseconds: 1_000,
      },
      25,
      500
    );
    await waitForFile(pidFile);
    await expect(execution).rejects.toThrow(
      /candidate sample 2 process timed out after 1000 ms/
    );
    const workerPid = Number(fs.readFileSync(pidFile, "utf8"));
    expect(() => process.kill(workerPid, 0)).to.throw();
    const deliveredSignals = kill.mock.calls.map(([signal]) => signal);
    expect(deliveredSignals[0]).to.equal("SIGTERM");
    if (process.platform !== "win32")
      expect(deliveredSignals).to.deep.equal(["SIGTERM", "SIGKILL"]);
    expect(Date.now() - started).to.be.lessThan(2_000);
  });

  it.skipIf(process.platform === "win32")(
    "settles once when a worker exits during the termination grace period",
    async () => {
      const rootDirectory = temporaryDirectory("quick-ab-timeout-race-");
      const workerPath = comparisonArmWorkerPath(rootDirectory);
      const readyMarker = path.join(rootDirectory, "worker-ready");
      const exitMarker = path.join(rootDirectory, "worker-exited");
      fs.mkdirSync(path.dirname(workerPath), { recursive: true });
      fs.writeFileSync(
        workerPath,
        [
          'const fs = require("node:fs");',
          'process.on("SIGTERM", () => setTimeout(() => {',
          `  fs.writeFileSync(${JSON.stringify(exitMarker)}, "exiting");`,
          "  process.exit(0);",
          "}, 25));",
          `fs.writeFileSync(${JSON.stringify(readyMarker)}, "ready");`,
          "setInterval(() => {}, 1000);",
        ].join("\n")
      );
      const kill = vi.spyOn(ChildProcess.prototype, "kill");
      const execution = executeArmProcess(
        {
          arm: "baseline",
          fixtureArtifactDirectory: path.join(
            rootDirectory,
            "fixture-artifact"
          ),
          harnessRootDirectory: path.join(rootDirectory, "harness"),
          measured: true,
          outputDir: path.join(rootDirectory, "output"),
          revision: "base-sha",
          rootDirectory,
          sample: 1,
          workerTimeoutMilliseconds: 1_000,
        },
        500,
        500
      );
      await waitForFile(readyMarker);
      await expect(execution).rejects.toThrow(
        /baseline sample 1 process timed out after 1000 ms/
      );
      expect(fs.readFileSync(exitMarker, "utf8")).to.equal("exiting");
      await new Promise((resolve) => setTimeout(resolve, 550));
      expect(kill.mock.calls.map(([signal]) => signal)).to.deep.equal([
        "SIGTERM",
      ]);
    }
  );

  it("rejects an invalid worker timeout before starting fixture work", async () => {
    let buildStarted = false;
    await expect(
      runComparison(
        {
          baseline: {
            revision: "base-sha",
            rootDirectory: "baseline-root",
          },
          candidate: {
            revision: "candidate-sha",
            rootDirectory: "candidate-root",
          },
          outputDir: temporaryDirectory("quick-ab-invalid-timeout-"),
          workerTimeoutMilliseconds: 0,
        },
        async () => benchmarkSample(),
        async () => {
          buildStarted = true;
          return fixtureArtifactManifest();
        }
      )
    ).rejects.toThrow(/worker timeout must be an integer between 1 and/);
    expect(buildStarted).to.equal(false);
  });

  it("rejects an RSS interval above the Node timer limit", async () => {
    let buildStarted = false;
    await expect(
      runComparison(
        {
          baseline: {
            revision: "base-sha",
            rootDirectory: "baseline-root",
          },
          candidate: {
            revision: "candidate-sha",
            rootDirectory: "candidate-root",
          },
          outputDir: temporaryDirectory("quick-ab-invalid-rss-interval-"),
          workerRssSamplingIntervalMilliseconds: 2_147_483_648,
        },
        async () => benchmarkSample(),
        async () => {
          buildStarted = true;
          return fixtureArtifactManifest();
        }
      )
    ).rejects.toThrow(/RSS sampling interval must be an integer between 0 and/);
    expect(buildStarted).to.equal(false);
  });

  it("removes stale reports before a failed rerun", async () => {
    const outputDir = temporaryDirectory("quick-ab-stale-");
    const options = {
      baseline: {
        revision: "base-sha",
        rootDirectory: "baseline-root",
      },
      candidate: {
        revision: "candidate-sha",
        rootDirectory: "candidate-root",
      },
      outputDir,
    };
    await runComparison(
      options,
      async (request) =>
        benchmarkSample({
          measured: request.measured,
          sample: request.sample,
        }),
      async () => fixtureArtifactManifest()
    );
    await expect(
      runComparison(
        options,
        async () => {
          throw new Error("intentional worker failure");
        },
        async () => fixtureArtifactManifest()
      )
    ).rejects.toThrow(/intentional worker failure/);
    expect(fs.existsSync(path.join(outputDir, "comparison.json"))).to.equal(
      false
    );
    expect(fs.existsSync(path.join(outputDir, "comparison.md"))).to.equal(
      false
    );
    expect(
      fs.existsSync(path.join(outputDir, "comparison-samples.jsonl"))
    ).to.equal(false);
  });

  it("rejects a transformer resolved outside the expected arm checkout", () => {
    const expectedRoot = temporaryDirectory("quick-ab-transformer-");
    const packageDirectory = path.join(expectedRoot, "packages", "transformer");
    const expectedEntry = path.join(packageDirectory, "lib", "cjs", "index.js");
    fs.mkdirSync(path.dirname(expectedEntry), { recursive: true });
    fs.writeFileSync(
      path.join(packageDirectory, "package.json"),
      JSON.stringify({ main: "lib/cjs/index.js", version: "1.2.3" })
    );
    fs.writeFileSync(expectedEntry, "module.exports = {};\n");

    const initial = resolveTransformerProvenance(
      expectedRoot,
      () => expectedEntry
    );
    expect(initial).to.include({
      version: "1.2.3",
      entryPoint: fs.realpathSync(expectedEntry),
    });
    const implementation = path.join(
      packageDirectory,
      "lib",
      "cjs",
      "implementation.js"
    );
    fs.writeFileSync(implementation, "module.exports = 1;\n");
    const changed = resolveTransformerProvenance(
      expectedRoot,
      () => expectedEntry
    );
    expect(changed.contentHash).not.to.equal(initial.contentHash);

    const wrongEntry = path.join(
      temporaryDirectory("quick-ab-wrong-transformer-"),
      "index.js"
    );
    fs.writeFileSync(wrongEntry, "module.exports = {};\n");
    expect(() =>
      resolveTransformerProvenance(expectedRoot, () => wrongEntry)
    ).to.throw(/resolved .* expected/);
  });
});
