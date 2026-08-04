/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  ArmExecutionRequest,
  comparisonArmWorkerPath,
  createExecutionSchedule,
  executeArmProcess,
  runComparison,
} from "../../src/comparison/ComparisonRunner.js";
import { benchmarkSample } from "./comparisonTestUtils.js";

describe("A/B comparison orchestration", () => {
  const temporaryDirectories: string[] = [];

  afterEach(() => {
    for (const directory of temporaryDirectories)
      fs.rmSync(directory, { recursive: true, force: true });
    temporaryDirectories.length = 0;
  });

  function temporaryDirectory(prefix: string): string {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
    temporaryDirectories.push(directory);
    return directory;
  }

  it("schedules one warm-up and three measured executions per arm in balanced order", () => {
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

  it("passes identical scenario and fixture configuration to isolated arms", async () => {
    const outputDir = temporaryDirectory("quick-ab-runner-");
    const requests: ArmExecutionRequest[] = [];
    const fixtureBuilds: string[] = [];
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
        fixtureBuilds.push(request.outputDir);
        return {
          contentHash:
            "1111111111111111111111111111111111111111111111111111111111111111",
        };
      }
    );

    expect(fixtureBuilds).to.deep.equal([
      path.join(outputDir, "shared-fixture"),
    ]);
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
    ).to.deep.equal(new Set(fixtureBuilds));
    expect(
      requests.filter((request) => request.rootDirectory === "baseline-root")
    ).to.have.length(4);
    expect(
      requests.filter((request) => request.rootDirectory === "candidate-root")
    ).to.have.length(4);
    expect(summary.baseline.measuredMilliseconds).to.deep.equal([
      101, 102, 103,
    ]);
    expect(summary.candidate.measuredMilliseconds).to.deep.equal([
      106, 107, 108,
    ]);
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
      fixtureArtifactDirectory: path.join(rootDirectory, "fixture"),
      harnessRootDirectory: path.join(rootDirectory, "harness"),
      measured: true,
      outputDir: path.join(rootDirectory, "output"),
      revision: "base-sha",
      rootDirectory,
      sample: 1,
      scenarioId: "changeset-scanning",
    });
    expect(sample).to.deep.equal(expected);
  });

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
        fixtureArtifactDirectory: path.join(rootDirectory, "fixture"),
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
    const buildFixture = async () => ({
      contentHash:
        "1111111111111111111111111111111111111111111111111111111111111111",
    });
    await runComparison(
      options,
      async (request) =>
        benchmarkSample({
          measured: request.measured,
          sample: request.sample,
        }),
      buildFixture
    );
    await expect(
      runComparison(
        options,
        async () => {
          throw new Error("intentional worker failure");
        },
        buildFixture
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
});
