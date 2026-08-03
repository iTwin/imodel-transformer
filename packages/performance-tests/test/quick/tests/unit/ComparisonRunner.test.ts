/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { loadArmModule, resolveArmSpec } from "../../src/comparison/ArmModule";
import {
  aggregateCalibration,
  ArmRunRequest,
  ArmRunResult,
  assertComparisonFingerprintMatches,
  comparisonExecutionsPerPair,
  comparisonMeasuredSamples,
  comparisonWarmups,
  fingerprintForArtifact,
  hashFixtureArtifact,
  PairRunArtifact,
  runPair,
} from "../../src/comparison/ComparisonRunner";
import {
  artifactBriefcaseFileName,
  artifactChangesetDirectoryName,
  artifactChangesetPropsFileName,
  artifactManifestFileName,
  fixtureArtifactVersion,
} from "../../src/fixtures/FixtureArtifact";
import { classifyCalibrationQuality } from "../../src/comparison/NoiseBand";
import { updateHeavyScanFixture } from "../../src/fixtures/recipes/updateHeavyScan";

const testRequire = createRequire(import.meta.url);

describe("quick comparison runner", () => {
  let root: string;
  let fixtureDirectory: string;
  let armPackage: string;

  beforeEach(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-comparison-unit-"));
    fixtureDirectory = path.join(root, "fixture");
    fs.mkdirSync(path.join(fixtureDirectory, artifactChangesetDirectoryName), {
      recursive: true,
    });
    fs.writeFileSync(
      path.join(fixtureDirectory, artifactBriefcaseFileName),
      ""
    );
    fs.writeFileSync(
      path.join(fixtureDirectory, artifactChangesetPropsFileName),
      "[]\n"
    );
    fs.writeFileSync(
      path.join(fixtureDirectory, artifactManifestFileName),
      `${JSON.stringify(
        {
          artifactVersion: fixtureArtifactVersion,
          descriptor: updateHeavyScanFixture.descriptor,
          briefcase: {
            fileName: artifactBriefcaseFileName,
            briefcaseId: 1,
            changeset: { id: "", index: 0 },
            byteLength: 0,
          },
          changesets: {
            directory: artifactChangesetDirectoryName,
            propsFile: artifactChangesetPropsFileName,
            count: 0,
            baseChangesetIndex: 0,
          },
          buildMilliseconds: 1,
          builtAt: new Date(0).toISOString(),
        },
        undefined,
        2
      )}\n`
    );
    armPackage = createArmPackage("explicit-arm", 17);
  });

  afterEach(() => {
    fs.rmSync(root, { recursive: true, force: true });
  });

  function createArmPackage(name: string, sentinel: number): string {
    const directory = path.join(root, name);
    fs.mkdirSync(directory);
    fs.writeFileSync(
      path.join(directory, "package.json"),
      `${JSON.stringify({
        name,
        version: `1.0.${sentinel}`,
        main: "index.js",
        peerDependencies: { "@itwin/core-backend": "^5.10.0" },
      })}\n`
    );
    fs.writeFileSync(
      path.join(directory, "index.js"),
      `exports.ChangedInstanceIds = { initialize: async () => ${sentinel} };\n`
    );
    const scope = path.join(directory, "node_modules", "@itwin");
    fs.mkdirSync(scope, { recursive: true });
    fs.symlinkSync(
      path.dirname(testRequire.resolve("@itwin/core-backend/package.json")),
      path.join(scope, "core-backend"),
      "junction"
    );
    return directory;
  }

  function armResult(
    request: ArmRunRequest,
    measured: readonly number[]
  ): ArmRunResult {
    return {
      arm: request.arm,
      source: request.source,
      runtime: {
        armId: request.arm.id,
        transformerVersion: "1.0.0",
        transformerPackageHash: "same-transformer",
        coreBackendVersion: "5.10.3",
        coreBackendPackageHash: "same-core",
      },
      fingerprint: request.fingerprint,
      fixtureArtifactHash: request.fixtureArtifactHash,
      samples: [
        {
          sample: 0,
          measured: false,
          wallMilliseconds: 999,
          semanticDigest: "same",
          reconstructionMilliseconds: 1,
          verificationMilliseconds: 1,
          teardownMilliseconds: 1,
        },
        ...measured.map((wallMilliseconds, index) => ({
          sample: index + 1,
          measured: true,
          wallMilliseconds,
          semanticDigest: "same",
          reconstructionMilliseconds: 1,
          verificationMilliseconds: 1,
          teardownMilliseconds: 1,
        })),
      ],
      generatedAt: new Date(0).toISOString(),
    };
  }

  async function validPair(
    pair: number,
    order: "AB" | "BA",
    armATimes: readonly number[] = [10, 12, 11],
    armBTimes: readonly number[] = [20, 22, 21]
  ): Promise<PairRunArtifact> {
    return runPair({
      jobId: `job-${pair}`,
      pair,
      order,
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "calibration", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "calibration", sha: "a".repeat(40) },
      outputDirectory: path.join(root, `pair-${pair}`),
      launcher: async (request) =>
        armResult(request, request.arm.id === "A" ? armATimes : armBTimes),
    });
  }

  it("executes one warm-up plus three measurements per arm and collapses one pair", async () => {
    const requests: ArmRunRequest[] = [];
    const pair = await runPair({
      jobId: "job",
      pair: 0,
      order: "AB",
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "base", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "pair"),
      launcher: async (request) => {
        requests.push(request);
        return armResult(
          request,
          request.arm.id === "A" ? [10, 100, 11] : [20, 21, 200]
        );
      },
    });
    expect(requests.map((request) => request.arm.id)).to.deep.equal(["A", "B"]);
    expect(
      requests.length * (comparisonWarmups + comparisonMeasuredSamples)
    ).to.equal(comparisonExecutionsPerPair);
    expect(pair.observation?.armASamples).to.deep.equal([10, 100, 11]);
    expect(pair.collapsed?.armA).to.equal(11);
    expect(pair.collapsed?.armB).to.equal(21);
  });

  it("executes BA in declared order without changing arm identity", async () => {
    const launched: string[] = [];
    const pair = await runPair({
      jobId: "job",
      pair: 1,
      order: "BA",
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "base", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "pair"),
      launcher: async (request) => {
        launched.push(request.arm.id);
        return armResult(
          request,
          request.arm.id === "A" ? [9, 10, 11] : [19, 20, 21]
        );
      },
    });
    expect(launched).to.deep.equal(["B", "A"]);
    expect(pair.observation?.order).to.equal("BA");
    expect(pair.collapsed?.armA).to.equal(10);
    expect(pair.collapsed?.armB).to.equal(20);
  });

  it("discards the whole pair on child failure or malformed output", async () => {
    let launches = 0;
    const failed = await runPair({
      jobId: "failed",
      pair: 0,
      order: "AB",
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "base", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "failed"),
      launcher: async () => {
        launches++;
        throw new Error("child crashed");
      },
    });
    expect(launches).to.equal(1);
    expect(failed.observation).to.be.undefined;
    expect(failed.discardedReason).to.contain("child crashed");

    const malformed = await runPair({
      jobId: "malformed",
      pair: 0,
      order: "AB",
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "base", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "malformed"),
      launcher: async () => ({ samples: [] }),
    });
    expect(malformed.observation).to.be.undefined;
    expect(malformed.discardedReason).to.contain("one warm-up and three");
  });

  it("rejects fixture and comparison fingerprint mismatches", async () => {
    const discarded = await runPair({
      jobId: "mismatch",
      pair: 0,
      order: "AB",
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "base", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "mismatch"),
      launcher: async (request) => ({
        ...armResult(request, [1, 2, 3]),
        fixtureArtifactHash: "wrong",
      }),
    });
    expect(discarded.discardedReason).to.contain("malformed identity");
    expect(() =>
      assertComparisonFingerprintMatches(
        { ...fingerprintForArtifact(fixtureDirectory), fixtureId: "wrong" },
        fingerprintForArtifact(fixtureDirectory)
      )
    ).to.throw("Comparison fingerprint does not match");
    expect(hashFixtureArtifact(fixtureDirectory)).to.have.length(64);
  });

  it("aggregates three independent A/A jobs and enforces job-level order", async () => {
    const artifacts = await Promise.all([
      validPair(0, "AB", [100, 101, 102], [101, 102, 103]),
      validPair(1, "BA", [100, 101, 102], [99, 100, 101]),
      validPair(2, "AB", [100, 101, 102], [100, 101, 102]),
    ]);
    const calibration = aggregateCalibration(artifacts);
    expect(calibration.pool.independentJobs).to.equal(3);
    expect(calibration.pool.observations).to.have.length(3);
    expect(calibration.band.status).to.equal("established");
    expect(calibration.orders).to.deep.equal(["AB", "BA", "AB"]);
    expect(() =>
      aggregateCalibration([
        artifacts[0],
        { ...artifacts[1], order: "AB" },
        artifacts[2],
      ])
    ).to.throw("requires BA");
  });

  it("uses the layer-3 5% and 10% calibration quality boundaries", () => {
    expect(classifyCalibrationQuality("established", 5)).to.equal("target");
    expect(classifyCalibrationQuality("established", 5.01)).to.equal(
      "marginal"
    );
    expect(classifyCalibrationQuality("established", 10)).to.equal(
      "unresolvable"
    );
  });

  it("loads ChangedInstanceIds from the explicit arm package", async () => {
    const baseline = createArmPackage("baseline-arm", 42);
    const candidate = createArmPackage("candidate-arm", 84);
    const loadedBaseline = loadArmModule(
      resolveArmSpec({
        id: "baseline",
        packageRoot: baseline,
        operation: "change-processing",
      })
    );
    const loadedCandidate = loadArmModule(
      resolveArmSpec({
        id: "candidate",
        packageRoot: candidate,
        operation: "change-processing",
      })
    );
    expect(
      await loadedBaseline.changedInstanceIds.initialize(undefined as never)
    ).to.equal(42);
    expect(
      await loadedCandidate.changedInstanceIds.initialize(undefined as never)
    ).to.equal(84);
    expect(loadedBaseline.runtime.transformerVersion).to.equal("1.0.42");
  });
});
