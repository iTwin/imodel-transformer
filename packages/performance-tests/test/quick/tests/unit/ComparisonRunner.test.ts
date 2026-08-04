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
  assertCalibrationMatchesPair,
  assertComparisonFingerprintMatches,
  comparisonExecutionsPerPair,
  comparisonMeasuredSamples,
  comparisonWarmups,
  fingerprintForArtifact,
  hashFixtureArtifact,
  PairRunArtifact,
  readCalibrationArtifact,
  readPairArtifact,
  runPair,
  spawnArmProcess,
  validateCalibrationArtifact,
  validatePairRunArtifact,
  writeJson,
} from "../../src/comparison/ComparisonRunner";
import {
  comparisonFixtureIdentityFileName,
  comparisonFixtureIdentityVersion,
  readComparisonFixtureIdentity,
  validateComparisonFixtureIdentity,
} from "../../src/comparison/ComparisonFixtureIdentity";
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
    fs.writeFileSync(
      path.join(fixtureDirectory, comparisonFixtureIdentityFileName),
      `${JSON.stringify({
        artifactVersion: comparisonFixtureIdentityVersion,
        contentDigest: "4".repeat(64),
        baseSemanticDigest: "5".repeat(64),
        changesetSemanticDigest: "6".repeat(64),
      })}\n`
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
        transformerPackageHash: "1".repeat(64),
        coreBackendVersion: "5.10.3",
        coreBackendPackageHash: "2".repeat(64),
      },
      fingerprint: request.fingerprint,
      fixtureArtifactHash: request.fixtureArtifactHash,
      fixtureIdentity: request.fixtureIdentity,
      samples: [
        {
          sample: 0,
          measured: false,
          wallMilliseconds: 999,
          semanticDigest: "3".repeat(64),
          reconstructionMilliseconds: 1,
          verificationMilliseconds: 1,
          teardownMilliseconds: 1,
        },
        ...measured.map((wallMilliseconds, index) => ({
          sample: index + 1,
          measured: true,
          wallMilliseconds,
          semanticDigest: "3".repeat(64),
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

  function armRequest(outputDirectory: string): ArmRunRequest {
    return {
      arm: {
        id: "timeout-arm",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      source: { ref: "timeout", sha: "a".repeat(40) },
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      fixtureArtifactHash: hashFixtureArtifact(fixtureDirectory),
      fixtureIdentity: readComparisonFixtureIdentity(fixtureDirectory),
      fingerprint: fingerprintForArtifact(fixtureDirectory),
      outputDirectory,
    };
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

  it("escalates a timed-out child and settles only after it exits", async () => {
    const marker = path.join(root, "timeout-child.txt");
    const childScript = path.join(root, "ignore-term.cjs");
    fs.writeFileSync(
      childScript,
      [
        'const fs = require("node:fs");',
        `const marker = ${JSON.stringify(marker)};`,
        "fs.writeFileSync(marker, String(process.pid));",
        'process.on("SIGTERM", () => fs.appendFileSync(marker, "\\nSIGTERM"));',
        "setInterval(() => {}, 1_000);",
      ].join("\n")
    );
    const startedAt = Date.now();
    let timeoutMessage: string | undefined;
    try {
      await spawnArmProcess(
        childScript,
        armRequest(path.join(root, "timeout-output")),
        500,
        150
      );
    } catch (error) {
      timeoutMessage = error instanceof Error ? error.message : String(error);
    }
    expect(timeoutMessage).to.contain("timed out");
    expect(Date.now() - startedAt).to.be.lessThan(5_000);
    const [rawPid, signal] = fs.readFileSync(marker, "utf8").split("\n");
    const pid = Number(rawPid);
    let alive = true;
    try {
      process.kill(pid, 0);
    } catch {
      alive = false;
    }
    expect(alive).to.equal(false);
    if (process.platform !== "win32") {
      expect(signal).to.equal("SIGTERM");
      expect(timeoutMessage).to.contain("required forced termination");
    }
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
    expect(() =>
      assertComparisonFingerprintMatches(
        {
          ...fingerprintForArtifact(fixtureDirectory),
          harnessHash: "0".repeat(64),
        },
        fingerprintForArtifact(fixtureDirectory)
      )
    ).to.throw("does not match the current implementation");
    expect(hashFixtureArtifact(fixtureDirectory)).to.have.length(64);
  });

  it("aggregates a complete three-job A/A plan as provisional by default", async () => {
    const artifacts = await Promise.all([
      validPair(0, "AB", [100, 101, 102], [101, 102, 103]),
      validPair(1, "BA", [100, 101, 102], [99, 100, 101]),
      validPair(2, "AB", [100, 101, 102], [100, 101, 102]),
    ]);
    const calibration = aggregateCalibration(artifacts);
    expect(calibration.pool.independentJobs).to.equal(3);
    expect(calibration.pool.observations).to.have.length(3);
    expect(calibration.band.status).to.equal("provisional");
    expect(calibration.orders).to.deep.equal(["AB", "BA", "AB"]);
    expect(calibration.jobs.map((job) => job.pair)).to.deep.equal([0, 1, 2]);

    const established = aggregateCalibration(artifacts, {
      expectedPairs: 3,
      requirements: {
        provisionalIndependentJobs: 1,
        provisionalObservations: 1,
        establishedIndependentJobs: 3,
        establishedObservations: 3,
      },
    });
    expect(established.band.status).to.equal("established");

    expect(() =>
      aggregateCalibration([
        artifacts[0],
        { ...artifacts[1], order: "AB" },
        artifacts[2],
      ])
    ).to.throw("does not match execution plan BA");
    expect(() =>
      aggregateCalibration([artifacts[0], artifacts[2]], { expectedPairs: 2 })
    ).to.throw("expected 1, received 2");
    expect(() =>
      aggregateCalibration([artifacts[0], artifacts[0], artifacts[2]])
    ).to.throw("expected 1, received 0");
    const pairFour = await validPair(4, "AB");
    expect(() =>
      aggregateCalibration([artifacts[0], artifacts[2], pairFour])
    ).to.throw("expected 1, received 2");

    const secondArmA = artifacts[1].armA;
    const secondArmB = artifacts[1].armB;
    if (!secondArmA || !secondArmB)
      throw new Error("Expected a valid pair artifact");
    const differentFixtureIdentity = {
      ...artifacts[1].fixtureIdentity,
      contentDigest: "6".repeat(64),
    };
    expect(() =>
      aggregateCalibration([
        artifacts[0],
        {
          ...artifacts[1],
          fixtureIdentity: differentFixtureIdentity,
          armA: {
            ...secondArmA,
            fixtureIdentity: differentFixtureIdentity,
          },
          armB: {
            ...secondArmB,
            fixtureIdentity: differentFixtureIdentity,
          },
        },
        artifacts[2],
      ])
    ).to.throw("used different fixture content");

    const differentSemanticDigest = "7".repeat(64);
    expect(() =>
      aggregateCalibration([
        artifacts[0],
        {
          ...artifacts[1],
          semanticDigest: differentSemanticDigest,
          armA: {
            ...secondArmA,
            samples: secondArmA.samples.map((sample) => ({
              ...sample,
              semanticDigest: differentSemanticDigest,
            })),
          },
          armB: {
            ...secondArmB,
            samples: secondArmB.samples.map((sample) => ({
              ...sample,
              semanticDigest: differentSemanticDigest,
            })),
          },
        },
        artifacts[2],
      ])
    ).to.throw("different semantic result digests");
  });

  it("retains discarded calibration jobs while pooling only valid observations", async () => {
    const artifacts = await Promise.all([
      validPair(0, "AB", [100, 101, 102], [101, 102, 103]),
      validPair(1, "BA", [100, 101, 102], [99, 100, 101]),
      validPair(2, "AB", [100, 101, 102], [100, 101, 102]),
    ]);
    const discarded: PairRunArtifact = {
      ...artifacts[1],
      observation: undefined,
      collapsed: undefined,
      semanticDigest: undefined,
      discardedReason: "child crashed",
    };
    const calibration = aggregateCalibration(
      [artifacts[0], discarded, artifacts[2]],
      { expectedPairs: 3 }
    );
    expect(calibration.jobs).to.have.length(3);
    expect(calibration.jobs[1].discardedReason).to.equal("child crashed");
    expect(calibration.pool.independentJobs).to.equal(2);
    expect(calibration.pool.observations).to.have.length(2);
    expect(calibration.band.status).to.equal("provisional");
  });

  it("rejects tampered provenance, malformed artifacts, and non-finite JSON", async () => {
    const artifacts = await Promise.all([
      validPair(0, "AB", [100, 101, 102], [101, 102, 103]),
      validPair(1, "BA", [100, 101, 102], [99, 100, 101]),
      validPair(2, "AB", [100, 101, 102], [100, 101, 102]),
    ]);
    const calibration = aggregateCalibration(artifacts);
    const armA = artifacts[0].armA;
    const armB = artifacts[0].armB;
    if (!armA || !armB) throw new Error("Expected a valid pair artifact");
    const mismatchedFixtureIdentity = {
      ...artifacts[0].fixtureIdentity,
      contentDigest: "8".repeat(64),
    };
    expect(() =>
      assertCalibrationMatchesPair(
        {
          ...artifacts[0],
          fixtureIdentity: mismatchedFixtureIdentity,
          armA: {
            ...armA,
            fixtureIdentity: mismatchedFixtureIdentity,
          },
          armB: {
            ...armB,
            fixtureIdentity: mismatchedFixtureIdentity,
          },
        },
        calibration
      )
    ).to.throw("fixture content identity does not match calibration");
    expect(() =>
      assertCalibrationMatchesPair(
        {
          ...artifacts[0],
          semanticDigest: "9".repeat(64),
          armA: {
            ...armA,
            samples: armA.samples.map((sample) => ({
              ...sample,
              semanticDigest: "9".repeat(64),
            })),
          },
          armB: {
            ...armB,
            samples: armB.samples.map((sample) => ({
              ...sample,
              semanticDigest: "9".repeat(64),
            })),
          },
        },
        calibration
      )
    ).to.throw("semantic result digest does not match calibration");
    expect(() =>
      validateCalibrationArtifact({
        ...calibration,
        band: {
          ...calibration.band,
          derivation: {
            ...calibration.band.derivation,
            poolDigest: "0".repeat(64),
          },
        },
      })
    ).to.throw("not derived from the supplied pool");
    expect(() =>
      validateComparisonFixtureIdentity({
        ...artifacts[0].fixtureIdentity,
        artifactVersion: comparisonFixtureIdentityVersion - 1,
      })
    ).to.throw("Unsupported comparison fixture identity version");

    const pairPath = path.join(root, "pair.json");
    fs.writeFileSync(
      pairPath,
      JSON.stringify({ ...artifacts[0], order: "BA" })
    );
    expect(() => readPairArtifact(pairPath)).to.throw(
      "does not match execution plan AB"
    );
    fs.writeFileSync(
      pairPath,
      JSON.stringify({ ...artifacts[0], artifactVersion: undefined })
    );
    expect(() => readPairArtifact(pairPath)).to.throw(
      "Unsupported pair artifact version"
    );
    const observation = artifacts[0].observation;
    if (!observation) throw new Error("Expected a valid pair artifact");
    expect(() =>
      validatePairRunArtifact({
        ...artifacts[0],
        observation: {
          ...observation,
          armASamples: [1, 2, 3],
        },
      })
    ).to.throw("do not match the raw measured samples");
    expect(() =>
      validatePairRunArtifact({
        ...artifacts[0],
        armB: {
          ...armB,
          runtime: {
            ...armB.runtime,
            coreBackendPackageHash: "3".repeat(64),
          },
        },
      })
    ).to.throw("core-backend package");

    const calibrationPath = path.join(root, "calibration.json");
    fs.writeFileSync(
      calibrationPath,
      JSON.stringify({
        ...calibration,
        pool: { ...calibration.pool, independentJobs: 2 },
      })
    );
    expect(() => readCalibrationArtifact(calibrationPath)).to.throw(
      "does not match the valid independent job observations"
    );
    fs.writeFileSync(
      calibrationPath,
      JSON.stringify({ ...calibration, artifactVersion: undefined })
    );
    expect(() => readCalibrationArtifact(calibrationPath)).to.throw(
      "Unsupported calibration artifact version"
    );
    expect(() =>
      writeJson(path.join(root, "non-finite.json"), { value: Number.NaN })
    ).to.throw("non-finite");
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
