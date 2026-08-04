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
  ArmRunRequest,
  ArmRunResult,
  assertComparisonFingerprintMatches,
  comparisonArtifactVersion,
  comparisonExecutionsPerPair,
  comparisonMeasuredSamples,
  comparisonWarmups,
  fingerprintForArtifact,
  hashFixtureArtifact,
  PairRunArtifact,
  readPairArtifact,
  renderPairSummary,
  runPair,
  spawnArmProcess,
  validatePairRunArtifact,
} from "../../src/comparison/ComparisonRunner";
import {
  comparisonFixtureIdentityFileName,
  comparisonFixtureIdentityVersion,
  readComparisonFixtureIdentity,
} from "../../src/comparison/ComparisonFixtureIdentity";
import {
  artifactBriefcaseFileName,
  artifactChangesetDirectoryName,
  artifactChangesetPropsFileName,
  artifactManifestFileName,
  fixtureArtifactVersion,
} from "../../src/fixtures/FixtureArtifact";
import { updateHeavyScanFixture } from "../../src/fixtures/recipes/updateHeavyScan";

const testRequire = createRequire(import.meta.url);

describe("isolated A/B comparison runner", () => {
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
    measured: readonly number[],
    semanticDigest = "3".repeat(64)
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
          semanticDigest,
          reconstructionMilliseconds: 1,
          verificationMilliseconds: 1,
          teardownMilliseconds: 1,
        },
        ...measured.map((wallMilliseconds, index) => ({
          sample: index + 1,
          measured: true,
          wallMilliseconds,
          semanticDigest,
          reconstructionMilliseconds: 1,
          verificationMilliseconds: 1,
          teardownMilliseconds: 1,
        })),
      ],
      generatedAt: new Date(0).toISOString(),
    };
  }

  async function validPair(
    order: "AB" | "BA" = "AB",
    armATimes: readonly number[] = [12, 10, 11],
    armBTimes: readonly number[] = [9, 10, 8]
  ): Promise<PairRunArtifact> {
    return runPair({
      jobId: "job-0",
      pair: 0,
      order,
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "main", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "pair"),
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

  it("executes one warm-up plus three measurements per arm and median-collapses the pair", async () => {
    const requests: ArmRunRequest[] = [];
    const artifact = await runPair({
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
      armASource: { ref: "main", sha: "a".repeat(40) },
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
          request.arm.id === "A" ? [12, 10, 11] : [9, 10, 8]
        );
      },
    });

    expect(requests.map((request) => request.arm.id)).toEqual(["A", "B"]);
    expect(artifact.armA?.samples).toHaveLength(
      comparisonWarmups + comparisonMeasuredSamples
    );
    expect(artifact.armB?.samples).toHaveLength(
      comparisonWarmups + comparisonMeasuredSamples
    );
    expect(
      (artifact.armA?.samples.length ?? 0) +
        (artifact.armB?.samples.length ?? 0)
    ).toBe(comparisonExecutionsPerPair);
    expect(artifact.summary?.armAMedianMilliseconds).toBe(11);
    expect(artifact.summary?.armBMedianMilliseconds).toBe(9);
    expect(artifact.summary?.percentDelta).toBeCloseTo(-18.1818, 3);
    expect(artifact.summary?.classification).toBe("candidate-faster");
    expect(artifact.discardedReason).toBeUndefined();
  });

  it("honors BA order while retaining baseline as arm A", async () => {
    const order: string[] = [];
    const artifact = await runPair({
      jobId: "job",
      pair: 0,
      order: "BA",
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "main", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "pair"),
      launcher: async (request) => {
        order.push(request.arm.id);
        return armResult(
          request,
          request.arm.id === "A" ? [10, 11, 12] : [9, 10, 8]
        );
      },
    });
    expect(order).toEqual(["B", "A"]);
    expect(artifact.order).toBe("BA");
    expect(artifact.summary?.armAMedianMilliseconds).toBe(11);
    expect(artifact.summary?.armBMedianMilliseconds).toBe(9);
  });

  it("uses the informational threshold without making a statistical claim", async () => {
    const within = await validPair("AB", [100, 100, 100], [109, 109, 109]);
    const slower = await validPair("AB", [100, 100, 100], [111, 111, 111]);
    expect(within.summary?.classification).toBe("within-threshold");
    expect(slower.summary?.classification).toBe("candidate-slower");
    expect(renderPairSummary(within)).toContain(
      "not a statistical confidence claim"
    );
  });

  it("discards the whole pair when either child fails or returns malformed output", async () => {
    const executed: string[] = [];
    const failed = await runPair({
      jobId: "failure",
      pair: 0,
      order: "AB",
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "main", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "failure"),
      launcher: async (request) => {
        executed.push(request.arm.id);
        if (request.arm.id === "B") throw new Error("child crashed");
        return armResult(request, [1, 2, 3]);
      },
    });
    expect(executed).toEqual(["A", "B"]);
    expect(failed.discardedReason).toContain("child crashed");
    expect(failed.summary).toBeUndefined();

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
      armASource: { ref: "main", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "malformed"),
      launcher: async () => ({}),
    });
    expect(malformed.discardedReason).toContain("Arm result");
    expect(malformed.summary).toBeUndefined();
  });

  it("rejects fixture, fingerprint, and semantic mismatches", async () => {
    const fixtureMismatch = await runPair({
      jobId: "fixture-mismatch",
      pair: 0,
      order: "AB",
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "main", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "fixture-mismatch"),
      launcher: async (request) => ({
        ...armResult(request, [1, 2, 3]),
        fixtureArtifactHash: "0".repeat(64),
      }),
    });
    expect(fixtureMismatch.discardedReason).toContain(
      "fixture artifact hash does not match"
    );

    const semanticMismatch = await runPair({
      jobId: "semantic-mismatch",
      pair: 0,
      order: "AB",
      scenarioId: "changeset-scanning",
      fixtureDirectory,
      armA: {
        id: "A",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "main", sha: "a".repeat(40) },
      armB: {
        id: "B",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "candidate", sha: "b".repeat(40) },
      outputDirectory: path.join(root, "semantic-mismatch"),
      launcher: async (request) =>
        armResult(
          request,
          [1, 2, 3],
          request.arm.id === "A" ? "3".repeat(64) : "7".repeat(64)
        ),
    });
    expect(semanticMismatch.discardedReason).toContain(
      "different semantic results"
    );

    expect(() =>
      assertComparisonFingerprintMatches(
        { ...fingerprintForArtifact(fixtureDirectory), fixtureId: "wrong" },
        fingerprintForArtifact(fixtureDirectory)
      )
    ).toThrow("Comparison fingerprint does not match");
  });

  it("loads ChangedInstanceIds from the explicit arm package", async () => {
    const selected = createArmPackage("selected-baseline", 42);
    const loaded = loadArmModule(
      resolveArmSpec({
        id: "selected",
        packageRoot: selected,
        operation: "change-processing",
      })
    );
    expect(await loaded.changedInstanceIds.initialize({} as never)).toBe(42);
    expect(loaded.runtime.transformerVersion).toBe("1.0.42");
  });

  it("escalates timeout termination and settles after the child exits", async () => {
    const marker = path.join(root, "timeout-marker.txt");
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
    expect(timeoutMessage).toContain("timed out");
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    const [pidText, signal] = fs.readFileSync(marker, "utf8").split("\n");
    let alive = true;
    try {
      process.kill(Number(pidText), 0);
    } catch {
      alive = false;
    }
    expect(alive).toBe(false);
    if (process.platform !== "win32") {
      expect(signal).toBe("SIGTERM");
      expect(timeoutMessage).toContain("required forced termination");
    }
  });

  it("strictly validates persisted pair artifacts", async () => {
    const artifact = await validPair();
    const pairPath = path.join(root, "pair.json");
    fs.writeFileSync(pairPath, `${JSON.stringify(artifact)}\n`);
    expect(readPairArtifact(pairPath).summary?.classification).toBe(
      "candidate-faster"
    );
    expect(() =>
      validatePairRunArtifact({
        ...artifact,
        artifactVersion: comparisonArtifactVersion - 1,
      })
    ).toThrow("Unsupported pair artifact version");
    expect(() =>
      validatePairRunArtifact({
        ...artifact,
        summary: artifact.summary
          ? { ...artifact.summary, percentDelta: 0 }
          : undefined,
      })
    ).toThrow("summary does not match measured arm samples");
    expect(() =>
      validatePairRunArtifact({
        ...artifact,
        summary: artifact.summary
          ? {
              ...artifact.summary,
              armAMedianMilliseconds: Number.POSITIVE_INFINITY,
            }
          : undefined,
      })
    ).toThrow("non-finite");
  });
});
