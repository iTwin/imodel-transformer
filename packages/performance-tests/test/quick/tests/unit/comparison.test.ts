/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  ArmRuntimeIdentity,
  assertArmRuntimeComparable,
  assertArmSpecsComparable,
  resolveArmSpec,
} from "../../src/comparison/ArmModule";
import { binomialPmf, twoSidedSignTestP } from "../../src/comparison/binomial";
import {
  buildComparisonReport,
  BuildComparisonReportOptions,
  renderComparisonReport,
  writeComparisonReport,
} from "../../src/comparison/ComparisonReport";
import { classifyEnvironment } from "../../src/comparison/EnvironmentClass";
import {
  assertExecutionFingerprintMatches,
  ExecutionFingerprint,
  executionFingerprintKey,
  validateExecutionFingerprint,
} from "../../src/comparison/ExecutionFingerprint";
import {
  aggregateLogRatios,
  bootstrapMedianInterval,
  collapseArmSamples,
  collapsePair,
  logRatio,
  logRatioToPercent,
  orderEffectLogRatios,
  percentToLogRatio,
} from "../../src/comparison/logRatio";
import {
  assertPoolApplies,
  CalibrationQuality,
  classifyBandStatus,
  classifyCalibrationQuality,
  deriveNoiseBand,
  medianNullThreshold,
  NoiseBand,
  NoiseBandKey,
  noiseBandKey,
  NoiseBandPool,
  targetNoiseBandPercent,
} from "../../src/comparison/NoiseBand";
import { SeededRandom } from "../../src/comparison/SeededRandom";
import {
  decideVerdict,
  defaultEquivalenceMarginPercent,
} from "../../src/comparison/verdict";

const execution: ExecutionFingerprint = {
  warmupSamplesPerArm: 1,
  measuredSamplesPerArm: 3,
  processPolicy: {
    kind: "one-process-per-arm",
    restartBetweenPairs: true,
  },
  pairPolicy: { kind: "paired", pairsPerJob: 1 },
  orderPolicy: { kind: "alternating", first: "AB" },
};

const calibrationKey: NoiseBandKey = {
  scenarioId: "changeset-scanning",
  fixtureId: "changeset-scan-balanced",
  recipeHash: "recipe-abc",
  environmentClass: "test-env",
  execution,
  kind: "paired",
};

function normalPool(count: number, sigma: number, seed = 7): number[] {
  const random = new SeededRandom(seed);
  return Array.from({ length: count }, () => {
    const u1 = Math.max(random.next(), Number.EPSILON);
    const u2 = random.next();
    return sigma * Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
  });
}

function makePool(
  observations: readonly number[],
  independentJobs = 3
): NoiseBandPool {
  return {
    ...calibrationKey,
    observations,
    independentJobs,
    updatedAt: new Date(0).toISOString(),
  };
}

function makeBand(
  bandPercent: number,
  status: NoiseBand["status"] = "established"
): NoiseBand {
  const band = percentToLogRatio(bandPercent);
  return {
    key: calibrationKey,
    kind: "paired",
    status,
    quality: classifyCalibrationQuality(status, bandPercent),
    observations: 30,
    independentJobs: 3,
    statisticSampleSize: 8,
    band,
    bandPercent,
    individualObservation95: band * 2,
    individualObservation95Percent: logRatioToPercent(band * 2),
    observedMaximum: band * 3,
    observedMaximumPercent: logRatioToPercent(band * 3),
  };
}

function gaussian(random: SeededRandom, mean: number, sigma: number): number {
  const u1 = Math.max(random.next(), Number.EPSILON);
  const u2 = random.next();
  return (
    mean + sigma * Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2)
  );
}

describe("quick performance comparison statistics", () => {
  describe("robust paired log ratios", () => {
    it("collapses each arm by median and preserves arm direction across order", () => {
      const ab = collapsePair({
        pair: 0,
        order: "AB",
        armASamples: [100, 102, 900],
        armBSamples: [110, 112, 1_000],
      });
      const ba = collapsePair({
        pair: 1,
        order: "BA",
        armASamples: [100, 102, 900],
        armBSamples: [110, 112, 1_000],
      });
      expect(ab.armA).to.equal(102);
      expect(ab.armB).to.equal(112);
      expect(ab.logRatio).to.equal(ba.logRatio);
      expect(ab.logRatio).to.be.greaterThan(0);
      const [first, second] = orderEffectLogRatios([ab, ba]);
      expect(first).to.be.closeTo(-second, 1e-12);
    });

    it("round-trips percentages throughout the practical range", () => {
      for (let percent = -50; percent <= 100; percent += 0.25)
        expect(logRatioToPercent(percentToLogRatio(percent))).to.be.closeTo(
          percent,
          1e-10
        );
    });

    it("rejects invalid durations and empty samples", () => {
      expect(() => logRatio(0, 1)).to.throw(/finite positive/);
      expect(() => logRatio(1, Number.NaN)).to.throw(/finite positive/);
      expect(() => collapseArmSamples([])).to.throw(/at least one/);
    });

    it("produces deterministic robust aggregates", () => {
      const values = [
        percentToLogRatio(-2),
        percentToLogRatio(1),
        percentToLogRatio(2),
        percentToLogRatio(80),
      ];
      const aggregate = aggregateLogRatios(values);
      expect(aggregate.medianLogRatio).to.be.lessThan(aggregate.meanLogRatio);
      expect(bootstrapMedianInterval(values)).to.deep.equal(
        bootstrapMedianInterval(values)
      );
    });

    it("keeps exact sign diagnostics mathematically valid", () => {
      let mass = 0;
      for (let successes = 0; successes <= 16; successes++)
        mass += binomialPmf(16, successes);
      expect(mass).to.be.closeTo(1, 1e-12);
      expect(twoSidedSignTestP(8, 8)).to.equal(0.0078125);
      expect(twoSidedSignTestP(8, 4)).to.equal(1);
    });
  });

  describe("execution fingerprint and calibration identity", () => {
    const key = calibrationKey;

    it("captures the initial eight-execution hypothesis without making it policy", () => {
      validateExecutionFingerprint(execution);
      expect(execution.pairPolicy.kind).to.equal("paired");
      if (execution.pairPolicy.kind !== "paired")
        throw new Error("Test fingerprint must be paired");
      const totalExecutions =
        2 *
        execution.pairPolicy.pairsPerJob *
        (execution.warmupSamplesPerArm + execution.measuredSamplesPerArm);
      expect(totalExecutions).to.equal(8);
    });

    it("rejects invalid sample and pair counts", () => {
      expect(() =>
        validateExecutionFingerprint({
          ...execution,
          measuredSamplesPerArm: 0,
        })
      ).to.throw(/Measured samples/);
      expect(() =>
        validateExecutionFingerprint({
          ...execution,
          pairPolicy: { kind: "paired", pairsPerJob: 0 },
        })
      ).to.throw(/Pairs per job/);
    });

    it("keys every structure property rather than a loose policy label", () => {
      const variants: ExecutionFingerprint[] = [
        { ...execution, warmupSamplesPerArm: 2 },
        { ...execution, measuredSamplesPerArm: 4 },
        {
          ...execution,
          processPolicy: { kind: "one-process-per-sample" },
        },
        {
          ...execution,
          pairPolicy: { kind: "paired", pairsPerJob: 2 },
        },
        {
          ...execution,
          orderPolicy: { kind: "fixed", order: "AB" },
        },
      ];
      for (const variant of variants) {
        expect(executionFingerprintKey(variant)).to.not.equal(
          executionFingerprintKey(execution)
        );
        expect(() =>
          assertExecutionFingerprintMatches(variant, execution)
        ).to.throw(/does not match calibration/);
      }
    });

    it("canonicalizes property order in the stable execution key", () => {
      const reordered: ExecutionFingerprint = {
        orderPolicy: { first: "AB", kind: "alternating" },
        pairPolicy: { pairsPerJob: 1, kind: "paired" },
        processPolicy: {
          restartBetweenPairs: true,
          kind: "one-process-per-arm",
        },
        measuredSamplesPerArm: 3,
        warmupSamplesPerArm: 1,
      };
      expect(executionFingerprintKey(reordered)).to.equal(
        executionFingerprintKey(execution)
      );
    });

    it("rejects calibration for any scenario, fixture, recipe, environment, or structure mismatch", () => {
      const pool = makePool(normalPool(12, 0.02));
      expect(() => assertPoolApplies(pool, key)).to.not.throw();
      const mismatches = [
        { ...key, scenarioId: "other-scenario" },
        { ...key, fixtureId: "other-fixture" },
        { ...key, recipeHash: "other-recipe" },
        { ...key, environmentClass: "other-environment" },
        { ...key, execution: { ...execution, warmupSamplesPerArm: 0 } },
      ];
      for (const mismatch of mismatches)
        expect(() => assertPoolApplies(pool, mismatch)).to.throw(
          /does not apply/
        );
    });

    it("includes fixture and typed execution identity in storage keys", () => {
      expect(noiseBandKey(key)).to.not.equal(
        noiseBandKey({ ...key, fixtureId: "other-fixture" })
      );
      expect(noiseBandKey(key)).to.not.equal(
        noiseBandKey({
          ...key,
          execution: { ...execution, measuredSamplesPerArm: 5 },
        })
      );
    });
  });

  describe("noise calibration", () => {
    it("accumulates establishment across independent jobs without a fixed pair count", () => {
      expect(classifyBandStatus(1, 1)).to.equal("provisional");
      expect(classifyBandStatus(3, 3)).to.equal("established");
      expect(
        classifyBandStatus(2, 2, {
          provisionalIndependentJobs: 2,
          establishedIndependentJobs: 2,
          provisionalObservations: 2,
          establishedObservations: 2,
        })
      ).to.equal("established");
    });

    it("classifies the absolute 5% target and 10% resolvability boundary", () => {
      const cases: readonly [number, CalibrationQuality][] = [
        [targetNoiseBandPercent, "target"],
        [5.01, "marginal"],
        [9.99, "marginal"],
        [10, "unresolvable"],
        [15, "unresolvable"],
      ];
      for (const [percent, expected] of cases)
        expect(classifyCalibrationQuality("established", percent)).to.equal(
          expected
        );
      expect(classifyCalibrationQuality("uncalibrated", 1)).to.equal(
        "uncalibrated"
      );
      expect(classifyCalibrationQuality("provisional", 1)).to.equal(
        "uncalibrated"
      );
      const exactFivePercent = deriveNoiseBand(
        makePool(Array.from({ length: 3 }, () => percentToLogRatio(5))),
        1
      );
      expect(exactFivePercent.quality).to.equal("target");
    });

    it("derives the gate for the statistic sample size, not individual observations", () => {
      const pool = makePool(normalPool(100, 0.03));
      const atThree = deriveNoiseBand(pool, 3);
      const atTwelve = deriveNoiseBand(pool, 12);
      expect(atThree.band).to.be.lessThan(atThree.individualObservation95);
      expect(atTwelve.band).to.be.lessThan(atThree.band);
      expect(medianNullThreshold(pool.observations, 12)).to.equal(
        atTwelve.band
      );
    });
  });

  describe("magnitude-led verdict", () => {
    const targetBand = makeBand(4);

    it("issues no verdict without established matching A/A data", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, () => percentToLogRatio(30))
      );
      expect(decideVerdict({ aggregate, mode: "paired" }).verdict).to.equal(
        "uncalibrated"
      );
      expect(
        decideVerdict({
          aggregate,
          band: makeBand(4, "provisional"),
          mode: "paired",
        }).verdict
      ).to.equal("uncalibrated");
    });

    it("uses the 10% meaningful margin even when calibration is quieter", () => {
      const belowMargin = aggregateLogRatios(
        Array.from({ length: 8 }, () => percentToLogRatio(8))
      );
      const aboveMargin = aggregateLogRatios(
        Array.from({ length: 8 }, () => percentToLogRatio(12))
      );
      expect(
        decideVerdict({
          aggregate: belowMargin,
          band: targetBand,
          mode: "paired",
        }).verdict
      ).to.not.equal("regressed");
      expect(
        decideVerdict({
          aggregate: aboveMargin,
          band: targetBand,
          mode: "paired",
        }).verdict
      ).to.equal("regressed");
      expect(defaultEquivalenceMarginPercent).to.equal(10);
    });

    it("applies a true +/-10% percentage boundary in both directions", () => {
      const inside = aggregateLogRatios(
        Array.from({ length: 8 }, () => percentToLogRatio(-9.5))
      );
      const outside = aggregateLogRatios(
        Array.from({ length: 8 }, () => percentToLogRatio(-10.5))
      );
      expect(
        decideVerdict({
          aggregate: inside,
          band: targetBand,
          mode: "paired",
        }).verdict
      ).to.not.equal("improved");
      expect(
        decideVerdict({
          aggregate: outside,
          band: targetBand,
          mode: "paired",
        }).verdict
      ).to.equal("improved");
    });

    it("does not conjunctively require strong sign agreement", () => {
      const sixToTwo = aggregateLogRatios(
        [40, 35, 30, 25, 20, 15, -2, -3].map(percentToLogRatio)
      );
      const result = decideVerdict({
        aggregate: sixToTwo,
        band: targetBand,
        mode: "paired",
      });
      expect(result.verdict).to.equal("regressed");
      expect(result.signDiagnostic?.nearEven).to.equal(false);
    });

    it("uses near-even signs as a disagreement and bimodality detector", () => {
      const fiveToThree = aggregateLogRatios(
        [60, 55, 50, 45, 40, -5, -6, -7].map(percentToLogRatio)
      );
      const result = decideVerdict({
        aggregate: fiveToThree,
        band: targetBand,
        mode: "paired",
      });
      expect(result.magnitudeGate?.passed).to.equal(true);
      expect(result.signDiagnostic?.nearEven).to.equal(true);
      expect(result.verdict).to.equal("inconclusive");
      expect(result.reason).to.match(/outliers|bimodality/);
    });

    it("reports marginal calibration as informational rather than execution policy", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, () => percentToLogRatio(20))
      );
      const result = decideVerdict({
        aggregate,
        band: makeBand(7),
        mode: "paired",
      });
      expect(result.verdict).to.equal("regressed");
      expect(result.evidence).to.equal("informational");
      expect(result).to.not.have.property("mergeBlocking");
    });

    it("cannot establish equivalence when the band reaches 10%", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, (_, index) =>
          percentToLogRatio(index % 2 === 0 ? 0.1 : -0.1)
        )
      );
      const result = decideVerdict({
        aggregate,
        band: makeBand(10),
        mode: "paired",
      });
      expect(result.verdict).to.equal("inconclusive");
      expect(result.calibrationQuality).to.equal("unresolvable");
      expect(result.reason).to.match(/cannot resolve/);
    });

    it("establishes equivalence inside the margin with resolving calibration", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, (_, index) =>
          percentToLogRatio(index % 2 === 0 ? 0.1 : -0.1)
        )
      );
      const result = decideVerdict({
        aggregate,
        band: targetBand,
        mode: "paired",
      });
      expect(result.verdict).to.equal("unchanged");
      expect(result.evidence).to.equal("actionable");
    });

    it("suppresses statistical outcomes on harness validity failure", () => {
      const result = decideVerdict({
        aggregate: aggregateLogRatios(
          Array.from({ length: 8 }, () => percentToLogRatio(30))
        ),
        band: targetBand,
        mode: "paired",
        validityFailures: [
          { check: "semanticDigest", detail: "arms disagreed" },
        ],
      });
      expect(result.verdict).to.equal("invalid");
      expect(result.evidence).to.equal("descriptive");
    });
  });

  describe("simulation", () => {
    it("keeps null changes quiet and detects meaningful shifts without a sign gate", () => {
      const pool = makePool(normalPool(120, 0.02, 11), 6);
      const band = deriveNoiseBand(pool, 8);
      expect(band.quality).to.equal("target");
      const random = new SeededRandom(99);
      let falsePositives = 0;
      let detections = 0;
      const trials = 1_000;
      for (let trial = 0; trial < trials; trial++) {
        const nullResult = decideVerdict({
          aggregate: aggregateLogRatios(
            Array.from({ length: 8 }, () => gaussian(random, 0, 0.02)),
            { resamples: 20 }
          ),
          band,
          mode: "paired",
        });
        if (
          nullResult.verdict === "regressed" ||
          nullResult.verdict === "improved"
        )
          falsePositives++;

        const shiftedResult = decideVerdict({
          aggregate: aggregateLogRatios(
            Array.from({ length: 8 }, () =>
              gaussian(random, percentToLogRatio(15), 0.02)
            ),
            { resamples: 20 }
          ),
          band,
          mode: "paired",
        });
        if (shiftedResult.verdict === "regressed") detections++;
      }
      expect(falsePositives / trials).to.be.lessThan(0.01);
      expect(detections / trials).to.be.greaterThan(0.95);
    });
  });

  describe("environment classification", () => {
    it("is stable for identical descriptors and changes with material environment identity", () => {
      const descriptor = {
        platform: "linux",
        arch: "x64",
        cpuModel: "test cpu",
        cpuCount: 8,
        memoryGibBucket: 16,
        nodeMajor: 24,
        runner: "ubuntu",
      };
      const first = classifyEnvironment(descriptor);
      expect(classifyEnvironment({ ...descriptor })).to.deep.equal(first);
      expect(
        classifyEnvironment({ ...descriptor, nodeMajor: 25 }).id
      ).to.not.equal(first.id);
    });
  });

  describe("isolated arm contracts", () => {
    let tempDir: string;

    beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "comparison-arm-"));
    });

    afterEach(() => {
      fs.rmSync(tempDir, { recursive: true, force: true });
    });

    function createArm(id: string, operation: "identity" | "fork-init") {
      const root = path.join(tempDir, id);
      fs.mkdirSync(root);
      fs.writeFileSync(path.join(root, "index.js"), "module.exports = {};\n");
      fs.writeFileSync(
        path.join(root, "package.json"),
        JSON.stringify({
          version: "2.0.0",
          main: "index.js",
          peerDependencies: { "@itwin/core-backend": "^5.10.3" },
        })
      );
      return resolveArmSpec({
        id,
        packageRoot: root,
        operation,
      });
    }

    it("resolves paths and manifests without importing arm modules", () => {
      const arm = createArm("A", "identity");
      expect(arm.transformerVersion).to.equal("2.0.0");
      expect(arm.coreBackendPeerRange).to.equal("^5.10.3");
      expect(arm.modulePath).to.equal(
        fs.realpathSync(path.join(tempDir, "A", "index.js"))
      );
    });

    it("requires the same operation from both arm specs", () => {
      const armA = createArm("A", "identity");
      const armB = createArm("B", "fork-init");
      expect(() => assertArmSpecsComparable(armA, armB)).to.throw(
        /different operations/
      );
    });

    it("rejects a module path that escapes the declared package", () => {
      const armRoot = path.join(tempDir, "A");
      fs.mkdirSync(armRoot);
      fs.writeFileSync(
        path.join(tempDir, "outside.js"),
        "module.exports = {};\n"
      );
      fs.writeFileSync(
        path.join(armRoot, "package.json"),
        JSON.stringify({
          peerDependencies: { "@itwin/core-backend": "^5.10.3" },
        })
      );
      expect(() =>
        resolveArmSpec({
          id: "A",
          packageRoot: armRoot,
          modulePath: "../outside.js",
          operation: "identity",
        })
      ).to.throw(/inside its package root/);
    });

    it("compares child-reported native dependency identities", () => {
      const identity: ArmRuntimeIdentity = {
        armId: "A",
        transformerVersion: "2.0.0",
        coreBackendVersion: "5.10.3",
        coreBackendPackageHash: "abc",
      };
      expect(() =>
        assertArmRuntimeComparable(identity, { ...identity, armId: "B" })
      ).to.not.throw();
      expect(() =>
        assertArmRuntimeComparable(identity, {
          ...identity,
          armId: "B",
          coreBackendPackageHash: "def",
        })
      ).to.throw(/different @itwin\/core-backend/);
    });
  });

  describe("reports", () => {
    it("builds and writes JSON and Markdown with calibration and execution identity", () => {
      const pool = makePool(normalPool(30, 0.02), 3);
      const pairs = Array.from({ length: 8 }, (_, index) =>
        collapsePair({
          pair: index,
          order: index % 2 === 0 ? "AB" : "BA",
          armASamples: [100, 101, 102],
          armBSamples: [112, 113, 114],
        })
      );
      const band = deriveNoiseBand(pool, pairs.length);
      const options: BuildComparisonReportOptions = {
        scenarioId: pool.scenarioId,
        fixtureId: pool.fixtureId,
        recipeHash: pool.recipeHash,
        mode: "paired",
        environment: {
          id: pool.environmentClass,
          descriptor: {
            platform: "test",
            arch: "x64",
            cpuModel: "test",
            cpuCount: 8,
            memoryGibBucket: 16,
            nodeMajor: 24,
            runner: "test",
          },
        },
        execution,
        armA: {
          id: "A",
          transformerVersion: "1",
          coreBackendVersion: "5",
        },
        armB: {
          id: "B",
          transformerVersion: "2",
          coreBackendVersion: "5",
        },
        pairs,
        independentJobs: 8,
        pool,
        band,
      };
      const report = buildComparisonReport(options);
      const markdown = renderComparisonReport(report);
      expect(markdown).to.contain("# Quick performance comparison");
      expect(markdown).to.contain("Execution fingerprint");
      expect(markdown).to.contain("independent jobs");
      expect(() =>
        buildComparisonReport({ ...options, independentJobs: 1 })
      ).to.throw(/execution fingerprint requires 1/);
      expect(() =>
        buildComparisonReport({
          ...options,
          execution: {
            ...execution,
            pairPolicy: {
              kind: "unpaired",
              armAObservationsPerJob: 1,
              armBObservationsPerJob: 1,
            },
          },
        })
      ).to.throw(/distinct estimator/);
      expect(() =>
        buildComparisonReport({
          ...options,
          pool: undefined,
          band: {
            ...band,
            key: { ...band.key, fixtureId: "unrelated-fixture" },
          },
        })
      ).to.throw(/does not apply/);

      const outputDir = fs.mkdtempSync(
        path.join(os.tmpdir(), "comparison-report-")
      );
      try {
        writeComparisonReport(outputDir, report);
        expect(fs.existsSync(path.join(outputDir, "comparison.json"))).to.equal(
          true
        );
        expect(fs.existsSync(path.join(outputDir, "comparison.md"))).to.equal(
          true
        );
      } finally {
        fs.rmSync(outputDir, { recursive: true, force: true });
      }
    });
  });
});
