/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "chai";
import { assertArmCoreBackendIdentity } from "./ArmModule";
import {
  binomialPmf,
  signGateRequirement,
  twoSidedSignTestP,
} from "./binomial";
import {
  aggregateLogRatios,
  bootstrapMedianInterval,
  collapseArmSamples,
  collapsePair,
  logRatio,
  logRatioToPercent,
  orderEffectLogRatios,
  percentToLogRatio,
} from "./logRatio";
import {
  assertPoolApplies,
  classifyBandStatus,
  deriveNoiseBand,
  medianNullThreshold,
  NoiseBand,
  noiseBandKey,
  NoiseBandPool,
} from "./NoiseBand";
import { SeededRandom } from "./SeededRandom";
import { decideVerdict, signGateTargetLevel } from "./verdict";

function normalPool(count: number, sigma: number, seed = 7): number[] {
  const random = new SeededRandom(seed);
  const values: number[] = [];
  for (let index = 0; index < count; index++) {
    // Box-Muller, so the pool has a known scale to test against.
    const u1 = Math.max(random.next(), Number.EPSILON);
    const u2 = random.next();
    values.push(
      sigma * Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2)
    );
  }
  return values;
}

/** One draw from `N(mu, sigma)`, so trials can be run at a known true effect size. */
function gaussian(random: SeededRandom, mu: number, sigma: number): number {
  const u1 = Math.max(random.next(), Number.EPSILON);
  const u2 = random.next();
  return mu + sigma * Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
}

interface PowerCharacterization {
  /** Fraction of trials returning `regressed` or `improved` -- both gates held. */
  readonly detected: number;
  readonly magnitudeOnly: number;
  readonly signOnly: number;
  /** Fraction landing on the side opposite the true effect. */
  readonly wrongDirection: number;
}

/**
 * Run the real verdict rule against synthetic pairs drawn at a known true effect, and report how
 * often it fires.
 *
 * This is the falsifiable form of the power claims in COMPARISON_STATISTICS.md. Prose asserting a
 * false-positive rate cannot notice when a threshold constant stops matching the rule it describes;
 * this can. The A1 defect -- a transcribed decimal excluding the exact unanimous level, making
 * `regressed` unreachable at every effect size -- shows up here as detection collapsing to zero.
 */
function characterizePower(
  mu: number,
  pairs: number,
  band: NoiseBand,
  trials: number,
  seed: number
): PowerCharacterization {
  const random = new SeededRandom(seed);
  let detected = 0;
  let magnitudeOnly = 0;
  let signOnly = 0;
  let wrongDirection = 0;
  for (let trial = 0; trial < trials; trial++) {
    const logRatios = Array.from({ length: pairs }, () =>
      gaussian(random, mu, 1)
    );
    // The bootstrap is not consulted by either change gate, so a small resample count here keeps
    // the characterization affordable without touching what is being measured.
    const aggregate = aggregateLogRatios(logRatios, { resamples: 40 });
    const result = decideVerdict({
      aggregate,
      band,
      look: 1,
      mode: "paired",
    });
    if (result.magnitudeGate?.passed === true) magnitudeOnly++;
    if (result.signGate?.passed === true) signOnly++;
    if (result.verdict === "regressed" || result.verdict === "improved") {
      detected++;
      const opposite = mu >= 0 ? "improved" : "regressed";
      if (result.verdict === opposite) wrongDirection++;
    }
  }
  return {
    detected: detected / trials,
    magnitudeOnly: magnitudeOnly / trials,
    signOnly: signOnly / trials,
    wrongDirection: wrongDirection / trials,
  };
}

function makePool(observations: number[], runs = 3): NoiseBandPool {
  return {
    environmentClass: "test-env",
    scenarioId: "incremental-synchronization",
    recipeHash: "recipe-abc",
    kind: "paired",
    samplesPerArmPerPair: 3,
    observations,
    runs,
    updatedAt: new Date(0).toISOString(),
  };
}

describe("quick performance comparison statistics", () => {
  describe("exact binomial sign test", () => {
    it("produces the exact two-sided levels", () => {
      expect(twoSidedSignTestP(8, 8)).to.equal(0.0078125);
      expect(twoSidedSignTestP(8, 7)).to.equal(0.0703125);
      expect(twoSidedSignTestP(16, 14)).to.be.closeTo(0.0041809, 1e-7);
      expect(twoSidedSignTestP(16, 13)).to.be.closeTo(0.0212708, 1e-7);
    });

    it("has a probability mass function summing to one", () => {
      let total = 0;
      for (let count = 0; count <= 16; count++) total += binomialPmf(16, count);
      expect(total).to.be.closeTo(1, 1e-12);
    });

    it("derives gate requirements as counts, not as rounded p-values", () => {
      const atEight = signGateRequirement(8, signGateTargetLevel);
      expect(atEight.requiredAgreeing).to.equal(8);
      expect(atEight.unachievable).to.equal(false);
      const atSixteen = signGateRequirement(16, signGateTargetLevel);
      expect(atSixteen.requiredAgreeing).to.equal(14);
      expect(atSixteen.unachievable).to.equal(false);
    });

    it("flags a level that no count can achieve instead of never firing", () => {
      // At four pairs, unanimity is p = 0.125, so a 1% target is unreachable.
      const requirement = signGateRequirement(4, signGateTargetLevel);
      expect(requirement.unachievable).to.equal(true);
    });

    it("bounds the family-wise rate across the two permitted looks", () => {
      const look1 = signGateRequirement(8, signGateTargetLevel).achievedLevel;
      const look2 = signGateRequirement(16, signGateTargetLevel).achievedLevel;
      expect(look1 + look2).to.be.lessThan(0.05);
    });
  });

  describe("log-ratio estimator", () => {
    it("collapses within-process samples by median", () => {
      expect(collapseArmSamples([100, 102, 400])).to.equal(102);
    });

    it("treats positive as arm B being slower", () => {
      expect(logRatio(100, 110)).to.be.greaterThan(0);
      expect(logRatio(110, 100)).to.be.lessThan(0);
      expect(logRatioToPercent(logRatio(100, 110))).to.be.closeTo(10, 1e-9);
    });

    it("round-trips between log and percent scales", () => {
      expect(logRatioToPercent(percentToLogRatio(7.5))).to.be.closeTo(
        7.5,
        1e-9
      );
    });

    it("does not sign-flip a BA-ordered pair", () => {
      const ab = collapsePair({
        pair: 0,
        order: "AB",
        armASamples: [100],
        armBSamples: [110],
      });
      const ba = collapsePair({
        pair: 1,
        order: "BA",
        armASamples: [100],
        armBSamples: [110],
      });
      expect(ba.logRatio).to.equal(ab.logRatio);
    });

    it("orders the order-effect ratio by execution position, not by arm", () => {
      const pairs = [
        collapsePair({
          pair: 0,
          order: "AB",
          armASamples: [100],
          armBSamples: [110],
        }),
        collapsePair({
          pair: 1,
          order: "BA",
          armASamples: [100],
          armBSamples: [110],
        }),
      ];
      const [first, second] = orderEffectLogRatios(pairs);
      // AB: second/first = B/A = 1.10. BA: second/first = A/B = 1/1.10.
      expect(first).to.be.greaterThan(0);
      expect(second).to.be.lessThan(0);
      expect(first).to.be.closeTo(-second, 1e-12);
    });

    it("rejects non-positive durations rather than producing NaN", () => {
      expect(() => logRatio(0, 100)).to.throw(/finite positive/);
      expect(() => logRatio(100, -1)).to.throw(/finite positive/);
    });

    it("excludes ties from the sign test and reduces the effective sample size", () => {
      const aggregate = aggregateLogRatios([0.1, 0.1, 0, 0.1]);
      expect(aggregate.signs.ties).to.equal(1);
      expect(aggregate.signs.effectivePairs).to.equal(3);
    });

    it("is reproducible from its own seed", () => {
      const values = normalPool(8, 0.03);
      expect(bootstrapMedianInterval(values)).to.deep.equal(
        bootstrapMedianInterval(values)
      );
    });
  });

  describe("noise band", () => {
    it("gates the median statistic, not individual pairs, and is therefore tighter", () => {
      const pool = makePool(normalPool(24, 0.03));
      const band = deriveNoiseBand(pool, 8);
      expect(band.band).to.be.lessThan(band.individualPair95);
    });

    it("tightens as the pair count rises, so escalation is analysed against its own band", () => {
      const observations = normalPool(24, 0.03);
      expect(medianNullThreshold(observations, 16)).to.be.lessThan(
        medianNullThreshold(observations, 8)
      );
    });

    it("requires both pairs and distinct runs before it is established", () => {
      expect(classifyBandStatus(24, 3)).to.equal("established");
      expect(classifyBandStatus(24, 2)).to.equal("provisional");
      expect(classifyBandStatus(16, 2)).to.equal("provisional");
      expect(classifyBandStatus(15, 5)).to.equal("uncalibrated");
    });
  });

  describe("calibration key", () => {
    const pool = makePool(normalPool(24, 0.03));
    const key = {
      scenarioId: pool.scenarioId,
      recipeHash: pool.recipeHash,
      environmentClass: pool.environmentClass,
      samplesPerArmPerPair: pool.samplesPerArmPerPair,
      kind: pool.kind,
    };

    it("keys a band by scenario and recipe, not by environment alone", () => {
      expect(noiseBandKey(key)).to.not.equal(
        noiseBandKey({ ...key, scenarioId: "scanner" })
      );
      expect(noiseBandKey(key)).to.not.equal(
        noiseBandKey({ ...key, recipeHash: "recipe-xyz" })
      );
    });

    it("accepts a pool that matches on every key component", () => {
      expect(() => assertPoolApplies(pool, key)).to.not.throw();
    });

    it("refuses to carry a band across scenarios", () => {
      // A scenario that rebuilds its fixture per sample has a longer process and a wider gap
      // between the arms of a pair, so its floor is not the floor of an artifact-backed scenario.
      expect(() =>
        assertPoolApplies(pool, { ...key, scenarioId: "scanner" })
      ).to.throw(/scenario/);
    });

    it("refuses to carry a band across recipes, environments, k, or band kind", () => {
      expect(() =>
        assertPoolApplies(pool, { ...key, recipeHash: "recipe-xyz" })
      ).to.throw(/recipe hash/);
      expect(() =>
        assertPoolApplies(pool, { ...key, environmentClass: "other-env" })
      ).to.throw(/environment class/);
      expect(() =>
        assertPoolApplies(pool, { ...key, samplesPerArmPerPair: 1 })
      ).to.throw(/samples per arm per pair/);
      expect(() =>
        assertPoolApplies(pool, { ...key, kind: "unpaired" })
      ).to.throw(/band kind/);
    });
  });

  describe("verdict rule", () => {
    const pool = makePool(normalPool(24, 0.03));
    const band = deriveNoiseBand(pool, 8);

    it("can actually reach a change verdict at eight pairs", () => {
      // The regression this guards: a threshold written as `<= 0.0078` excludes the exact
      // unanimous level of 0.0078125, making `regressed` unreachable at any effect size.
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, (_, index) =>
          percentToLogRatio(12 + index * 0.1)
        )
      );
      const result = decideVerdict({
        aggregate,
        band,
        look: 1,
        mode: "paired",
      });
      expect(result.verdict).to.equal("regressed");
      expect(result.magnitudeGate?.passed).to.equal(true);
      expect(result.signGate?.passed).to.equal(true);
    });

    it("reports an improvement when arm B is faster", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, (_, index) =>
          percentToLogRatio(-(12 + index * 0.1))
        )
      );
      expect(
        decideVerdict({ aggregate, band, look: 1, mode: "paired" }).verdict
      ).to.equal("improved");
    });

    it("refuses a verdict without a band", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, () => percentToLogRatio(30))
      );
      const result = decideVerdict({ aggregate, look: 1, mode: "paired" });
      expect(result.verdict).to.equal("uncalibrated");
    });

    it("refuses a verdict below the minimum pair count", () => {
      const aggregate = aggregateLogRatios([0.1, 0.1, 0.1]);
      expect(
        decideVerdict({ aggregate, band, look: 1, mode: "paired" }).verdict
      ).to.equal("insufficient-pairs");
    });

    it("suppresses the verdict entirely on a validity failure", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, () => percentToLogRatio(30))
      );
      const result = decideVerdict({
        aggregate,
        band,
        look: 1,
        mode: "paired",
        validityFailures: [
          { check: "semanticDigest", detail: "arms disagreed" },
        ],
      });
      expect(result.verdict).to.equal("invalid");
    });

    it("does not call a large split-sign result a regression", () => {
      const aggregate = aggregateLogRatios([
        percentToLogRatio(30),
        percentToLogRatio(28),
        percentToLogRatio(26),
        percentToLogRatio(24),
        percentToLogRatio(22),
        percentToLogRatio(20),
        percentToLogRatio(18),
        percentToLogRatio(-4),
      ]);
      const result = decideVerdict({
        aggregate,
        band,
        look: 1,
        mode: "paired",
      });
      expect(result.verdict).to.equal("inconclusive");
      expect(result.magnitudeGate?.passed).to.equal(true);
      expect(result.signGate?.passed).to.equal(false);
    });

    it("recommends escalation only when signs failed while magnitude passed", () => {
      const splitSigns = aggregateLogRatios([
        percentToLogRatio(30),
        percentToLogRatio(28),
        percentToLogRatio(26),
        percentToLogRatio(24),
        percentToLogRatio(22),
        percentToLogRatio(20),
        percentToLogRatio(18),
        percentToLogRatio(-4),
      ]);
      expect(
        decideVerdict({
          aggregate: splitSigns,
          band,
          look: 1,
          mode: "paired",
        }).escalationRecommended
      ).to.equal(true);

      // Magnitude failed: escalation does not move a fixed threshold, so it is not recommended.
      const tiny = aggregateLogRatios(
        Array.from({ length: 8 }, () => percentToLogRatio(0.01))
      );
      expect(
        decideVerdict({ aggregate: tiny, band, look: 1, mode: "paired" })
          .escalationRecommended
      ).to.equal(false);
    });

    it("never recommends escalation at the second look", () => {
      const splitSigns = aggregateLogRatios([
        ...Array.from({ length: 15 }, () => percentToLogRatio(25)),
        percentToLogRatio(-4),
      ]);
      expect(
        decideVerdict({
          aggregate: splitSigns,
          band,
          look: 2,
          mode: "paired",
        }).escalationRecommended
      ).to.equal(false);
    });

    it("cannot establish `unchanged` until an equivalence margin is declared", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, (_, index) =>
          percentToLogRatio(index % 2 === 0 ? 0.05 : -0.05)
        )
      );
      const result = decideVerdict({
        aggregate,
        band,
        look: 1,
        mode: "paired",
      });
      expect(result.verdict).to.equal("inconclusive");
      expect(result.reason).to.match(/equivalence margin/i);
    });

    it("establishes `unchanged` against a declared margin", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, (_, index) =>
          percentToLogRatio(index % 2 === 0 ? 0.05 : -0.05)
        )
      );
      const result = decideVerdict({
        aggregate,
        band,
        look: 1,
        mode: "paired",
        equivalenceMargin: percentToLogRatio(5),
      });
      expect(result.verdict).to.equal("unchanged");
    });

    it("refuses to widen a margin the environment cannot resolve", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, () => percentToLogRatio(0.05))
      );
      const result = decideVerdict({
        aggregate,
        band,
        look: 1,
        mode: "paired",
        equivalenceMargin: percentToLogRatio(0.01),
      });
      expect(result.verdict).to.equal("inconclusive");
      expect(result.reason).to.match(/below the measured noise floor/i);
    });

    it("drops the sign gate for unpaired baseline comparison", () => {
      const aggregate = aggregateLogRatios(
        Array.from({ length: 8 }, (_, index) =>
          percentToLogRatio(index % 2 === 0 ? 14 : 13)
        )
      );
      const result = decideVerdict({
        aggregate,
        band,
        look: 1,
        mode: "unpaired",
      });
      expect(result.verdict).to.equal("regressed");
      expect(result.signGate).to.equal(undefined);
    });
  });

  describe("arm identity gate", () => {
    it("accepts arms sharing one core-backend realpath", () => {
      expect(() =>
        assertArmCoreBackendIdentity(
          "/pnpm/core-backend",
          "/pnpm/core-backend",
          "B"
        )
      ).to.not.throw();
    });

    it("rejects a mismatched core-backend rather than recording it", () => {
      expect(() =>
        assertArmCoreBackendIdentity(
          "/pnpm/a/core-backend",
          "/pnpm/b/core-backend",
          "B"
        )
      ).to.throw(/different @itwin\/core-backend/);
    });
  });

  describe("power characterization of the verdict rule", () => {
    // A pool large enough that the band sits near its asymptote, so these figures describe the
    // RULE rather than the sampling noise of a 24-pair calibration.
    const pool = makePool(normalPool(4000, 1, 11));

    it("derives a band near the asymptotic median-null value at P = 8", () => {
      const band = deriveNoiseBand(pool, 8);
      expect(band.band).to.be.closeTo(0.809, 0.03);
      // The individual-pair diagnostic is 2.4x looser and does not shrink with P at all. Using it
      // as the gate -- the A2 defect -- leaves the rule far more permissive than it claims.
      expect(band.individualPair95).to.be.closeTo(1.964, 0.05);
      expect(band.individualPair95).to.be.greaterThan(band.band * 2);
    });

    it("holds the false-positive rate under 1% when nothing changed", () => {
      const band = deriveNoiseBand(pool, 8);
      const result = characterizePower(0, 8, band, 4000, 99);
      expect(result.detected).to.be.lessThan(0.01);
      // Each gate alone is far more permissive than the conjunction -- the magnitude gate alone
      // fires at roughly its nominal 5%. That is why both are required.
      expect(result.magnitudeOnly).to.be.closeTo(0.05, 0.02);
      expect(result.signOnly).to.be.greaterThan(result.detected);
    });

    it("detects real effects, and detection increases with effect size", () => {
      const band = deriveNoiseBand(pool, 8);
      const at1 = characterizePower(1, 8, band, 4000, 99);
      const at15 = characterizePower(1.5, 8, band, 4000, 99);
      const at2 = characterizePower(2, 8, band, 4000, 99);

      expect(at1.detected).to.be.within(0.15, 0.32);
      expect(at15.detected).to.be.within(0.45, 0.68);
      expect(at2.detected).to.be.within(0.72, 0.92);

      // Guards A1 directly: a threshold that excludes the exact unanimous level makes `regressed`
      // unreachable at EVERY effect size, so these collapse to zero rather than degrading.
      expect(at2.detected).to.be.greaterThan(0.5);
    });

    it("essentially never reports a change in the wrong direction", () => {
      const band = deriveNoiseBand(pool, 8);
      for (const mu of [1, 2]) {
        const result = characterizePower(mu, 8, band, 4000, 99);
        expect(result.wrongDirection).to.equal(0);
      }
    });

    it("is limited by the sign gate, not the magnitude gate, near the band", () => {
      const band = deriveNoiseBand(pool, 8);
      const atBand = characterizePower(band.band, 8, band, 4000, 99);
      // Both gates must hold, so the binding constraint is whichever fires less often. The sign
      // gate is binding across the whole usable range, which is what makes escalation worth
      // anything: escalation relaxes unanimity to 14/16, and that is the gate under strain.
      expect(atBand.signOnly).to.be.lessThan(atBand.magnitudeOnly);
      expect(atBand.detected).to.be.closeTo(atBand.signOnly, 0.05);
      // The band is NOT the effect size the harness reliably catches -- detection there is about
      // an eighth. Reporting the band as "the MDE" would overstate detection several-fold.
      expect(atBand.detected).to.be.within(0.05, 0.25);
    });

    it("has ~50% magnitude power at its own band for any P, which is an identity not a finding", () => {
      // The band is the 95th percentile of the null median, and the median is centred on the true
      // effect, so at `mu = band` the magnitude gate is a coin flip at EVERY pair count. Comparing
      // that number across P appears to show escalation achieving nothing; it only shows that each
      // P is being evaluated at a different effect size. Pinned so nobody re-derives that mistake.
      for (const pairs of [8, 16]) {
        const band = deriveNoiseBand(pool, pairs);
        const atBand = characterizePower(band.band, pairs, band, 4000, 99);
        expect(atBand.magnitudeOnly).to.be.closeTo(0.5, 0.05);
      }
    });

    it("improves substantially at the escalated pair count, at a fixed effect size", () => {
      const at8 = deriveNoiseBand(pool, 8);
      const at16 = deriveNoiseBand(pool, 16);
      // The band is a function of P -- a stored scalar reused at a different P would be wrong.
      expect(at16.band).to.be.lessThan(at8.band);

      // Holding the true effect fixed is the only comparison that answers "is the extra run worth
      // it". It is: detection roughly doubles.
      const initial = characterizePower(1, 8, at8, 4000, 99);
      const escalated = characterizePower(1, 16, at16, 4000, 99);
      expect(escalated.detected).to.be.greaterThan(initial.detected * 1.8);
    });
  });
});
