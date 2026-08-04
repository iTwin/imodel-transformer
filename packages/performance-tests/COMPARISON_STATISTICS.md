# Quick performance comparison statistics

This library answers one question: **did arm B become meaningfully slower or faster than arm A?**
It analyzes benchmark observations and writes comparison reports. A later runner chooses refs, builds
the arms, launches isolated processes, and stores calibration data.

## Reading a result

Positive percentages mean arm B is slower than arm A.

| Verdict                     | Meaning                                                                 |
| --------------------------- | ----------------------------------------------------------------------- |
| `regressed`                 | B is slower by more than both expected noise and the 10% action margin. |
| `improved`                  | B is faster by more than both expected noise and the 10% action margin. |
| `unchanged`                 | A confidence interval fits inside +/-10%, with resolving calibration.   |
| `inconclusive`              | The data cannot distinguish a meaningful change or equivalence.         |
| `uncalibrated`              | No established matching A/A calibration exists.                         |
| `insufficient-observations` | The configured minimum for change detection was not met.                |
| `invalid`                   | A harness validity check failed.                                        |

Evidence is separate from the verdict. Target-quality calibration can produce `actionable` evidence;
marginal or unresolvable calibration is `informational`; missing or provisional calibration is
`descriptive`. The library deliberately has no merge-blocking field.

### Example

1. Repeated A/A jobs compare the same code against itself. Their paired timing differences form a
   calibration pool for one exact scenario, fixture, environment, and execution structure.
2. An explicitly configured policy decides when that pool is large and independent enough to be
   established. Until then, reports remain descriptive.
3. An A/B run compares the baseline and candidate using the same structure. Suppose its median says
   B is 14% slower and the matching A/A band is 4%.
4. Because 14% exceeds both the 4% noise band and the 10% action margin, the result is `regressed`.
   Near-even paired signs can still make it `inconclusive` as a disagreement or bimodality warning.

An `unchanged` result is intentionally harder to establish. It requires at least six independent
paired observations by default. Six is the first sample size whose exact distribution-free
minimum-to-maximum interval covers the population median with at least 95% confidence. The interval
must fit inside +/-10%, and the matching A/A band must also be below 10%. The bootstrap interval is
reported only as a diagnostic.

## Safety rules

- **No established calibration, no verdict.** Default calibration never becomes established merely
  by accumulating a small arbitrary number of jobs. Establishment requires explicit observation and
  independent-job thresholds chosen from real calibration evidence.
- **Calibration must match execution exactly.** Scenario, fixture, recipe, environment, warm-ups,
  measured samples, process policy, pair policy, and order policy are calibration identity.
- **A band must come from its supplied pool.** Reports verify a digest of the calibration inputs and
  reproduce the band from its recorded quantile, resample count, seed, sample size, and requirements.
- **The execution plan must be complete.** Pair indexes are unique and contiguous, and every valid
  or discarded observation records the order required by the fixed, alternating, or seeded policy.
- **Invalid numbers are rejected.** Durations, ratios, percentages, resampling parameters, pools,
  aggregates, and report output must be finite and within their valid ranges.
- **Failed pairs stay paired.** A failed observation is discarded as one A/B unit, never one arm at a
  time. Statistical outliers are reported rather than removed.

## Runner contract

`ExecutionFingerprint` is the typed identity of the execution structure. A/A and A/B data are
comparable only when all of these match:

- scenario, fixture, recipe hash, and environment class;
- warm-up and measured sample counts;
- process isolation and restart policy;
- paired or unpaired observation policy, including observations per CI job;
- ordering policy and its first order or random seed.

The initial runner hypothesis is eight scenario executions for one independent pair: each arm gets
one warm-up and three measured executions. The three measurements within one arm collapse to their
median; they are not three independent pairs. Pair count and calibration accumulation remain runner
configuration rather than fixed statistical constants.

Pair indexes are zero-based across the complete report. Fixed order repeats one order, alternating
order starts with its configured first order, and seeded-random order consumes the shared deterministic
random sequence once per pair. A discarded observation still records its scheduled index and order.

`ArmModule.ts` validates serializable arm paths and manifests for isolated child processes. The
analysis process never imports both transformer arms or loads their native dependencies. Child
processes report runtime identities, and comparison rejects different core-backend versions or
package hashes.

Unpaired verdicts are deferred. They require an estimator and bootstrap over the two arms
independently; zipping unrelated observations into synthetic pairs would depend on arbitrary order.

## Statistical details

### Paired estimator

For each independent pair, measured samples within each arm collapse by median. Analysis then uses:

```text
d_i = ln(B_i / A_i)
```

The headline estimate is `median(d_i)`, reported as `(exp(median(d_i)) - 1) * 100`. The geometric
mean ratio, bootstrap interval, signs, ties, and raw collapsed observations remain diagnostics.
Order never changes the sign of `d_i`; it is recorded as a covariate.

### A/A calibration

The magnitude threshold is the configured quantile, normally 95%, of `|median(d)|` obtained by
resampling the matching raw A/A pool at the comparison's observation count. The pool stays raw
because the null distribution changes with sample size.

Calibration quality is absolute:

| Derived A/A band                   | Quality        | Interpretation                                           |
| ---------------------------------- | -------------- | -------------------------------------------------------- |
| `<= 5%`                            | `target`       | Meets the calibration target.                            |
| `> 5%` and `< 10%`                 | `marginal`     | Informational; below the desired resolution.             |
| `>= 10%`                           | `unresolvable` | Cannot resolve the meaningful 10% threshold.             |
| establishment requirements not met | `uncalibrated` | Descriptive output only; no verdict.                     |

The 5% target is not a coefficient-of-variation rule. CV and normalized median absolute deviation
describe dispersion among samples within one benchmark run; the A/A band describes repeatability of
the paired comparison across independent observations. See [ARCHITECTURE.md](./ARCHITECTURE.md) for
the within-run CV and MAD fields.

### Change and equivalence

A magnitude-led change requires:

```text
|median(d)| > max(A/A band, 10% action margin)
```

Direction comes from the median sign. Sign agreement is not a conjunctive change gate. It remains a
disagreement, outlier, or bimodality detector: when magnitude fires but signs are near 50/50, the
result is `inconclusive`.

Equivalence uses an exact two-sided confidence interval for the population median based on observed
order statistics. The configured confidence level determines a mathematical minimum observation
count; callers may raise but cannot lower that minimum. `unchanged` requires this interval to lie
strictly inside +/-10% and requires the established A/A band to be strictly below 10%.

## Reports and persistence

Reports include the execution fingerprint, calibration provenance and quality, evidence level,
aggregate and diagnostics, arm/environment identity, and collapsed observations. Writers emit
`comparison.json` and `comparison.md` only after runtime validation confirms that numeric values are
finite and the observation plan is internally consistent.
