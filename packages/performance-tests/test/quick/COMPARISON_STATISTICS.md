# Quick performance comparison statistics

This layer defines analysis and serializable contracts only. It does not choose refs, build arms, launch child
processes, persist calibration, or decide whether CI blocks a merge.

## Execution identity

A/A calibration and A/B comparison are comparable only when all of these match:

- scenario, fixture, recipe hash, and environment class;
- warm-up and measured sample counts;
- process isolation/restart policy;
- paired or unpaired observation policy, including observations per CI job;
- ordering policy and its first order or random seed.

`ExecutionFingerprint` is the typed identity for that structure. `NoiseBandPool` includes it, and pool lookup rejects
any mismatch. This prevents a calibration collected with different warm-up, sample, process, pairing, or ordering
structure from silently authorizing a verdict.

Comparison reports also record the number of independent jobs and reject observation counts that do not equal the
fingerprint's per-job count multiplied by that job count.

The initial runner hypothesis is eight scenario executions total: each arm receives one warm-up and three measured
executions. That is runner configuration, not a statistical constant. Pair count, sample count, and any future
escalation are empirical policy choices and become calibration-key material when changed. This specification does not
require 8/16 pairs or 64/128 executions.

Calibration accumulates across independent CI jobs. The default pool-establishment rule requires three independent jobs
and three observations, but those requirements are explicit configuration rather than a fixed benchmark budget.

## Estimator

For each paired observation, collapse measured samples within each arm by median, then compute:

```text
d_i = ln(B_i / A_i)
```

Positive means arm B is slower. The headline estimate is `median(d_i)` and is reported as
`(exp(median(d_i)) - 1) * 100`. The geometric-mean ratio, bootstrap interval, signs, ties, and raw collapsed observations
remain diagnostics.

Order never changes the sign of `d_i`; it is recorded separately. Failed observations are discarded as whole units,
never one arm at a time, and statistical outliers are not removed.

## A/A calibration

The magnitude threshold uses the 95th percentile of `|median(d)|` obtained by resampling the matching raw A/A pool at
the comparison's observation count. The pool stores raw observations because the null distribution changes with sample
count. The individual-observation 95th percentile and observed maximum are diagnostics, not gates.

Calibration quality is absolute:

| Derived A/A band                   | Quality        | Interpretation                                           |
| ---------------------------------- | -------------- | -------------------------------------------------------- |
| `<= 5%`                            | `target`       | Meets the calibration target.                            |
| `> 5%` and `< 10%`                 | `marginal`     | Informational; useful, but below the desired resolution. |
| `>= 10%`                           | `unresolvable` | Cannot resolve the declared meaningful 10% threshold.    |
| establishment requirements not met | `uncalibrated` | Descriptive output only; no verdict.                     |

The 5% calibration target is not a coefficient-of-variation rule. It is the absolute paired or unpaired A/A
median-null band for this exact execution structure.

## Meaningful effect and verdict

The practical equivalence/action margin is 10%. A magnitude-led change verdict requires:

```text
|median(d)| > max(A/A band, 10% margin)
```

Direction comes from the sign of `median(d)`. Sign agreement is not a conjunctive gate. It remains visible as an
outlier/disagreement/bimodality detector: when magnitude fires but paired signs are near 50/50, the result is
`inconclusive`. A 6/2 split at eight observations does not suppress a magnitude-supported verdict; a 5/3 or 4/4 split
does under the default diagnostic threshold.

`unchanged` requires the bootstrap interval to lie entirely inside the declared +/-10% margin and the A/A band to be
strictly below that margin. Otherwise the result is `inconclusive`.

Established `target` calibration yields `actionable` evidence. Established `marginal` or `unresolvable` calibration
yields `informational` evidence. The analysis library deliberately has no `mergeBlocking` field; a later workflow may
map evidence and verdict to execution policy only after real A/A data supports that decision.

## Arms and process isolation

`ArmModule.ts` validates serializable arm paths and package manifests for a future child-process runner. It never
imports both transformer arms, links peers, starts `IModelHost`, or loads native modules in the analysis process.
Child processes must return runtime identities, and comparison rejects arms that report different core-backend version
or package hashes.

## Reports

The report includes the full execution fingerprint, calibration status and quality, evidence level, aggregate and
diagnostics, arm/environment identity, and collapsed observations. Writers emit `comparison.json` and
`comparison.md`.

Unpaired baseline verdicts are deferred. They require an estimator and bootstrap over the two arms independently;
zipping unrelated observations into synthetic pairs would make the result depend on arbitrary ordering. Report
construction rejects unpaired execution fingerprints until that distinct estimator exists.
