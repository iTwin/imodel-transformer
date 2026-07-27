# Quick performance comparison — statistical specification

Status: **reviewed; amended.** Revision 2 folds in review items A1, A2, A3, A5 and A6. One item remains open: the
equivalence margin (§5.3) is a declared domain judgement and has not yet been supplied, so `unchanged` is currently
unreachable by construction.

Amendments in revision 2, all of which changed behaviour rather than wording:

| Item | Change |
|---|---|
| A1 | Gates are expressed in **counts**, not transcribed decimal p-values. As written, revision 1 made `regressed` unreachable at `P = 8`. §3.1, §4.3, §5.2 |
| A2 | The magnitude gate is tested against the null of `d_median`, not of individual `d_i` — roughly 2.2x tighter for the same executions. §4.1 |
| A3 | Escalation is triggered only by consistency-failed-while-magnitude-passed, not by any `inconclusive`. §3.1 |
| A5 | `k` is chosen from a pilot and frozen before calibration; it is not a budget lever afterwards. §1.3 |
| A6 | The order-effect check runs on the accumulated pool at `>= 24` pairs, never per-run. §6.3 |
| — | The band is reported as a noise-floor scale with power annotated, never as "the MDE". §4.4 |

This document fixes the sampling structure, estimator, uncertainty model, verdict rule and stopping rule for the
quick-performance comparison layer. It governs both comparison modes, which share one implementation:

| Mode | Arm A | Arm B | Pairing available? |
|---|---|---|---|
| **A/B** | code version A | code version B | yes — both measured in the same run |
| **A/A** | code version A | the same code version A | yes — this is the calibration mode |
| **Baseline** | a stored prior run | this run | **no** — see §8 |

---

## 1. Sampling structure

### 1.1 The unit of independence is one pair

A **pair** is one arm-A process and one arm-B process, executed adjacent in time, with pair order alternated
`AB` / `BA` across pairs, and the order recorded on the pair record.

Most measured variance on a benchmark host is machine-level — thermal state, page cache, background load, CPU
frequency scaling. That component is *common-mode* across the two arms of a pair, and analysing the within-pair
difference cancels it. `P` pairs is honestly `n = P` independent differences from `P` fresh process pairs.

This is why the design is not block-structured. Eight samples spread across two processes has a true independent-unit
count closer to two; treating them as eight is pseudo-replication.

### 1.2 Within a process

Each process runs **1 unmeasured warm-up sample + k = 3 measured samples**, matching the existing
`BenchmarkRunner` convention of an unmeasured sample 0.

The three measured samples collapse to **one value per arm per pair** by **median**.

Rationale for `k = 3`: the median of three is robust to a single within-process spike (GC, JIT, page fault), which is
the failure mode replication inside a process can actually fix. Beyond that, extra within-process samples buy little —
they are correlated with each other and do not reduce the between-process component that dominates the pair
difference. Budget spent past `k = 3` is better spent on more pairs.

`k` is configurable, but changing it invalidates any stored A/A band, because the band is a property of the whole
process structure, not of the estimator alone. Bands record `k` and are rejected on mismatch.

### 1.3 Execution cost

Per pair: `2 arms x (1 + k) = 8` scenario executions. At the default `P = 8`, a comparison run is **64 executions**,
against today's single-arm run of 9. With the permitted escalation to `P = 16` it is **128**, and 128 is the number the
budget must be planned against — not 64.

`P` and `k` are parameters, not constants, and neither is frozen until W1 reports the stage-1 artifact copy cost.

**`k` is not a budget lever after calibration.** Changing `k` invalidates every stored A/A band (§1.2), so cutting it
under budget pressure discards roughly 192 executions of calibration to save 64. The correct sequence is: choose `k`
from a cheap pilot **before** calibrating, then freeze it. Once a band exists, the only lever is `P`, and reducing `P`
below 6 stops the run from producing a verdict at all.

---

## 2. Estimator: per-pair log-ratio

For pair `i` with collapsed values `A_i`, `B_i` (milliseconds, both > 0):

```
d_i = ln(B_i / A_i)
```

Analysed on the log scale, not as a raw millisecond difference. Performance noise is multiplicative — a slow machine
inflates both arms proportionally — so the log-ratio makes noise roughly symmetric, is scale-free with respect to the
scenario's absolute duration, and converts directly to a percentage.

`d_i` is defined by **arm identity, not by run order**. A `BA`-ordered pair is not sign-flipped; order is carried
separately as a covariate for the validity check in §6.3.

Reported point estimate:

```
d_median = median(d_1 .. d_P)
percentChange = (exp(d_median) - 1) * 100
```

The median of the per-pair log-ratios is the headline. The mean of logs (geometric mean ratio) is also reported, for
reference and because a large median/mean divergence is itself a signal that one pair is behaving oddly.

Sign convention: **positive means arm B is slower than arm A.** Stated once in the report header, never inferred.

---

## 3. Number of pairs and the stopping rule

- Default `P = 8`.
- Minimum valid `P = 6`. Below that the run reports `insufficient-pairs` and emits no verdict.
- Exactly **one** pre-declared escalation is permitted, to `P = 16`.

### 3.1 The rule

1. Run 8 pairs. Analyse once.
2. Escalate to 16 pairs and analyse once more **only when the consistency gate failed while the magnitude gate
   passed** (§5.2). Every other `inconclusive` terminates at look 1.
3. No other analysis points exist. In particular there is **no early stop on a significant result** — that is the
   mechanism that inflates the false-positive rate, and it is prohibited.

The escalation trigger is deliberately narrower than "any `inconclusive`". The consistency gate is the binding
constraint across the usable effect range (§4.4), and escalation is precisely what relaxes it — from unanimity at 8
pairs to 14/16. Escalating on a magnitude failure instead spends 64 additional executions on the gate that was not the
obstacle. Narrowing the continuation set also strengthens the family-wise argument rather than weakening it.

One measurement must not be used to justify this, because it is an identity rather than a finding: magnitude power
evaluated **at the band** is ~50% at every pair count. The band is the 95th percentile of the null median and the
median is centred on the true effect, so `mu = band` is a coin flip by construction. Comparing that figure across `P`
appears to show escalation achieving nothing; it only shows that each `P` is being evaluated at a *different* effect
size, since the band itself tightens with `P`. At a **fixed** effect, escalation roughly doubles detection
(0.230 to 0.517 at `mu = 1.0`). A regression test pins this so the mistake is not re-derived.

Because the only permitted continuation is on a pre-declared subset of `inconclusive`, and both looks use gates
declared in advance (§5.2), the family-wise false-positive rate is bounded by the union of the two look-level rates:
`0.0078125 + 0.00418... = 0.0120`, comfortably under 0.05.

These two numbers are the **achieved** levels of the count-based gates in §5.2, not thresholds chosen by hand. Both
are the largest achievable exact binomial level at or below the single declared target of `0.01`. There is no separate
`0.01` threshold anywhere in the implementation.

### 3.2 Failed pairs

If either arm of a pair fails (crash, timeout, verification failure), the **whole pair is discarded** — never one
arm of it. Discarded pairs are recorded with their reason and are not replaced silently; the run either reaches
`P >= 6` valid pairs or reports `insufficient-pairs`.

**No pair is ever discarded for being extreme.** There is no outlier rule at the pair level. Within-process spikes
are handled by the median-of-3 collapse; discarding pairs after seeing them is a researcher degree of freedom and is
not available.

---

## 4. Uncertainty

Three quantities are computed. They do different jobs and are not interchangeable.

### 4.1 The A/A band — primary, and the only thing a verdict may be issued against

Obtained by running the comparison with **identical code in both arms**. The true `d` is exactly zero, so the
observed spread *is* the noise floor for that environment, measured on real hardware with the real process structure.

The band must be the null distribution of **the statistic the gate actually tests**. The gate tests `d_median`, a
statistic over `P` pairs. The 95th percentile of individual `|d_i|` is therefore the wrong scale: the spread of a
median shrinks with `P`, while an individual-pair quantile does not shrink at all. Using the individual-pair quantile
makes the gate roughly twice as loose as it should be.

The band for an environment class at pair count `P` is the **95th percentile of `|median|` under resampling `P` pairs
with replacement from the accumulated A/A pool** — 10,000 resamples, fixed seed.

Measured against a simulated pool (units of the per-pair sd of `d`):

| definition | value |
|---|---|
| 95th pct of `\|d_i\|` — *not* the gate | 1.95 |
| 95th pct of `\|median of 8\|`, pool = 24 | 0.87 |
| same, pool = 24,000 | 0.81 |

Same executions, same pool, a threshold roughly 2.2x tighter. A 24-pair pool lands slightly conservative relative to
the asymptote, which is the correct direction to err.

The 95th percentile of individual `|d_i|` is still computed and printed **as a diagnostic**, next to the observed
maximum. It describes per-pair spread, which is genuinely useful for spotting a misbehaving environment. It is not a
gate. Note that at a 24-pair pool this diagnostic is itself a coarse order statistic and reads low (~1.75 against an
asymptote of ~1.95); it should not be read as a stable quantile.

**A band cannot be stored as a single number.** Because the median-null depends on `P`, the band at `P = 16` is
legitimately tighter than at `P = 8`. Storage therefore keeps the **raw accumulated observations**, and the band is
derived per-`P` at analysis time. A band frozen at `P = 8` and reused at `P = 16` would silently under-detect.

Accumulation requirements:

| Pairs accumulated | Runs | Band status |
|---|---|---|
| `< 16` | — | none — comparisons in that environment are `uncalibrated` |
| `16 .. 23` | `>= 2` | **provisional** — verdicts permitted but flagged `provisional-band` in every report |
| `>= 24` | `>= 3` | established |

The `>= 3 separate runs` requirement exists so the band captures between-run drift (process start, page cache state,
machine uptime) and not just within-run jitter.

### 4.2 Bootstrap interval — indicative, printed, never the verdict driver

Percentile bootstrap of `d_median`: 10,000 resamples of the `P` pairs with replacement, fixed seed, so a report is
reproducible from its own sample file.

At `P = 8` the bootstrap distribution of a median is chunky and the interval is coarse. It is printed to convey the
spread of *this* run, and it participates in the equivalence test (§5.3), but it does not by itself establish a
change.

### 4.3 Sign test — exact, assumption-free

Count of positive `d_i` against `Binomial(P, 0.5)`, two-sided. This makes no distributional assumption at all and is
exact at these sample sizes, which is precisely why it is in the verdict rule.

**The rule is expressed in counts, never in a transcribed decimal p-value.** One target level is declared
(`0.01`); the required count is the smallest agreeing count whose exact two-sided tail is at or below it; the p is
computed for reporting only.

This is not a stylistic preference. The exact unanimous level at `P = 8` is `2 x 0.5^8 = 0.0078125`, and a threshold
written as `<= 0.0078` excludes it — making `regressed` and `improved` unreachable at `P = 8` at any effect size. If
no count achieves the target (as at `P = 4`, where unanimity is only `0.125`), the implementation raises an
`unachievable` flag rather than silently never firing.

| P | required split | exact two-sided p |
|---|---|---|
| 8 | 8/8 | 0.0078125 |
| 8 | 7/8 | 0.0703125 — **not** sufficient |
| 16 | 14/16 | 0.00418... |
| 16 | 13/16 | 0.02127... — not sufficient |

### 4.4 Detectability — and why the band is not "the MDE"

The band is the smallest `|d_median|` that can clear the magnitude gate. It is **not** the effect size the harness
reliably detects, and labelling it "MDE" invites exactly that misreading.

At a true effect exactly equal to the band, measured through this implementation:

| gate | power at `mu = band` |
|---|---|
| magnitude alone | ~50% |
| magnitude **and** consistency (what a verdict requires) | ~13% |

Full characterization, `P = 8`, effects in units of the per-pair sd, 4,000 trials through the shipped verdict rule
(pinned by tests in `comparison/comparison.quick-unit.ts`):

| true effect | magnitude | consistency | **verdict** |
|---|---|---|---|
| 0 (A/A) | 0.048 | 0.009 | **0.002** |
| 0.5 | 0.237 | 0.052 | **0.032** |
| 0.81 (= band) | 0.509 | 0.160 | **0.127** |
| 1.0 | 0.687 | 0.260 | **0.230** |
| 1.5 | 0.953 | 0.575 | **0.566** |
| 2.0 | 0.997 | 0.837 | **0.836** |

Two things follow. The **consistency gate is binding everywhere** — the verdict column tracks it, not the magnitude
column — which is why escalation, whose whole effect is to relax unanimity to 14/16, is the lever that matters. And a
reader told "MDE 4%" will assume 4% regressions are caught; at the band they are caught about an eighth of the time.

Reports therefore print the band as a **noise-floor scale with its power annotated**, plus the ~80%-power point, and
never as a bare "MDE".

**The band is printed on every comparison report.** When the environment is uncalibrated, it prints as
`unknown (uncalibrated)` accompanied by the bootstrap half-width explicitly labelled *within-run spread, not a noise
floor*. A within-run spread is not a noise floor and the report never lets the two be confused.

---

## 5. Verdict rule

### 5.1 Statuses

| Status | Meaning |
|---|---|
| `regressed` | B is slower than A by more than the noise floor, consistently |
| `improved` | B is faster than A by more than the noise floor, consistently |
| `unchanged` | equivalence established — the difference is demonstrably smaller than the declared margin |
| `inconclusive` | cannot distinguish. **Not** a pass. |
| `uncalibrated` | no A/A band for this environment class — descriptive output only, no verdict |
| `insufficient-pairs` | fewer than 6 valid pairs |
| `invalid` | harness validity failure (§6) — the numbers are not to be used |

`unchanged` and `inconclusive` are distinct and must never be collapsed. "We showed it did not move" and "we could
not tell" are different claims, and reporting the second as the first is how a regression ships.

### 5.2 Change (`regressed` / `improved`)

Both gates must hold. Either alone is insufficient.

1. **Magnitude** — `|d_median| > band`, where the band is derived at this run's own `P` (§4.1).
2. **Consistency** — agreeing sign count at or above the required count for this `P`: **8/8** at look 1, **14/16** at
   look 2. Ties contribute to neither side and reduce the effective pair count.

Direction is the sign of `d_median`.

The magnitude gate is what makes the claim "outside the measured A/A band for this environment" — an empirical
statement about this hardware. A 95%-confidence claim derived from normal theory at n = 8 would not be defensible and
is not made anywhere in the output.

### 5.3 Equivalence (`unchanged`)

`unchanged` requires the **bootstrap interval to lie entirely inside `+/- margin`**, where `margin` is a
**declared equivalence margin** — not the noise floor. This is the direction that legitimately needs an interval: to
assert "no meaningful change" you must show the plausible range excludes meaningful change, which a point estimate
cannot do.

**The margin and the band answer different questions and must not be the same number.** The band says what the
environment *can resolve*; the margin says what the team *would act on*. Using the band for both makes `unchanged`
hostage to how quiet the machine happened to be — reachable only ~27% of the time at `P = 8` against a correctly
computed band, which makes `inconclusive` the modal outcome for a genuinely unchanged system.

Reachability against a declared, fixed margin:

| margin | `unchanged` at `P = 8` | at `P = 16` |
|---|---|---|
| 1.0x band | 41% | 80% |
| 1.5x band | 86% | 99.6% |
| 2.0x band | 98% | 100% |

The margin is a domain judgement — "we do not act on anything under X%" — and is **declared as configuration, not
derived**. Until one is declared, `unchanged` is unreachable by construction and the verdict is `inconclusive` with
the reason recorded as no declared margin.

**The declared margin is 5%** (`defaultEquivalenceMarginPercent`). `unchanged` therefore means "demonstrably smaller
than 5%", which is a claim about relevance, not about how quiet the machine was.

The report prints the **declared margin and the measured detectability side by side**, because their relationship is
what tells the reader whether the run could have answered the question at all. Where the floor exceeds the margin, the
environment cannot resolve what we care about, and that is stated rather than papered over.

Note this 5% coincides with the existing `classifyVariance` constant (§9), which makes retiring that constant a
clarification rather than the introduction of a second unexplained number. The two are **not** the same quantity: one
is a declared relevance threshold on a *difference between arms*, the other is a hardcoded dispersion threshold on a
*single run*. They agree numerically today by intention, and nothing may couple them.

**If the declared margin falls below the measured noise floor, the output is `inconclusive`** with an explicit note
that this environment cannot resolve the declared margin. The margin is never widened to make it reachable.

### 5.4 Everything else

`inconclusive`. In particular: a large `|d_median|` with a split sign count is inconclusive, not a regression, and a
tiny `|d_median|` with a wide interval is inconclusive, not `unchanged`.

---

## 6. Harness validity — checked before any verdict

A validity failure yields `invalid` and suppresses the verdict entirely. These are not statistical outcomes; they
mean the measurement was not of what it claims to measure.

### 6.1 Core-backend match

Both arms must resolve the **same exact** `@itwin/core-backend` version. A mismatch is rejected, not recorded and
continued. The package itself refuses unsupported transformer/core-backend peer combinations unless explicitly
bypassed (`packages/transformer/src/imodel-transformer.ts:43-52`); stamping a version into a report does not make an
invalid comparison valid. Core-backend-vs-core-backend comparison is out of scope for the initial implementation.

**Co-resolution across subprocesses is confirmed achievable, and must be constructed.** Verified in this workspace by
building two arm packages carrying *different* transformer versions, linking each arm's declared peers to the
harness's resolved realpaths, and loading each in its own process: both resolved one byte-identical core-backend
realpath at one version. Three findings this gate depends on:

- pnpm does **not** hoist `@itwin/core-backend` to the workspace root, so an arm resolves nothing by default —
  including an arm placed inside the workspace. Bare resolution fails with `MODULE_NOT_FOUND`.
- Only **peers** may be redirected. The arm's own runtime dependencies (`semver` today) must come from the arm's own
  install; loading fails outright if that install has not been run. Overriding them would replace part of what is
  under test.
- Identity is checked by **realpath, not version string**. Two copies of one version are two distinct trees on disk,
  and within any single process `IModelHost` and the native addon must be loaded exactly once.

An in-process alternative exists — the weekly harness loads every arm into one process, which forces a single
core-backend by construction — and it is **not** adopted. Subprocess isolation here is not about permitting arms to
differ in core-backend; §6.1 forbids that. It is about **measurement independence**: the pair-as-unit design requires
each observation to come from fresh process state, and in-process arms would have the second arm measured on a heap
the first had already warmed. Order alternation cancels the mean of such an effect while inflating the per-pair
spread, and per-pair spread is the quantity the entire design exists to reduce.

### 6.2 Behaviour equality

Cross-arm `semanticDigest` equality is a gate. Arms that computed different results are not comparable on speed, and
a speed number from such a run is meaningless regardless of how clean its statistics look.

### 6.3 Order effect (A/A only)

Because pair order alternates, an A/A run can test whether position-in-pair matters: apply the same sign test to the
per-pair `ln(second / first)`. A significant result means a systematic warm-up or ordering defect in the harness, not
noise. It fails calibration — the resulting band is not stored, and the defect is fixed rather than absorbed into a
wider band.

**This test runs on the accumulated pool at `>= 24` pairs, never per-run.** Applied to a single 8-pair run it is the
same unanimity requirement as the main sign test (§4.3), so it has essentially no power and a real ordering defect
would pass calibration unnoticed — the precise failure the check exists to catch. Deferring it to the pool is what
gives it enough pairs to fire.

### 6.4 Fixture identity

Where both arms consume the same stage-1 artifact, that artifact's `recipeHash` must match across arms. Mismatch is
`invalid`.

Not every scenario can share an artifact. `incremental-synchronization` measures `process()` with
`argsForProcessChanges`, which re-enters `BriefcaseManager` and needs a live target briefcase to push provenance to;
it stays on a live-hub topology and rebuilds per sample, so **each arm builds its own fixture**.

That remains valid here, for reasons that were checked rather than assumed: fixture generation is deterministic (no
`Math.random`, `Guid.createValue`, `randomUUID` or `Date.now()` in the recipe, scenario or validation paths), and
`semanticDigest` hashes sorted labels, payloads, sequences, geometry and relationship tuples while deliberately
excluding ElementIds, briefcase ids and GUIDs. Independent rebuilds are therefore content-identical, and the cross-arm
digest gate of §6.2 is what carries the guarantee in place of artifact identity.

What independent rebuild costs is not correctness but resolution — see §7.1. It raises that scenario's noise floor,
which is precisely why its band may not travel to another scenario.

### 6.5 The harness owns the measured region, not the arm

Arms are supplied as `TestTransformerModule`-shaped modules — the same contract the weekly regression harness already
dynamic-imports through `EXTRA_TRANSFORMERS`. Reusing that shape means the arms that already exist
(`NativeTransformer`, `RawForkOperations`, `RawForkCreateFedGuids`) are usable here without modification, and a new arm
written for either suite works in both. The quick contract adds two optional members and takes nothing away:

- `createChangeProcessingTransform`, because the shared contract covers identity and fork-init only, while the quick
  scenario measures `process()` under `argsForProcessChanges`.
- `dispose` on the runner.

The second is a correctness matter, not tidiness. The existing implementations perform teardown **inside** `run()` —
`NativeTransformer` calls `transformer.dispose()` and `editTxn.end()` there — which means the arm, not the harness,
decides what falls inside the timed region. That is harmless for a weekly regression number and unacceptable for a
comparison resolving a few percent: two arms could differ in what they fold into `run()`, and that difference would be
indistinguishable from a real effect. Note that A/A calibration cannot catch it, because A/A runs identical code in
both arms and so has no cross-arm boundary difference to expose.

An arm supplying `dispose` has its teardown run outside the timed region. An arm that does not is still accepted — only
the arm knows what its teardown is — and the report records `teardownInMeasuredRegion` for it. A comparison whose arms
disagree on that flag is reported, because the arms are measuring different regions.

---

## 7. Calibration key — what a band and a baseline are allowed to describe

A baseline or band captured on one machine class is noise on another: hosted Ubuntu CV was measured at 1.03%, while
six local macOS runs measured 6.32, 3.71, 3.00, 4.80, 2.17, 6.43 percent.

Environment class key is a stable hash over: platform, arch, CPU model string, logical CPU count, total memory
bucketed to a power of two, major Node version, and a CI marker (runner label when `GITHUB_ACTIONS` is set, else
`local`). The human-readable components are stored next to the hash so a mismatch can be explained rather than just
detected.

### 7.1 The environment is not sufficient on its own

**Bands are keyed by `scenarioId + recipeHash + environmentClass`, the same key as baselines** — plus `k` and band
kind (§1.2, §8), which describe the process structure the pool was collected under.

The noise floor is a property of the *measured region*, not only of the machine. Two scenarios on one machine can have
materially different floors:

- `incremental-synchronization` runs on a live-hub topology and rebuilds its fixture per sample. That rebuild sits
  immediately before the timed region — outside it, so it does not enter `wallMilliseconds`, but it lengthens the
  process, widens the time gap between the A and B measurements of a pair, and leaves different page-cache and GC
  state going into the measured call. It is symmetric across arms, so it does not bias the estimate; it raises the
  floor.
- A scenario consuming a byte-identical stage-1 artifact has none of that.

Transporting a band between them silently manufactures verdicts in one direction and suppresses real ones in the
other. `recipeHash` is included for the same reason it is on baselines: a larger recipe is a longer measured region
and a different noise profile.

A lookup miss is `uncalibrated`. There is **no fall-back across scenarios**, exactly as there is none across machines.

### 7.2 Cost consequence, stated rather than discovered

Calibration is **per scenario, not per machine**. At a 24-pair established band that is roughly 192 executions for
each scenario that needs a verdict.

This is a deliberate cost, and the way to manage it is to decide which scenarios need a verdict at all — the rest stay
descriptive, which is an honest output. It is **not** to be managed by sharing a band.

### 7.3 Measuring the rebuild's contribution instead of arguing about it

`reconstructionMilliseconds` is already captured per sample (`BenchmarkRunner.ts`). On the first A/A run, regress
`wallMilliseconds` on it to quantify how much of the floor the per-sample rebuild actually accounts for. This is free
data already being collected, and it converts a plausible mechanism into a measured one.

---

## 8. Baseline comparison is weaker, and says so

Run-vs-baseline uses the same estimator and the same report shape, but **the pairing does not exist** — the baseline
was measured at a different time, on a different process, possibly a different day. The common-mode cancellation that
justifies the paired band is unavailable.

Consequences, all of which the implementation must honour:

- A/A calibration produces **two** bands per environment class: a **paired** band (within-run A/B) and an
  **unpaired** band (A/A run vs. a stored A/A run, same environment class, different run).
- The unpaired band will be materially wider. Baseline comparison uses it. Its detectable effect is correspondingly
  larger and is printed as such.
- The sign test is unavailable — there are no pairs. Baseline verdicts rest on the magnitude gate against the
  unpaired band plus the bootstrap interval over this run's samples, and are therefore weaker. Reports label baseline
  verdicts `unpaired`.

Baseline records are keyed by `scenarioId + recipeHash + environmentClass` and additionally record `k`, `P`,
resolved transformer and core-backend versions, and the harness commit.

---

## 9. Consequence for the existing CV threshold

`classifyVariance` currently flags `unstable` above a hardcoded CV of 5% (`BenchmarkReporter.ts:16`). Observed local
runs measured 6.32% and 6.43%, so the constant already misfires on developer hardware — it is a live bug, not a
tuning preference.

Once an A/A band exists for an environment class, the stability judgement is made against that band. The hardcoded
constant is retired or environment-scoped at that point, and not before — removing it earlier would leave no
stability signal at all.

---

## 10. What this specification does not claim

- It does not claim a 95% confidence interval on the difference. It claims "outside / inside the measured A/A band
  for this environment", which is an empirical statement about observed hardware behaviour.
- Pairing cancels only common-mode noise. Process-internal noise — GC timing, JIT variation, allocation patterns — is
  uncorrelated between arms and survives the subtraction. The A/A run is what measures the split.
- If A/A comes back near 6%, the correct response is **more pairs or a longer measured region**, not a tighter
  threshold. A threshold below the measured noise floor manufactures verdicts; it does not detect regressions.
- Every noise-floor figure anywhere in this design is provisional until A/A calibration has actually run.
