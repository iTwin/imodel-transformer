---
name: code-review
description: Use when GitHub Copilot Code Review is performing an automated pull request review in this repository. Apply the correctness, performance-validity, and thermo-nuclear maintainability gates.
license: See the repository LICENSE.md
---

<!-- Attribution: The thermo-nuclear maintainability guidance in this skill was adapted from Cursor Team Kit's skill: https://github.com/cursor/plugins/blob/main/cursor-team-kit/skills/thermo-nuclear-code-quality-review/SKILL.md -->

# Automated Copilot Code Review

This recipe produces evidence-backed findings while leaving implementation, tests, and review artifacts unchanged.

## 1. Pin the review context

- Use the pull request metadata as the source of truth for base SHA, head SHA, title, description, and changed files.
- Inspect the complete base-to-head diff and every commit in the review range.
- Apply the root instructions and every path-specific instruction file matching a changed path.
- Follow the contract-document pointers named by the applicable instructions when a changed surface reaches them.

**Completion check:** The base SHA, head SHA, changed paths, applicable instructions, contract documents, and validation scope are recorded.

## 2. Build the change model

- Trace changed entry points through callers, tests, workflows, package boundaries, and release metadata.
- State the invariant the pull request changes, the owner of that invariant, and the lifecycle and failure boundaries that preserve it.
- Identify affected public APIs, transaction boundaries, mutable state, concurrency, caches, measured operations, and generated artifacts.

**Completion check:** Every changed surface has an owner, an invariant, and an affected success or failure path.

## 3. Apply the review gates

### Correctness

- Compare the changed behavior with its callers, existing paths, tests, and documented contract.
- Trace identity, ownership, ordering, cardinality, state cleanup, error propagation, and retry behavior across every affected boundary.
- Exercise relevant empty, duplicate, missing, deleted, excluded, repeated, large, concurrent, and failure inputs.
- For transformer or performance-test changes, apply the matching path-specific instructions as the source of detailed domain checks.

### Performance validity

- For performance claims, apply the matching performance-review instructions and evaluate measured-operation scope, workload representativeness, comparison isolation, provenance, semantic equivalence, metric interpretation, and failure evidence.

### Thermo-nuclear maintainability

Look for the **code-judo** move: a restructuring that removes concepts, branches, state, or indirection instead of relocating the same complexity.

- Raise a maintainability finding for ad-hoc conditionals, one-off flags, nullable modes, scattered special cases, duplicated helpers, or feature logic in the wrong layer when a simpler ownership model is available.
- Treat a PR-introduced file above 1,000 lines as a decomposition candidate; accept cohesion when the structure has a clear reason to remain together.
- Prefer direct, boring, typed code over magic, identity abstractions, cast-heavy contracts, `any`, unclear `unknown`, and unnecessary optionality.
- Prefer canonical repository helpers and ownership boundaries over bespoke near-duplicates and shared-path leakage.
- Prefer parallel orchestration and atomic updates when they make ownership and failure behavior clearer.
- Raise a structural finding when the PR makes surrounding code materially more coupled, stateful, or difficult to scan despite preserving apparent behavior.

### Contract and release impact

- For public behavior, APIs, dependencies, workflows, or performance-test contracts, verify generated reports, change files, package metadata, documentation, and release notes as applicable.
- Use the smallest relevant validation for the changed surface and record environment or credential limitations.

**Completion check:** Each applicable gate has been considered for every changed surface, and the review distinguishes correctness, performance validity, maintainability, and release findings.

## 4. Construct evidence-backed findings

- Report actionable findings anchored in changed code; use pre-existing code as context only when the pull request changes it or materially worsens it.
- Give each finding a severity, exact file and line anchor, trigger or input sequence, impact, evidence, smallest complete remediation, confidence, and validation limitation.
- Separate confirmed defects from risks requiring more evidence and from questions that need author clarification.
- Prefer a small set of high-confidence findings over stylistic observations or unsupported hypotheses.

**Completion check:** Every finding is independently actionable from its anchor and evidence, and every suspected issue that lacks evidence is labeled as uncertainty rather than presented as a defect.

## 5. Close the review

- Account for every changed file, including tests, workflows, generated API reports, change files, and documentation.
- State the validation commands and relevant results.
- State remaining uncertainty, missing environment access, and the surfaces inspected when no actionable finding remains.

**Completion check:** The final review contains findings or an evidence-backed no-finding result, validation status, and explicit residual uncertainty.
