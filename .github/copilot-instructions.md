# Repository Copilot Context

- Apply `AGENTS.md` and the path-specific instruction file for each changed path before making or reviewing changes.
- Treat package documentation, package scripts, CI workflows, generated API reports, tests, and change files as contract evidence; inspect the owning file when a rule matters.
- `packages/transformer` owns the published transformer library and its test suite. `packages/performance-tests` owns the credential-free quick and credential-dependent weekly performance suites.

## Automated pull-request review

- When GitHub Copilot Code Review is performing an automated pull-request review, use the `code-review` skill in `.github/skills/code-review`.
- Human-led reviews, walkthroughs, and implementation work follow their selected workflow; invoke the automated review skill explicitly when those tasks need its gates.
