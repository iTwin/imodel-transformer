/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const workflow = fs.readFileSync(
  path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../../../../../..",
    ".github/workflows/quick-performance-comparison.yml"
  ),
  "utf8"
);

describe("quick performance comparison workflow", () => {
  it("is manual A/B-only automation with an immutable candidate fallback", () => {
    expect(workflow).toContain("workflow_dispatch:");
    expect(workflow).not.toContain("pull_request:");
    expect(workflow).not.toMatch(/calibrat/i);
    expect(
      workflow.match(/inputs\.candidate_ref \|\| github\.sha/g)
    ).toHaveLength(3);
    expect(workflow).toContain("pair_order:");
  });

  it("runs only the compiled CLI and publishes one comparison artifact", () => {
    expect(workflow).toContain(
      "test/quick/runtime/.compiled/quick/src/cli/comparisonCli.js"
    );
    expect(workflow).toContain("run-pair");
    expect(workflow).toContain("QuickPerformanceComparison");
    expect(workflow).not.toContain("test:comparison-smoke");
    expect(workflow).not.toMatch(/\bts-node\b|\bmocha\b|\bchai\b/i);
  });
});
