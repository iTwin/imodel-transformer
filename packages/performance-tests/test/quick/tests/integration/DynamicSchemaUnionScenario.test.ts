/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { dynamicSchemaUnionMediumDescriptor } from "../../src/catalogs/FixtureCatalog.js";
import { BenchmarkRunner } from "../../src/framework/BenchmarkRunner.js";
import { dynamicSchemaUnionScenario } from "../../src/scenarios/dynamicSchemaUnion.js";

/**
 * End-to-end proof that the scenario measures only `processSchemas()` and then performs complete
 * semantic validation outside that measured region. `BenchmarkRunner` owns `IModelHost` startup
 * and shutdown for this run; this topology needs no `HubMock`.
 */
describe("dynamic-schema-union scenario", () => {
  it("measures processSchemas and validates the complete resulting union", async () => {
    const outputDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-perf-dynamic-schema-union-")
    );
    try {
      const samples = await new BenchmarkRunner(
        dynamicSchemaUnionMediumDescriptor,
        outputDir,
        dynamicSchemaUnionScenario
      ).run(1);

      // One warm-up plus one measured sample.
      expect(samples).to.have.lengthOf(2);
      expect(samples.map((sample) => sample.measured)).to.deep.equal([
        false,
        true,
      ]);
      expect(samples.map((sample) => sample.topology)).to.deep.equal([
        "snapshot-schema-pair",
        "snapshot-schema-pair",
      ]);
      expect(samples.map((sample) => sample.scenarioId)).to.deep.equal([
        "dynamic-schema-union",
        "dynamic-schema-union",
      ]);
      expect(samples.map((sample) => sample.fixtureId)).to.deep.equal([
        "dynamic-schema-union-medium",
        "dynamic-schema-union-medium",
      ]);

      // finish() runs a complete semantic validation of the union after measure(); if it did not
      // pass, BenchmarkRunner.run() would have rejected instead of returning samples.
      for (const sample of samples) {
        expect(Number.isFinite(sample.wallMilliseconds)).to.be.true;
        expect(Number.isFinite(sample.verificationMilliseconds)).to.be.true;
        expect(sample.semanticDigest).to.be.a("string");
        expect(sample.semanticDigest.length).to.be.greaterThan(0);
      }
      // Deterministic: warm-up and the measured sample must observe the identical union.
      expect(
        new Set(samples.map((sample) => sample.semanticDigest)).size
      ).to.equal(1);
    } finally {
      fs.rmSync(outputDir, { recursive: true, force: true });
    }
  });
});
