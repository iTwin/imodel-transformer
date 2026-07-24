/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { BenchmarkReporter } from "./BenchmarkReporter";
import {
  BenchmarkRunner,
  prepareBenchmarkOutputDirectory,
} from "./BenchmarkRunner";
import { balancedIncrementalDescriptor } from "./FixtureCatalog";
import { getScenarioDefinition } from "./ScenarioCatalog";

function writeManifest(outputDir: string): void {
  fs.writeFileSync(
    path.join(outputDir, "manifest.json"),
    `${JSON.stringify(balancedIncrementalDescriptor, undefined, 2)}\n`
  );
}

async function main() {
  const command = process.argv[2];
  const outputDir =
    process.env.QUICK_PERF_OUTPUT ??
    path.join(__dirname, ".quick-output", balancedIncrementalDescriptor.id);
  if (command === "build-fixture") {
    prepareBenchmarkOutputDirectory(outputDir);
    writeManifest(outputDir);
    return;
  }
  if (command === "verify-fixture") {
    const scenario = getScenarioDefinition(process.env.QUICK_PERF_SCENARIO);
    const samples = await new BenchmarkRunner(
      balancedIncrementalDescriptor,
      outputDir,
      scenario
    ).run(1);
    if (new Set(samples.map((sample) => sample.semanticDigest)).size !== 1)
      throw new Error("Fixture reconstruction is not deterministic");
    BenchmarkReporter.write(outputDir, samples);
    writeManifest(outputDir);
    return;
  }
  throw new Error(`Unknown quick fixture command: ${command ?? "<missing>"}`);
}

void main().catch((error) => {
  process.stderr.write(`${String(error)}\n`);
  process.exitCode = 1;
});
