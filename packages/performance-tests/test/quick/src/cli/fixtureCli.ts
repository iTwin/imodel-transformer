/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { BenchmarkReporter } from "../reporting/BenchmarkReporter.js";
import { resolveBenchmarkRunFromEnvironment } from "../framework/BenchmarkResolution.js";
import {
  BenchmarkRunner,
  prepareBenchmarkOutputDirectory,
} from "../framework/BenchmarkRunner.js";
import { FixtureDescriptor } from "../fixtures/FixtureDescriptor.js";
import { quickPath } from "../support/paths.js";

function writeManifest(outputDir: string, descriptor: FixtureDescriptor): void {
  fs.writeFileSync(
    path.join(outputDir, "manifest.json"),
    `${JSON.stringify(descriptor, undefined, 2)}\n`
  );
}

async function main() {
  const command = process.argv[2];
  const { descriptor, scenario } = resolveBenchmarkRunFromEnvironment();
  const outputDir =
    process.env.QUICK_PERF_OUTPUT ?? quickPath(".quick-output", descriptor.id);
  if (command === "build-fixture") {
    prepareBenchmarkOutputDirectory(outputDir);
    writeManifest(outputDir, descriptor);
    return;
  }
  if (command === "verify-fixture") {
    const started = process.hrtime.bigint();
    const samples = await new BenchmarkRunner(
      descriptor,
      outputDir,
      scenario
    ).run(1);
    const elapsedMilliseconds =
      Number(process.hrtime.bigint() - started) / 1_000_000;
    if (new Set(samples.map((sample) => sample.semanticDigest)).size !== 1)
      throw new Error("Fixture reconstruction is not deterministic");
    BenchmarkReporter.write(outputDir, samples, elapsedMilliseconds);
    writeManifest(outputDir, descriptor);
    return;
  }
  throw new Error(`Unknown quick fixture command: ${command ?? "<missing>"}`);
}

void main().catch((error) => {
  process.stderr.write(`${String(error)}\n`);
  process.exitCode = 1;
});
