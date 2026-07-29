/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { BenchmarkReporter } from "./BenchmarkReporter";
import { resolveBenchmarkRunFromEnvironment } from "./BenchmarkResolution";
import {
  BenchmarkRunner,
  prepareBenchmarkOutputDirectory,
} from "./BenchmarkRunner";
import { DatasetDescriptor } from "./DatasetDescriptor";
import { quickSourcePath } from "./quickPaths";

function writeManifest(outputDir: string, descriptor: DatasetDescriptor): void {
  fs.writeFileSync(
    path.join(outputDir, "manifest.json"),
    `${JSON.stringify(descriptor, undefined, 2)}\n`
  );
}

async function main() {
  const command = process.argv[2];
  const { descriptor, scenario } = resolveBenchmarkRunFromEnvironment();
  const outputDir =
    process.env.QUICK_PERF_OUTPUT ??
    quickSourcePath(".quick-output", descriptor.id);
  if (command === "build-fixture") {
    prepareBenchmarkOutputDirectory(outputDir);
    writeManifest(outputDir, descriptor);
    return;
  }
  if (command === "verify-fixture") {
    const samples = await new BenchmarkRunner(
      descriptor,
      outputDir,
      scenario
    ).run(1);
    if (new Set(samples.map((sample) => sample.semanticDigest)).size !== 1)
      throw new Error("Fixture reconstruction is not deterministic");
    BenchmarkReporter.write(outputDir, samples);
    writeManifest(outputDir, descriptor);
    return;
  }
  throw new Error(`Unknown quick fixture command: ${command ?? "<missing>"}`);
}

void main().catch((error) => {
  process.stderr.write(`${String(error)}\n`);
  process.exitCode = 1;
});
