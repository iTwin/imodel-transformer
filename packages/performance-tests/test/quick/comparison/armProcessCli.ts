/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import {
  aliasHarnessCoreBackendToArm,
  ArmSpec,
  resolveArmSpec,
} from "./ArmModule";

interface ArmRequestFile {
  readonly arm: ArmSpec;
}

function argument(name: string): string {
  const index = process.argv.indexOf(`--${name}`);
  const value = index < 0 ? undefined : process.argv[index + 1];
  if (!value) throw new Error(`Missing required argument --${name}`);
  return value;
}

async function main(): Promise<void> {
  const requestFile = argument("request");
  const outputFile = argument("output");
  const request = JSON.parse(
    fs.readFileSync(requestFile, "utf8")
  ) as ArmRequestFile;
  aliasHarnessCoreBackendToArm(resolveArmSpec(request.arm));
  const { runArm } = await import("./ComparisonRunner");
  const result = await runArm(request as Parameters<typeof runArm>[0]);
  fs.mkdirSync(path.dirname(outputFile), { recursive: true });
  fs.writeFileSync(outputFile, `${JSON.stringify(result, undefined, 2)}\n`);
}

void main().catch((error) => {
  process.stderr.write(
    `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`
  );
  process.exitCode = 1;
});
