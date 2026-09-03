/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { createHash } from "node:crypto";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as path from "node:path";

const localRequire = createRequire(import.meta.url);

export interface TransformerProvenance {
  readonly contentHash: string;
  readonly entryPoint: string;
  readonly version: string;
}

export type TransformerResolver = (packageName: string) => string;

function buildFiles(directory: string, relative = ""): string[] {
  return fs
    .readdirSync(path.join(directory, relative), { withFileTypes: true })
    .flatMap((entry) => {
      const entryRelative = path.join(relative, entry.name);
      return entry.isDirectory()
        ? buildFiles(directory, entryRelative)
        : [entryRelative];
    });
}

function transformerBuildContentHash(entryPoint: string): string {
  const buildDirectory = path.dirname(entryPoint);
  const hash = createHash("sha256");
  for (const relative of buildFiles(buildDirectory).sort()) {
    const contents = fs.readFileSync(path.join(buildDirectory, relative));
    hash.update(relative.replaceAll(path.sep, "/"));
    hash.update("\0");
    hash.update(String(contents.byteLength));
    hash.update("\0");
    hash.update(contents);
  }
  return hash.digest("hex");
}

/**
 * Resolve the transformer through the worker's module graph and prove it is the build from the
 * checkout assigned to that arm.
 */
export function resolveTransformerProvenance(
  expectedRootDirectory: string,
  resolvePackage: TransformerResolver = (packageName) =>
    localRequire.resolve(packageName)
): TransformerProvenance {
  const packageDirectory = path.join(
    path.resolve(expectedRootDirectory),
    "packages",
    "transformer"
  );
  const packageJson = JSON.parse(
    fs.readFileSync(path.join(packageDirectory, "package.json"), "utf8")
  ) as { main?: string; version?: string };
  if (
    typeof packageJson.main !== "string" ||
    typeof packageJson.version !== "string"
  )
    throw new Error(
      `Transformer package metadata is invalid below ${expectedRootDirectory}`
    );

  const expectedEntryPoint = fs.realpathSync(
    path.join(packageDirectory, packageJson.main)
  );
  const resolvedEntryPoint = fs.realpathSync(
    resolvePackage("@itwin/imodel-transformer")
  );
  if (resolvedEntryPoint !== expectedEntryPoint)
    throw new Error(
      `Worker resolved @itwin/imodel-transformer from ${resolvedEntryPoint}; expected ${expectedEntryPoint}`
    );

  return {
    contentHash: transformerBuildContentHash(resolvedEntryPoint),
    entryPoint: resolvedEntryPoint,
    version: packageJson.version,
  };
}
