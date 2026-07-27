/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import type { IModelTransformer } from "@itwin/imodel-transformer";

/**
 * Arm module contract for quick-performance comparison.
 *
 * Modelled on `TestTransformerModule`, which the weekly harness dynamic-imports
 * (`TransformerRegression.test.ts`). The difference is what varies: a quick-performance arm varies
 * the TRANSFORMER BUILD while holding core-backend fixed, so the contract is mostly about proving
 * that the fixed part really is fixed.
 *
 * Resolution behaviour this contract is built on, established empirically in this workspace:
 *
 * - The transformer declares `@itwin/core-backend` as a PEER dependency with no runtime
 *   dependency, so a built arm carries no core-backend of its own and must resolve one.
 * - An arm placed outside the workspace resolves nothing: `MODULE_NOT_FOUND`.
 * - An arm placed inside the workspace ALSO resolves nothing, because pnpm does not hoist
 *   `@itwin/core-backend` to the root `node_modules`.
 * - Linking the arm's declared peers to the harness's own resolved realpaths yields the same
 *   realpath, the same module instance, and the same `IModelHost` class -- and the transformer's
 *   built-in peer-version check passes.
 *
 * Co-resolution is therefore achievable but must be CONSTRUCTED. It is never assumed, and never
 * inferred from a version string: two identical version strings can still be two distinct module
 * instances, and `IModelHost` plus the native addon are process-global singletons that must not be
 * loaded twice.
 */

/** The transformer surface an arm must expose. Scenarios take this instead of a static import. */
export interface QuickArmTransformerApi {
  readonly IModelTransformer: typeof IModelTransformer;
}

export interface QuickArmModule {
  readonly transformer: QuickArmTransformerApi;
}

export interface ArmSpec {
  /** Stable identifier used in reports, e.g. `"A"`, `"B"`, or a git ref. */
  readonly id: string;
  /** Root of the built arm package: the directory containing its `package.json`. */
  readonly packageRoot: string;
  /** Entry point relative to `packageRoot`. */
  readonly entryPoint?: string;
  /** Human-readable provenance, e.g. the git ref the arm was built from. */
  readonly label?: string;
}

export interface ResolvedArm {
  readonly spec: ArmSpec;
  readonly module: QuickArmModule;
  readonly transformerVersion: string;
  readonly coreBackendVersion: string;
  readonly coreBackendRealPath: string;
}

function readPackageJson(packageRoot: string): {
  name?: string;
  version?: string;
  peerDependencies?: Record<string, string>;
} {
  const manifestPath = path.join(packageRoot, "package.json");
  if (!fs.existsSync(manifestPath))
    throw new Error(`Arm package has no package.json: ${manifestPath}`);
  return JSON.parse(fs.readFileSync(manifestPath, "utf8"));
}

function realResolve(specifier: string, fromDir: string): string {
  return fs.realpathSync(
    require.resolve(`${specifier}/package.json`, { paths: [fromDir] })
  );
}

/**
 * Point the arm's declared peer dependencies at the harness's own resolved copies.
 *
 * Only peers are overridden. The arm's genuine runtime dependencies stay with the arm's own
 * install, because those are part of what is under test; the peers are the shared substrate that
 * must be identical for the comparison to mean anything.
 */
export function linkArmPeerDependencies(
  packageRoot: string,
  harnessResolutionDir: string
): string[] {
  const peers = Object.keys(
    readPackageJson(packageRoot).peerDependencies ?? {}
  );
  const linked: string[] = [];
  for (const peer of peers) {
    const target = path.dirname(realResolve(peer, harnessResolutionDir));
    const link = path.join(packageRoot, "node_modules", peer);
    fs.mkdirSync(path.dirname(link), { recursive: true });
    if (fs.existsSync(link)) {
      if (fs.realpathSync(link) !== target)
        fs.rmSync(link, { recursive: true, force: true });
      else {
        linked.push(peer);
        continue;
      }
    }
    fs.symlinkSync(target, link, "junction");
    linked.push(peer);
  }
  return linked;
}

/**
 * Reject an arm whose core-backend is not the harness's core-backend.
 *
 * A mismatch is rejected outright rather than recorded and carried into the report. The package
 * itself refuses unsupported transformer/core-backend peer combinations unless explicitly bypassed
 * (`packages/transformer/src/imodel-transformer.ts`), and stamping a version into a report does not
 * make an invalid comparison valid.
 *
 * Identity is checked by realpath, not by version string, because two copies of the same version
 * are still two module instances and would mean two native addon loads.
 */
export function assertArmCoreBackendIdentity(
  armCoreBackendRealPath: string,
  harnessCoreBackendRealPath: string,
  armId: string
): void {
  if (armCoreBackendRealPath === harnessCoreBackendRealPath) return;
  throw new Error(
    `Arm "${armId}" resolves a different @itwin/core-backend than the harness.\n` +
      `  arm:     ${armCoreBackendRealPath}\n` +
      `  harness: ${harnessCoreBackendRealPath}\n` +
      "Comparison arms must share one exact core-backend. Two instances would dual-load the " +
      "native addon and the IModelHost singleton, and any timing difference would be " +
      "unattributable. Core-backend-vs-core-backend comparison is out of scope."
  );
}

/** Load one arm and prove it shares the harness's core-backend before returning it. */
export async function loadArm(
  spec: ArmSpec,
  harnessResolutionDir: string = __dirname
): Promise<ResolvedArm> {
  const packageRoot = path.resolve(spec.packageRoot);
  const manifest = readPackageJson(packageRoot);
  linkArmPeerDependencies(packageRoot, harnessResolutionDir);

  const harnessCoreBackend = realResolve(
    "@itwin/core-backend",
    harnessResolutionDir
  );
  const armCoreBackend = realResolve("@itwin/core-backend", packageRoot);
  assertArmCoreBackendIdentity(armCoreBackend, harnessCoreBackend, spec.id);

  const entry = path.join(
    packageRoot,
    spec.entryPoint ?? "lib/cjs/imodel-transformer.js"
  );
  if (!fs.existsSync(entry))
    throw new Error(`Arm "${spec.id}" has no entry point at ${entry}`);

  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const loaded = require(entry) as Partial<QuickArmTransformerApi>;
  if (typeof loaded.IModelTransformer !== "function")
    throw new Error(
      `Arm "${spec.id}" does not export IModelTransformer from ${entry}`
    );

  return {
    spec,
    module: { transformer: { IModelTransformer: loaded.IModelTransformer } },
    transformerVersion: manifest.version ?? "unknown",
    coreBackendVersion: JSON.parse(fs.readFileSync(armCoreBackend, "utf8"))
      .version,
    coreBackendRealPath: armCoreBackend,
  };
}

/**
 * Validate a pair of arms before any measurement.
 *
 * Both arms must already have been loaded against the same harness, so this is a cheap
 * cross-check that the pair is comparable at all.
 */
export function assertArmsComparable(
  armA: ResolvedArm,
  armB: ResolvedArm
): void {
  if (armA.coreBackendRealPath !== armB.coreBackendRealPath)
    throw new Error(
      `Arms "${armA.spec.id}" and "${armB.spec.id}" resolve different core-backend instances; ` +
        "the comparison is invalid."
    );
}
