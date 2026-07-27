/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import type { IModelDb } from "@itwin/core-backend";
import type {
  TestTransformerModule,
  TransformRunner,
} from "../../TestTransformerModule";

/**
 * Arm module contract for quick-performance comparison.
 *
 * This is a thin SUPERSET of `TestTransformerModule`, the contract the weekly regression harness
 * already dynamic-imports via `EXTRA_TRANSFORMERS` (`TransformerRegression.test.ts`). Taking that
 * shape rather than inventing a parallel one means the three arms that already exist --
 * `NativeTransformer`, `RawForkOperations`, `RawForkCreateFedGuids` -- are usable here immediately,
 * and an arm written for either suite works in both.
 *
 * Two additions, both optional, so every existing module stays valid:
 *
 * - `createChangeProcessingTransform`, because the existing contract covers identity and fork-init
 *   only, and the quick scenario measures `process()` under `argsForProcessChanges`.
 * - `dispose` on the runner. This one is not cosmetic. The existing implementations perform
 *   teardown INSIDE `run()` -- `NativeTransformer` calls `transformer.dispose()` and `editTxn.end()`
 *   there -- so the arm, not the harness, decides the boundary of the measured region. That is
 *   tolerable for a weekly regression number and not tolerable for a comparison resolving a few
 *   percent, because two arms could differ in what they fold into the timed region and the
 *   difference would be indistinguishable from a real effect. An arm that supplies `dispose` gets
 *   its teardown run outside the timed region; an arm that does not is still accepted, and the
 *   report records that its measurement includes teardown.
 *
 * Resolution behaviour this contract is built on, established empirically in this workspace:
 *
 * - The transformer declares `@itwin/core-backend` as a PEER dependency with no runtime
 *   dependency, so a built arm carries no core-backend of its own and must resolve one.
 * - An arm placed outside the workspace resolves nothing: `MODULE_NOT_FOUND`.
 * - An arm placed inside the workspace ALSO resolves nothing, because pnpm does not hoist
 *   `@itwin/core-backend` to the root `node_modules`.
 * - Linking the arm's declared peers to the harness's own resolved realpaths yields the same
 *   realpath and the same core-backend version. Verified across two arms carrying DIFFERENT
 *   transformer versions loaded in separate processes: both resolved one identical realpath.
 * - Only peers may be redirected. The arm's own runtime dependencies -- `semver` today -- must come
 *   from the arm's own install, and loading fails outright if that install has not been run.
 *
 * Co-resolution is therefore achievable but must be CONSTRUCTED. It is never assumed, and never
 * inferred from a version string: two identical version strings can still be two distinct copies on
 * disk, and within any one process `IModelHost` and the native addon must be loaded exactly once.
 */

/** Which measured operation an arm supplies. */
export type ArmOperation = "identity" | "fork-init" | "change-processing";

/** `TransformRunner` plus teardown the harness can keep out of the timed region. */
export interface QuickTransformRunner extends TransformRunner {
  dispose?(): void;
}

/** Superset of the weekly harness contract; every member is optional there and here. */
export interface QuickArmModule extends TestTransformerModule {
  createChangeProcessingTransform?(
    sourceDb: IModelDb,
    targetDb: IModelDb
  ): Promise<QuickTransformRunner>;
}

const operationFactories: Record<ArmOperation, keyof QuickArmModule> = {
  identity: "createIdentityTransform",
  "fork-init": "createForkInitTransform",
  "change-processing": "createChangeProcessingTransform",
};

export interface ArmSpec {
  /** Stable identifier used in reports, e.g. `"A"`, `"B"`, or a git ref. */
  readonly id: string;
  /** Root of the built arm package: the directory containing its `package.json`. */
  readonly packageRoot: string;
  /**
   * Module to import, resolved from `packageRoot`. Matches `EXTRA_TRANSFORMERS` semantics: the
   * module's DEFAULT export must conform to `QuickArmModule`.
   */
  readonly modulePath?: string;
  /** Human-readable provenance, e.g. the git ref the arm was built from. */
  readonly label?: string;
}

export interface ResolvedArm {
  readonly spec: ArmSpec;
  readonly module: QuickArmModule;
  readonly operations: readonly ArmOperation[];
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

/** Operations an arm actually supplies. Every factory is optional in the shared contract. */
export function armOperations(module: QuickArmModule): ArmOperation[] {
  return (Object.keys(operationFactories) as ArmOperation[]).filter(
    (operation) => typeof module[operationFactories[operation]] === "function"
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

  const specifier = spec.modulePath ?? packageRoot;
  const resolved = require.resolve(specifier, { paths: [packageRoot] });
  // Default export, matching how the weekly harness consumes `EXTRA_TRANSFORMERS` entries.
  const imported = (await import(resolved)) as {
    default?: QuickArmModule;
  };
  const module = imported.default;
  if (module === undefined || typeof module !== "object")
    throw new Error(
      `Arm "${spec.id}" must default-export a TestTransformerModule-shaped object from ${resolved}`
    );

  const operations = armOperations(module);
  if (operations.length === 0)
    throw new Error(
      `Arm "${spec.id}" supplies no transform factory. Expected at least one of: ${Object.values(
        operationFactories
      ).join(", ")}.`
    );

  return {
    spec,
    module,
    operations,
    transformerVersion: manifest.version ?? "unknown",
    coreBackendVersion: JSON.parse(fs.readFileSync(armCoreBackend, "utf8"))
      .version,
    coreBackendRealPath: armCoreBackend,
  };
}

export interface ArmRunnerHandle {
  readonly runner: QuickTransformRunner;
  /**
   * True when the arm supplied no `dispose`, so whatever teardown it performs happens inside
   * `run()` and therefore inside the timed region. Recorded in the report rather than corrected,
   * because only the arm knows what its teardown is.
   */
  readonly teardownInMeasuredRegion: boolean;
}

/** Build the runner for one operation, or explain precisely why this arm cannot supply it. */
export async function createArmRunner(
  arm: ResolvedArm,
  operation: ArmOperation,
  sourceDb: IModelDb,
  targetDb: IModelDb
): Promise<ArmRunnerHandle> {
  const factory = arm.module[operationFactories[operation]];
  if (typeof factory !== "function")
    throw new Error(
      `Arm "${arm.spec.id}" does not implement "${operationFactories[operation]}", ` +
        `so it cannot run the "${operation}" operation. It supplies: ${arm.operations.join(", ")}.`
    );
  const runner = (await factory.call(
    arm.module,
    sourceDb,
    targetDb
  )) as QuickTransformRunner;
  return {
    runner,
    teardownInMeasuredRegion: typeof runner.dispose !== "function",
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
  armB: ResolvedArm,
  operation: ArmOperation
): void {
  if (armA.coreBackendRealPath !== armB.coreBackendRealPath)
    throw new Error(
      `Arms "${armA.spec.id}" and "${armB.spec.id}" resolve different core-backend instances; ` +
        "the comparison is invalid."
    );
  for (const arm of [armA, armB])
    if (!arm.operations.includes(operation))
      throw new Error(
        `Arm "${arm.spec.id}" cannot run the "${operation}" operation; it supplies: ` +
          `${arm.operations.join(", ")}.`
      );
}
