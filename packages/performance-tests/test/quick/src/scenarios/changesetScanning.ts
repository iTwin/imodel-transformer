/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { ChangedInstanceIds } from "@itwin/imodel-transformer";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  PreparedDataset,
  requireDetachedDataset,
} from "../fixtures/FixtureProvider.js";
import { BenchmarkScenarioDefinition } from "../framework/BenchmarkScenario.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import { updateHeavyScanFixture } from "../fixtures/recipes/updateHeavyScan.js";

type ScanResult = NonNullable<
  Awaited<ReturnType<typeof ChangedInstanceIds.initialize>>
>;

function scanDigest(result: ScanResult): string {
  const collections = {
    aspect: result.aspect,
    codeSpec: result.codeSpec,
    element: result.element,
    font: result.font,
    model: result.model,
    relationship: result.relationship,
  };
  const normalized = Object.fromEntries(
    Object.entries(collections).map(([collection, operations]) => [
      collection,
      {
        delete: [...operations.deleteIds].sort(),
        insert: [...operations.insertIds].sort(),
        update: [...operations.updateIds].sort(),
      },
    ])
  );
  // The baseline predates aspectOwnerElementIds, so digest only the result shared by both A/B arms.
  return canonicalSha256(normalized);
}

function changedIdCount(result: ScanResult): number {
  return [
    result.aspect,
    result.codeSpec,
    result.element,
    result.font,
    result.model,
    result.relationship,
  ].reduce(
    (total, operations) =>
      total +
      operations.insertIds.size +
      operations.updateIds.size +
      operations.deleteIds.size,
    0
  );
}

/**
 * Measures {@link ChangedInstanceIds.initialize} over a set of local changeset files.
 *
 * This deliberately isolates scanning from provenance initialization, clone-context/exporter setup,
 * and deleted-entity remapping so those costs cannot dilute a scanning regression.
 *
 * `csFileProps` is the hub-free input mode that still scans changeset files. Precomputed
 * `changedInstanceIds` also avoids the hub but bypasses scanning, while range-based inputs query or
 * download changesets through `BriefcaseManager`.
 */
class ChangesetScanningScenario {
  private _result?: ScanResult;
  private _aborted = false;

  constructor(private readonly _dataset: PreparedDataset) {}

  public async measure(): Promise<void> {
    const dataset = requireDetachedDataset(this._dataset);
    if (dataset.csFileProps.length === 0)
      throw new Error("Changeset scanning fixture contains no changeset files");
    const result = await ChangedInstanceIds.initialize({
      csFileProps: dataset.csFileProps,
      iModel: dataset.sourceDb,
    });
    if (!result)
      throw new Error("Changeset scanning did not produce a scan result");
    this._result = result;
  }

  public abort(): void {
    this._aborted = true;
  }

  public async finish(): Promise<string> {
    if (this._aborted) throw new Error("Changeset scanning scenario aborted");
    if (!this._result)
      throw new Error("Changeset scanning scenario finished before measuring");
    if (changedIdCount(this._result) === 0)
      throw new Error("Changeset scanning produced no changed instance ids");
    return scanDigest(this._result);
  }
}

export const changesetScanningScenario: BenchmarkScenarioDefinition = {
  id: "changeset-scanning",
  defaultFixtureId: updateHeavyScanFixture.descriptor.id,
  capabilities: {
    topology: "source-only",
    requiredClaims: ["changeset scanning"],
  },
  factory: (dataset) => new ChangesetScanningScenario(dataset),
};

export const changesetScanningBenchmark = defineBenchmark({
  scenario: changesetScanningScenario,
  fixtures: [updateHeavyScanFixture],
});
