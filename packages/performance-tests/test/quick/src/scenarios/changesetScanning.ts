/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import type { ChangedInstanceIds } from "@itwin/imodel-transformer";
import { createRequire } from "node:module";
import { updateHeavyScanDescriptor } from "../catalogs/FixtureCatalog.js";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  PreparedDataset,
  requireDetachedDataset,
} from "../fixtures/FixtureProvider.js";
import { BenchmarkScenarioDefinition } from "../framework/BenchmarkScenario.js";

const workspaceRequire = createRequire(import.meta.url);

export type ChangedInstanceIdsDependency = Pick<
  typeof ChangedInstanceIds,
  "initialize"
>;

function workspaceChangedInstanceIds(): ChangedInstanceIdsDependency {
  return (
    workspaceRequire("@itwin/imodel-transformer") as {
      readonly ChangedInstanceIds: ChangedInstanceIdsDependency;
    }
  ).ChangedInstanceIds;
}

type ScanResult = NonNullable<
  Awaited<ReturnType<ChangedInstanceIdsDependency["initialize"]>>
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
 * The measured region is deliberately narrow. `IModelTransformer.initialize` also performs
 * provenance initialization, clone-context and exporter setup and deleted-entity remapping, so a
 * regression in any of those would surface here as a scanning regression, and — more importantly —
 * a scanning regression would be diluted by everything else. Scanning is roughly a third of that
 * larger region, so at the ~3% run-to-run variation this framework sees, a 20% scan regression
 * would move the total by about 6% and disappear into the noise. Narrowing the region is what makes
 * the regression detectable.
 *
 * `csFileProps` is the only one of the four `initialize` input modes that touches no hub: the other
 * three re-enter `BriefcaseManager` to query or download changesets. That is why this scenario needs
 * no hub and no target iModel, and why `HubMock` is deliberately not running during measurement.
 */
class ChangesetScanningScenario {
  private _result?: ScanResult;
  private _aborted = false;

  constructor(
    private readonly _dataset: PreparedDataset,
    private readonly _changedInstanceIds: ChangedInstanceIdsDependency
  ) {}

  public async measure(): Promise<void> {
    const dataset = requireDetachedDataset(this._dataset);
    if (dataset.csFileProps.length === 0)
      throw new Error("Changeset scanning fixture contains no changeset files");
    const result = await this._changedInstanceIds.initialize({
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

export function createChangesetScanningScenario(
  changedInstanceIds: ChangedInstanceIdsDependency = workspaceChangedInstanceIds()
): BenchmarkScenarioDefinition {
  return {
    id: "changeset-scanning",
    defaultFixtureId: updateHeavyScanDescriptor.id,
    capabilities: {
      topology: "source-only",
      requiredClaims: ["changeset scanning"],
    },
    factory: (dataset) =>
      new ChangesetScanningScenario(dataset, changedInstanceIds),
  };
}

export const changesetScanningScenario = createChangesetScanningScenario();
