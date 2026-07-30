/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { ChangedInstanceIds } from "@itwin/imodel-transformer";
import { updateHeavyScanDescriptor } from "../catalogs/FixtureCatalog.js";
import {
  PreparedDataset,
  requireDetachedDataset,
} from "../fixtures/FixtureProvider.js";
import {
  assertScanMatchesOracle,
  scanDigest,
  ScanExpectation,
  ScanLedger,
  ScanLedgerEntry,
  squashLedger,
} from "../fixtures/validation/scanOracle.js";
import { BenchmarkScenarioDefinition } from "../framework/BenchmarkScenario.js";

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
  private _result?: ScanExpectation;
  private _aborted = false;

  constructor(private readonly _dataset: PreparedDataset) {}

  public async measure(): Promise<void> {
    const dataset = requireDetachedDataset(this._dataset);
    this._result = await ChangedInstanceIds.initialize({
      csFileProps: dataset.csFileProps,
      iModel: dataset.sourceDb,
    });
  }

  public abort(): void {
    this._aborted = true;
  }

  public async finish(): Promise<string> {
    if (this._aborted) throw new Error("Changeset scanning scenario aborted");
    const dataset = requireDetachedDataset(this._dataset);
    if (!this._result)
      throw new Error("Changeset scanning scenario finished before measuring");

    const entries = dataset.recipe as ScanLedgerEntry[] | undefined;
    if (!entries)
      throw new Error(
        `Fixture "${dataset.descriptor.id}" carries no recipe ledger, so the scan result cannot be verified`
      );

    // Verify against what the recipe says it did, not against a previous digest. A digest can only
    // detect that behaviour changed; it cannot detect behaviour that was wrong from the start.
    assertScanMatchesOracle(
      this._result,
      squashLedger(ScanLedger.fromEntries(entries))
    );
    return scanDigest(this._result);
  }
}

export const changesetScanningScenario: BenchmarkScenarioDefinition = {
  id: "changeset-scanning",
  defaultFixtureId: updateHeavyScanDescriptor.id,
  capabilities: {
    topology: "source-only",
    requiredClaims: ["changeset scanning"],
  },
  factory: (dataset) => new ChangesetScanningScenario(dataset),
};
