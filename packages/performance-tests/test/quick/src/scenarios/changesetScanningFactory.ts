/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import type { ChangedInstanceIds } from "@itwin/imodel-transformer";
import type { BriefcaseDb } from "@itwin/core-backend";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import type {
  PreparedDataset,
  PreparedDetachedDataset,
} from "../fixtures/FixtureProvider.js";
import { updateHeavyScanFixture } from "../fixtures/recipes/updateHeavyScan.js";
import {
  BenchmarkRegistration,
  defineBenchmark,
} from "../framework/BenchmarkRegistration.js";
import { BenchmarkScenarioDefinition } from "../framework/BenchmarkScenario.js";

export type ChangedInstanceIdsDependency = Pick<
  typeof ChangedInstanceIds,
  "initialize"
>;

type ScanResult = NonNullable<
  Awaited<ReturnType<ChangedInstanceIdsDependency["initialize"]>>
>;

function requireDetachedDataset(
  dataset: PreparedDataset
): PreparedDetachedDataset {
  if (dataset.topology !== "source-only")
    throw new Error(
      `Changeset scanning requires a "source-only" fixture but received "${dataset.topology}"`
    );
  return dataset;
}

async function queryRecords(
  db: BriefcaseDb,
  ecsql: string,
  propertyNames: readonly string[]
): Promise<unknown[]> {
  const values: unknown[] = [];
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  while (await reader.step()) {
    values.push(
      Object.fromEntries(
        propertyNames.map((propertyName) => [
          propertyName,
          reader.current[propertyName],
        ])
      )
    );
  }
  return values;
}

/** Stable semantic identity for the generated seed content, independent of SQLite metadata. */
export async function changesetScanningFixtureDigest(
  db: BriefcaseDb
): Promise<string> {
  return canonicalSha256({
    aspects: await queryRecords(
      db,
      "SELECT a.Payload payload,a.Sequence sequence,e.UserLabel owner FROM QuickPerfScan.ScanAspect a JOIN bis.Element e ON e.ECInstanceId=a.Element.Id ORDER BY e.UserLabel,a.Payload",
      ["owner", "payload", "sequence"]
    ),
    elements: await queryRecords(
      db,
      "SELECT UserLabel label FROM Generic.PhysicalObject WHERE UserLabel IS NOT NULL ORDER BY UserLabel",
      ["label"]
    ),
    relationships: await queryRecords(
      db,
      "SELECT s.UserLabel sourceLabel,t.UserLabel targetLabel,r.MemberPriority priority FROM bis.ElementGroupsMembers r JOIN bis.Element s ON s.ECInstanceId=r.SourceECInstanceId JOIN bis.Element t ON t.ECInstanceId=r.TargetECInstanceId ORDER BY s.UserLabel,t.UserLabel,r.MemberPriority",
      ["priority", "sourceLabel", "targetLabel"]
    ),
  });
}

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
  return canonicalSha256({
    ...normalized,
    aspectOwnerElementIds: [...result.aspectOwnerElementIds].sort(),
  });
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

/**
 * Creates the registration around an explicitly selected transformer dependency.
 *
 * This module intentionally has no workspace transformer registration or eager default export so
 * an isolated arm can load it without evaluating the harness transformer package.
 */
export function createChangesetScanningBenchmark(
  changedInstanceIds: ChangedInstanceIdsDependency
): BenchmarkRegistration {
  const scenario: BenchmarkScenarioDefinition = {
    id: "changeset-scanning",
    defaultFixtureId: updateHeavyScanFixture.descriptor.id,
    capabilities: {
      topology: "source-only",
      requiredClaims: ["changeset scanning"],
    },
    factory: (dataset) =>
      new ChangesetScanningScenario(dataset, changedInstanceIds),
  };
  return defineBenchmark({
    scenario,
    fixtures: [updateHeavyScanFixture],
  });
}
