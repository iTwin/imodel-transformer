/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { SnapshotDb } from "@itwin/core-backend";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";

async function classDistribution(
  db: SnapshotDb,
  className: string
): Promise<unknown[]> {
  const rows: unknown[] = [];
  const reader = db.createQueryReader(
    `SELECT ec_classname(ECClassId, 's.c') className, count(*) cnt
     FROM ${className}
     GROUP BY ECClassId
     ORDER BY className`,
    undefined,
    { usePrimaryConn: true }
  );
  while (await reader.step())
    rows.push({
      className: reader.current.className,
      count: reader.current.cnt,
    });
  return rows;
}

export async function outputShapeDigest(
  targetDb: SnapshotDb,
  classQueries: Readonly<Record<string, string>>
): Promise<string> {
  const distributions = await Promise.all(
    Object.entries(classQueries).map(async ([name, className]) => [
      name,
      await classDistribution(targetDb, className),
    ])
  );
  return canonicalSha256(Object.fromEntries(distributions));
}
