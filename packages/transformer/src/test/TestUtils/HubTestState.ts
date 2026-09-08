/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "vitest";
import { IModelDb } from "@itwin/core-backend";
import { Id64String, omit } from "@itwin/core-bentley";
import { ElementProps, RelationshipProps } from "@itwin/core-common";

export type HubTestStateValue = number | ElementProps | RelationshipProps;
export type HubTestIModelState = Record<string, HubTestStateValue>;

export async function getHubTestIModelState(
  db: IModelDb
): Promise<HubTestIModelState> {
  const result: HubTestIModelState = {};
  const elemIds: Id64String[] = [];
  const reader = db.createQueryReader(
    `
    SELECT ECInstanceId
    FROM Bis.Element
    WHERE ECInstanceId>${IModelDb.dictionaryId}
      AND CodeValue NOT IN ('SpatialCategory', 'PhysicalModel')
  `,
    undefined,
    { usePrimaryConn: true }
  );
  for await (const row of reader) elemIds.push(row.id);

  for (const elemId of elemIds) {
    const elem = db.elements.getElement(elemId);
    const tag = elem.userLabel ?? elem.id;
    if (tag in result)
      throw Error("hub test iModel state requires unique user labels");
    result[tag] =
      elem.jsonProperties.updateState !== undefined
        ? elem.jsonProperties.updateState
        : elem.toJSON();
  }

  const relationshipReader = db.createQueryReader(
    `
    SELECT erte.ECInstanceId AS id, ec_classname(erte.ECClassId, 's.c') AS className,
        se.ECInstanceId AS sourceId, se.UserLabel AS sourceUserLabel,
        te.ECInstanceId AS targetId, te.UserLabel AS targetUserLabel
    FROM Bis.ElementRefersToElements erte
    JOIN Bis.Element se
      ON se.ECInstanceId=erte.SourceECInstanceId
    JOIN Bis.Element te
      ON te.ECInstanceId=erte.TargetECInstanceId
  `,
    undefined,
    { usePrimaryConn: true }
  );
  for await (const row of relationshipReader) {
    const sourceLabel = row.sourceUserLabel ?? row.sourceId;
    const targetLabel = row.targetUserLabel ?? row.targetId;
    const tag = `REL_${sourceLabel}_${targetLabel}_${row.className}`;
    if (tag in result)
      throw Error("hub test iModel state requires unique relationship labels");
    result[tag] = omit(
      db.relationships.getInstanceProps(row.className, row.id),
      ["id"]
    );
  }

  return result;
}

export async function assertHubTestIModelState(
  db: IModelDb,
  state: HubTestIModelState,
  { subset = false } = {}
): Promise<void> {
  expect(await getHubTestIModelState(db)).to.deep.subsetEqual(state, {
    useSubsetEquality: subset,
  });
}
