/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelDb } from "@itwin/core-backend";

/** Counts derived from the actual source iModel captured in a fixture artifact. */
export interface IModelInventory {
  readonly byteLength: number;
  readonly schemaCount: number;
  readonly classCount: number;
  readonly propertyCount: number;
  readonly modelCount: number;
  readonly elementCount: number;
}

type SemanticIModelInventory = Omit<IModelInventory, "byteLength">;

async function queryCount(db: IModelDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(`iModel inventory query returned no rows: ${ecsql}`);
  const count = Number(reader.current.cnt);
  if (!Number.isSafeInteger(count) || count < 0)
    throw new Error(
      `iModel inventory query returned an invalid count: ${ecsql}`
    );
  return count;
}

/** Derive semantic scale from an open fixture iModel outside the measured region. */
export async function deriveIModelInventory(
  db: IModelDb
): Promise<SemanticIModelInventory> {
  return {
    schemaCount: await queryCount(
      db,
      "SELECT count(*) cnt FROM ECDbMeta.ECSchemaDef"
    ),
    classCount: await queryCount(
      db,
      "SELECT count(*) cnt FROM ECDbMeta.ECClassDef"
    ),
    propertyCount: await queryCount(
      db,
      "SELECT count(*) cnt FROM ECDbMeta.ECPropertyDef"
    ),
    modelCount: await queryCount(db, "SELECT count(*) cnt FROM bis.Model"),
    elementCount: await queryCount(db, "SELECT count(*) cnt FROM bis.Element"),
  };
}

export function isIModelInventory(value: unknown): value is IModelInventory {
  if (value === null || typeof value !== "object") return false;
  const inventory = value as Partial<IModelInventory>;
  return [
    inventory.byteLength,
    inventory.schemaCount,
    inventory.classCount,
    inventory.propertyCount,
    inventory.modelCount,
    inventory.elementCount,
  ].every(
    (count) => count !== undefined && Number.isSafeInteger(count) && count >= 0
  );
}
