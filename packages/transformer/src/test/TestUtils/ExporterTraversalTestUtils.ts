/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelDb, Subject, withEditTxn } from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { IModel } from "@itwin/core-common";

/** Inserts a subject tree below the root Subject. Node arrays are child specs. */
export interface TreeSpec {
  name: string;
  children?: TreeSpec[];
}

export function insertSubjectTree(
  db: IModelDb,
  specs: TreeSpec[]
): Map<string, Id64String> {
  return withEditTxn(db, "insert subject tree", (txn) => {
    const ids = new Map<string, Id64String>();
    const insertNode = (spec: TreeSpec, parentId: Id64String) => {
      const id = Subject.insert(txn, parentId, spec.name);
      ids.set(spec.name, id);
      for (const child of spec.children ?? []) insertNode(child, id);
    };
    for (const spec of specs) insertNode(spec, IModel.rootSubjectId);
    return ids;
  });
}
