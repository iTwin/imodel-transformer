/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  IModelDb,
} from "@itwin/core-backend";
import { Id64String, OrderedId64Iterable } from "@itwin/core-bentley";
import { QueryBinder } from "@itwin/core-common";

/** The minimal parent-child forest containing changed or excluded elements and their ancestors. */
export class ChangedElementForest {
  private constructor(
    private readonly _rootsByModel: ReadonlyMap<
      Id64String,
      readonly Id64String[]
    >,
    private readonly _childrenByParent: ReadonlyMap<
      Id64String,
      readonly Id64String[]
    >
  ) {}

  public static async create(
    sourceDb: IModelDb,
    ownedCandidateIds: Id64String[],
    maximumElementCount: number
  ): Promise<ChangedElementForest | undefined> {
    if (ownedCandidateIds.length === 0) {
      return new ChangedElementForest(new Map(), new Map());
    }

    const sql = `
      WITH RECURSIVE ChangedElementHierarchy (ECInstanceId, ParentId, ModelId) AS (
        SELECT e.ECInstanceId, e.Parent.Id, e.Model.Id
        FROM ${Element.classFullName} e
        INNER JOIN IdSet(:candidateIds) ids ON ids.id = e.ECInstanceId
        UNION
        SELECT p.ECInstanceId, p.Parent.Id, p.Model.Id
        FROM ${Element.classFullName} p
        INNER JOIN ChangedElementHierarchy c ON p.ECInstanceId = c.ParentId
      )
      SELECT ECInstanceId, ParentId, ModelId FROM ChangedElementHierarchy
      OPTIONS ENABLE_EXPERIMENTAL_FEATURES
    `;
    OrderedId64Iterable.sortArray(ownedCandidateIds);
    const params = new QueryBinder().bindIdSet(
      "candidateIds",
      ownedCandidateIds
    );
    // QueryBinder serializes the IdSet synchronously, so release this temporary copy before retaining the hierarchy.
    ownedCandidateIds.length = 0;
    const rootsByModel = new Map<Id64String, Id64String[]>();
    const childrenByParent = new Map<Id64String, Id64String[]>();
    let elementCount = 0;
    for await (const row of sourceDb.createQueryReader(sql, params, {
      usePrimaryConn: true,
    })) {
      if (elementCount >= maximumElementCount) return undefined;
      elementCount++;
      const elementId: Id64String = row[0];
      const parentId: Id64String | undefined = row[1] ?? undefined;
      const modelId: Id64String = row[2];
      const index = parentId === undefined ? rootsByModel : childrenByParent;
      const key = parentId ?? modelId;
      const siblings = index.get(key);
      if (siblings === undefined) index.set(key, [elementId]);
      else siblings.push(elementId);
    }

    for (const siblings of rootsByModel.values()) {
      OrderedId64Iterable.sortArray(siblings);
    }
    for (const siblings of childrenByParent.values()) {
      OrderedId64Iterable.sortArray(siblings);
    }
    return new ChangedElementForest(rootsByModel, childrenByParent);
  }

  public getModelRoots(modelId: Id64String): readonly Id64String[] {
    return this._rootsByModel.get(modelId) ?? [];
  }

  public getChildren(parentId: Id64String): readonly Id64String[] {
    return this._childrenByParent.get(parentId) ?? [];
  }
}
