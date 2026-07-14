/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn, RelationshipProps } from "@itwin/core-backend";
import { ElementProps } from "@itwin/core-common";
import { Id64String } from "@itwin/core-bentley";
import { IModelImporter, IModelImportOptions } from "@itwin/imodel-transformer";
import { BenchmarkStats } from "./BenchmarkStats";

/**
 * An IModelImporter subclass that captures timing data for import operations.
 * Measures time spent inserting elements and relationships into the target iModel.
 */
export class BenchmarkImporter extends IModelImporter {
  private readonly _stats: BenchmarkStats;

  public constructor(
    editTxn: EditTxn,
    stats: BenchmarkStats,
    options?: IModelImportOptions
  ) {
    super(editTxn, options);
    this._stats = stats;
  }

  protected override async onInsertElement(
    elementProps: ElementProps
  ): Promise<Id64String> {
    const start = performance.now();
    const result = await super.onInsertElement(elementProps);
    this._stats.importInsertElementMs += performance.now() - start;
    this._stats.importInsertElementCount++;
    return result;
  }

  protected override async onInsertRelationship(
    relationshipProps: RelationshipProps
  ): Promise<Id64String> {
    const start = performance.now();
    const result = await super.onInsertRelationship(relationshipProps);
    this._stats.importInsertRelationshipMs += performance.now() - start;
    this._stats.importInsertRelationshipCount++;
    return result;
  }
}
