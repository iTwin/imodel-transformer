/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ElementAspect,
  ElementMultiAspect,
  ElementUniqueAspect,
  IModelDb,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { ensureECSqlReaderIsAsyncIterableIterator } from "./ECSqlReaderAsyncIterableIteratorAdapter";
import { QueryBinder } from "@itwin/core-common";

interface AspectChanges {
  readonly insertIds: ReadonlySet<Id64String>;
  readonly updateIds: ReadonlySet<Id64String>;
}

export interface ElementAspectExportProcessorHandler {
  shouldExportElementAspect(aspect: ElementAspect): Promise<boolean>;
  onExportElementUniqueAspect(
    uniqueAspect: ElementUniqueAspect,
    isUpdate?: boolean | undefined
  ): Promise<void>;
  onExportElementMultiAspects(
    multiAspects: ElementMultiAspect[]
  ): Promise<void>;
  trackProgress: () => Promise<void>;
}

/** Queries and exports ElementAspects for accepted source owners, applying class and handler filters and grouping multi-aspects by owner.
 * @internal
 */
export class ElementAspectExportProcessor {
  /** ElementAspect classes excluded from source queries. */
  private readonly _excludedElementAspectClassFullNames = new Set<string>();
  private _hasExportableAspects: Promise<boolean> | undefined;
  private _expandedExcludedElementAspectClassFullNames:
    | Promise<ReadonlySet<string>>
    | undefined;
  private _aspectChanges: AspectChanges | undefined;

  /** ElementAspect class names excluded from source queries. */
  public get excludedElementAspectClassFullNames(): ReadonlySet<string> {
    return this._excludedElementAspectClassFullNames;
  }
  private readonly _sourceDb: IModelDb;
  private readonly _handler: ElementAspectExportProcessorHandler;

  public constructor(
    sourceDb: IModelDb,
    handler: ElementAspectExportProcessorHandler
  ) {
    this._sourceDb = sourceDb;
    this._handler = handler;
  }

  /** Exports ElementAspects owned by the supplied elements.
   * Multi-aspects are emitted in one callback group per owner.
   */
  public async exportAllElementAspects(
    ownerElementIds: ReadonlySet<Id64String>
  ): Promise<void> {
    if (ownerElementIds.size === 0 || !(await this.getHasExportableAspects()))
      return;

    let multiAspectOwnerId: Id64String | undefined;
    let multiAspects: ElementMultiAspect[] = [];
    const exportMultiAspects = async () => {
      if (multiAspects.length === 0) return;
      await this._handler.onExportElementMultiAspects(multiAspects);
      await this._handler.trackProgress();
      multiAspects = [];
      multiAspectOwnerId = undefined;
    };

    const excludedAspectClassFullNames =
      await this.getExpandedExcludedElementAspectClassFullNames();
    for await (const aspect of this._sourceDb.elements.queryAspects({
      elementIds: new Set(ownerElementIds),
      excludedAspectClassFullNames,
      groupByOwner: true,
      usePrimaryConn: true,
    })) {
      if (!(await this._handler.shouldExportElementAspect(aspect))) continue;

      if (
        multiAspectOwnerId !== undefined &&
        multiAspectOwnerId !== aspect.element.id
      ) {
        await exportMultiAspects();
      }

      if (aspect instanceof ElementUniqueAspect) {
        const isUpdate = this._aspectChanges?.updateIds.has(aspect.id)
          ? true
          : this._aspectChanges?.insertIds.has(aspect.id)
            ? false
            : undefined;
        await this._handler.onExportElementUniqueAspect(aspect, isUpdate);
        await this._handler.trackProgress();
      } else if (aspect instanceof ElementMultiAspect) {
        multiAspectOwnerId = aspect.element.id;
        multiAspects.push(aspect);
      } else {
        throw new Error(
          `Unexpected ElementAspect type ${aspect.classFullName}`
        );
      }
    }

    await exportMultiAspects();
  }

  /** Sets the aspect changes used to distinguish inserted and updated unique aspects during change export. */
  public setAspectChanges(aspectChanges?: AspectChanges): void {
    this._aspectChanges = aspectChanges;
  }

  /** Clears source schema and content caches at an outer export-scope boundary. */
  public resetCaches(): void {
    this._hasExportableAspects = undefined;
    this._expandedExcludedElementAspectClassFullNames = undefined;
  }

  /** Excludes an ElementAspect class from subsequent queries and export callbacks. */
  public excludeElementAspectClass(classFullName: string): void {
    if (this._excludedElementAspectClassFullNames.has(classFullName)) return;
    this._excludedElementAspectClassFullNames.add(classFullName);
    this.resetCaches();
  }

  private getExcludedClassFilter(queryParams: QueryBinder): string {
    const excludedClassParameters: string[] = [];
    let excludedClassIndex = 0;
    for (const classFullName of this._excludedElementAspectClassFullNames) {
      const parameterName = `excludedAspectClass${excludedClassIndex++}`;
      excludedClassParameters.push(`ec_classid(:${parameterName})`);
      queryParams.bindString(parameterName, classFullName.replace(".", ":"));
    }

    return excludedClassParameters.length === 0
      ? ""
      : `AND NOT EXISTS (
          SELECT 1 FROM meta.ClassHasAllBaseClasses excluded
          WHERE excluded.SourceECInstanceId = aspect.ECClassId
            AND excluded.TargetECInstanceId IN (${excludedClassParameters.join(",")})
        )`;
  }

  private async getHasExportableAspects(): Promise<boolean> {
    this._hasExportableAspects ??= this.queryHasExportableAspects();
    return this._hasExportableAspects;
  }

  private async queryHasExportableAspects(): Promise<boolean> {
    const queryParams = new QueryBinder();
    const excludedClassFilter = this.getExcludedClassFilter(queryParams);
    const reader = this._sourceDb.createQueryReader(
      `SELECT ECInstanceId FROM (
         SELECT aspect.ECInstanceId
         FROM Bis.ElementMultiAspect aspect
         WHERE TRUE ${excludedClassFilter}
         UNION ALL
         SELECT aspect.ECInstanceId
         FROM Bis.ElementUniqueAspect aspect
         WHERE TRUE ${excludedClassFilter}
       )
       LIMIT 1`,
      queryParams,
      { usePrimaryConn: true }
    );

    for await (const _row of ensureECSqlReaderIsAsyncIterableIterator(reader))
      return true;
    return false;
  }

  private async getExpandedExcludedElementAspectClassFullNames(): Promise<
    ReadonlySet<string>
  > {
    this._expandedExcludedElementAspectClassFullNames ??=
      this.queryExpandedExcludedElementAspectClassFullNames();
    return this._expandedExcludedElementAspectClassFullNames;
  }

  private async queryExpandedExcludedElementAspectClassFullNames(): Promise<
    ReadonlySet<string>
  > {
    const excludedClassFullNames = new Set<string>();
    for (const classFullName of this._excludedElementAspectClassFullNames) {
      const reader = this._sourceDb.createQueryReader(
        `SELECT ec_classname(classes.ECInstanceId, 's:c') AS classFullName
         FROM meta.ClassHasAllBaseClasses inheritance
         JOIN meta.ECClassDef classes
           ON classes.ECInstanceId = inheritance.SourceECInstanceId
         WHERE inheritance.TargetECInstanceId = ec_classid(:excludedClassName)`,
        new QueryBinder().bindString("excludedClassName", classFullName),
        { usePrimaryConn: true }
      );
      for await (const row of ensureECSqlReaderIsAsyncIterableIterator(reader))
        excludedClassFullNames.add(row.classFullName);
    }
    return excludedClassFullNames;
  }
}
