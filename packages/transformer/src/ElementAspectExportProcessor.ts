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
import { Id64Set, Id64String } from "@itwin/core-bentley";
import { ensureECSqlReaderIsAsyncIterableIterator } from "./ECSqlReaderAsyncIterableIteratorAdapter";
import {
  ElementAspectProps,
  QueryBinder,
  QueryRowFormat,
} from "@itwin/core-common";

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
  shouldExportElement(elementId: Id64String): Promise<boolean>;
  trackProgress: () => Promise<void>;
}

/** Queries and exports ElementAspects for accepted source owners, applying class and handler filters and grouping multi-aspects by owner.
 * @internal
 */
export class ElementAspectExportProcessor {
  private readonly _aspectClasses = new Map<
    string,
    Promise<ReadonlyMap<Id64String, { schemaName: string; className: string }>>
  >();
  /** Aspect classes (by ECClassId) that have at least one row in the source iModel, per base class.
   * Populated lazily for unscoped exports; explicit owner batches are always scanned independently.
   */
  private readonly _populatedAspectClassIds = new Map<
    string,
    Promise<ReadonlySet<Id64String>>
  >();
  /** ElementAspect classes excluded from source queries. */
  private readonly _excludedElementAspectClassFullNames = new Set<string>();
  /** ECClassIds excluded from source queries: every excluded class plus all of its
   * declared subclasses.
   */
  private _excludedClassIds: Promise<ReadonlySet<Id64String>> | undefined;
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

  /** Exports accepted ElementAspects, optionally restricting the query to the supplied owner IDs.
   * Multi-aspects are emitted in one callback group per owner.
   */
  public async exportAllElementAspects(
    elementIds?: ReadonlySet<Id64String>
  ): Promise<void> {
    if (elementIds !== undefined && elementIds.size === 0) return;
    const exportAllForOwners = elementIds !== undefined;

    await this.exportAspectsLoop<ElementUniqueAspect>(
      ElementUniqueAspect.classFullName,
      async (uniqueAspect) => {
        const isInsertChange =
          this._aspectChanges?.insertIds.has(uniqueAspect.id) ?? false;
        const isUpdateChange =
          this._aspectChanges?.updateIds.has(uniqueAspect.id) ?? false;
        const doExport =
          exportAllForOwners ||
          this._aspectChanges === undefined ||
          isInsertChange ||
          isUpdateChange;
        if (doExport) {
          const isKnownUpdate = exportAllForOwners
            ? isUpdateChange
              ? true
              : isInsertChange
                ? false
                : undefined
            : this._aspectChanges
              ? isUpdateChange
              : undefined;
          await this._handler.onExportElementUniqueAspect(
            uniqueAspect,
            isKnownUpdate
          );
          await this._handler.trackProgress();
        }
      },
      elementIds
    );

    const multiAspectsByOwner = new Map<Id64String, ElementMultiAspect[]>();
    await this.exportAspectsLoop<ElementMultiAspect>(
      ElementMultiAspect.classFullName,
      async (multiAspect) => {
        const ownerAspects = multiAspectsByOwner.get(multiAspect.element.id);
        if (ownerAspects === undefined) {
          multiAspectsByOwner.set(multiAspect.element.id, [multiAspect]);
        } else {
          ownerAspects.push(multiAspect);
        }
      },
      elementIds
    );

    for (const multiAspects of multiAspectsByOwner.values()) {
      await this._handler.onExportElementMultiAspects(multiAspects);
      await this._handler.trackProgress();
    }
  }

  /** Sets the aspect changes used to distinguish inserted and updated unique aspects during change export. */
  public setAspectChanges(aspectChanges?: AspectChanges): void {
    this._aspectChanges = aspectChanges;
  }

  /** Clears source schema and content caches at an outer export-scope boundary. */
  public resetCaches(): void {
    this._aspectClasses.clear();
    this._populatedAspectClassIds.clear();
    this._excludedClassIds = undefined;
  }

  /** Excludes an ElementAspect class from subsequent queries and export callbacks. */
  public excludeElementAspectClass(classFullName: string): void {
    this._excludedElementAspectClassFullNames.add(classFullName);
    this._excludedClassIds = undefined;
  }

  private async exportAspectsLoop<T extends ElementAspect>(
    baseAspectClass: string,
    exportAspect: (aspect: T) => Promise<void>,
    elementIds?: ReadonlySet<Id64String>
  ): Promise<void> {
    for await (const aspect of this.queryAspects<T>(
      baseAspectClass,
      elementIds
    )) {
      if (elementIds !== undefined && !elementIds.has(aspect.element.id)) {
        continue;
      }
      if (
        elementIds === undefined &&
        !(await this._handler.shouldExportElement(aspect.element.id))
      ) {
        continue;
      }
      if (!(await this._handler.shouldExportElementAspect(aspect))) {
        continue;
      }
      await exportAspect(aspect);
    }
  }

  private async *queryAspects<T extends ElementAspect>(
    baseElementAspectClassFullName: string,
    elementIds?: ReadonlySet<Id64String>
  ) {
    if (elementIds !== undefined && elementIds.size === 0) return;

    const queryElementIds =
      elementIds === undefined ? undefined : (new Set(elementIds) as Id64Set);
    const populatedClassIds = await this.getPopulatedAspectClassIds(
      baseElementAspectClassFullName,
      queryElementIds
    );
    if (populatedClassIds.size === 0) return;

    const aspectClassNameIdMap = await this.getAspectClasses(
      baseElementAspectClassFullName
    );
    const excludedClassIds = await this.getExcludedClassIds();
    for (const [classId, { schemaName, className }] of aspectClassNameIdMap) {
      // Skip classes with no rows in the current owner scope: most schemas declare
      // far more concrete aspect subclasses than a batch actually populates.
      if (!populatedClassIds.has(classId)) continue;
      // excludedClassIds is the excluded class plus every declared subclass.
      if (excludedClassIds.has(classId)) continue;

      const classFullName = `${schemaName}:${className}`;
      const queryParams = new QueryBinder().bindId("classId", classId);
      const fromClause =
        queryElementIds === undefined
          ? `[${schemaName}]:[${className}] aspect`
          : `[${schemaName}]:[${className}] aspect
             INNER JOIN IdSet(:elementIds) ids ON ids.id = aspect.Element.Id`;
      if (queryElementIds !== undefined) {
        queryParams.bindIdSet("elementIds", queryElementIds);
      }
      const queryOptions =
        queryElementIds === undefined
          ? ""
          : " OPTIONS ENABLE_EXPERIMENTAL_FEATURES";
      const aspectQueryReader = this._sourceDb.createQueryReader(
        `SELECT aspect.* FROM ${fromClause}
         WHERE aspect.ECClassId = :classId
         ORDER BY aspect.Element.Id, aspect.ECInstanceId${queryOptions}`,
        queryParams,
        // eslint-disable-next-line @typescript-eslint/no-deprecated
        { rowFormat: QueryRowFormat.UseJsPropertyNames, usePrimaryConn: true }
      );
      for await (const rowProxy of ensureECSqlReaderIsAsyncIterableIterator(
        aspectQueryReader
      )) {
        const { className: _className, ...aspectProps } =
          rowProxy.toRow() as ElementAspectProps & {
            className?: string;
          };
        aspectProps.classFullName = classFullName;
        yield this._sourceDb.constructEntity<T>(aspectProps);
      }
    }
  }

  private async getAspectClasses(
    baseElementAspectClassFullName: string
  ): Promise<
    ReadonlyMap<Id64String, { schemaName: string; className: string }>
  > {
    let aspectClasses = this._aspectClasses.get(baseElementAspectClassFullName);
    if (aspectClasses === undefined) {
      aspectClasses = this.queryAspectClasses(baseElementAspectClassFullName);
      this._aspectClasses.set(baseElementAspectClassFullName, aspectClasses);
    }
    return aspectClasses;
  }

  private async queryAspectClasses(
    baseElementAspectClassFullName: string
  ): Promise<
    ReadonlyMap<Id64String, { schemaName: string; className: string }>
  > {
    const aspectClassNameIdMap = new Map<
      Id64String,
      { schemaName: string; className: string }
    >();
    const aspectClassesQueryReader = this._sourceDb.createQueryReader(
      `
        SELECT c.ECInstanceId as classId,
          (ec_className(c.ECInstanceId, 's')) as schemaName,
          (ec_className(c.ECInstanceId, 'c')) as className
        FROM ECDbMeta.ClassHasAllBaseClasses r
        JOIN ECDbMeta.ECClassDef c ON c.ECInstanceId = r.SourceECInstanceId
        WHERE r.TargetECInstanceId = ec_classId(:baseClassName)
        ORDER BY schemaName, className
      `,
      new QueryBinder().bindString(
        "baseClassName",
        baseElementAspectClassFullName
      ),
      { usePrimaryConn: true }
    );
    for await (const rowProxy of ensureECSqlReaderIsAsyncIterableIterator(
      aspectClassesQueryReader
    )) {
      const row = rowProxy.toRow();
      aspectClassNameIdMap.set(row.classId, {
        schemaName: row.schemaName,
        className: row.className,
      });
    }
    return aspectClassNameIdMap;
  }

  private async getPopulatedAspectClassIds(
    baseElementAspectClassFullName: string,
    elementIds?: Id64Set
  ): Promise<ReadonlySet<Id64String>> {
    if (elementIds !== undefined) {
      return this.queryPopulatedAspectClassIds(
        baseElementAspectClassFullName,
        elementIds
      );
    }

    let populatedClassIds = this._populatedAspectClassIds.get(
      baseElementAspectClassFullName
    );
    if (populatedClassIds === undefined) {
      populatedClassIds = this.queryPopulatedAspectClassIds(
        baseElementAspectClassFullName
      );
      this._populatedAspectClassIds.set(
        baseElementAspectClassFullName,
        populatedClassIds
      );
    }
    return populatedClassIds;
  }

  private async getExcludedClassIds(): Promise<ReadonlySet<Id64String>> {
    if (this._excludedClassIds === undefined) {
      this._excludedClassIds = this.queryExcludedClassIds();
    }
    return this._excludedClassIds;
  }

  /** Resolves every excluded class full name to the full set of ECClassIds it covers:
   * the class itself plus every declared subclass. Reuses the same
   * ECDbMeta.ClassHasAllBaseClasses shape as {@link queryAspectClasses}, rooted at
   * each excluded class instead of at ElementUniqueAspect/ElementMultiAspect.
   */
  private async queryExcludedClassIds(): Promise<ReadonlySet<Id64String>> {
    const excludedClassIds = new Set<Id64String>();
    for (const classFullName of this._excludedElementAspectClassFullNames) {
      const excludedClassesQueryReader = this._sourceDb.createQueryReader(
        `
          SELECT c.ECInstanceId as classId
          FROM ECDbMeta.ClassHasAllBaseClasses r
          JOIN ECDbMeta.ECClassDef c ON c.ECInstanceId = r.SourceECInstanceId
          WHERE r.TargetECInstanceId = ec_classId(:excludedClassName)
        `,
        new QueryBinder().bindString("excludedClassName", classFullName),
        { usePrimaryConn: true }
      );
      for await (const rowProxy of ensureECSqlReaderIsAsyncIterableIterator(
        excludedClassesQueryReader
      )) {
        excludedClassIds.add(rowProxy.toRow().classId);
      }
    }
    return excludedClassIds;
  }

  /** Scans which concrete subclasses of the given base aspect class have rows,
   * optionally restricted to an explicit owner set.
   */
  private async queryPopulatedAspectClassIds(
    baseElementAspectClassFullName: string,
    elementIds?: Id64Set
  ): Promise<ReadonlySet<Id64String>> {
    const [schemaName, className] = baseElementAspectClassFullName.split(":");
    const populatedClassIds = new Set<Id64String>();
    const params =
      elementIds === undefined
        ? undefined
        : new QueryBinder().bindIdSet("elementIds", elementIds);
    const ownerJoin =
      elementIds === undefined
        ? " aspect"
        : " aspect INNER JOIN IdSet(:elementIds) ids ON ids.id = aspect.Element.Id";
    const queryOptions =
      elementIds === undefined ? "" : " OPTIONS ENABLE_EXPERIMENTAL_FEATURES";
    const populatedClassesQueryReader = this._sourceDb.createQueryReader(
      `SELECT DISTINCT aspect.ECClassId as classId
       FROM [${schemaName}]:[${className}]${ownerJoin}${queryOptions}`,
      params,
      { usePrimaryConn: true }
    );
    for await (const rowProxy of ensureECSqlReaderIsAsyncIterableIterator(
      populatedClassesQueryReader
    )) {
      populatedClassIds.add(rowProxy.toRow().classId);
    }
    return populatedClassIds;
  }
}
