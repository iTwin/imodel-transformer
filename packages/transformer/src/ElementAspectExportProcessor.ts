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

  /** Exports ElementAspects owned by the supplied elements.
   * Multi-aspects are emitted in one callback group per owner.
   */
  public async exportAllElementAspects(
    ownerElementIds: ReadonlySet<Id64String>
  ): Promise<void> {
    if (ownerElementIds.size === 0) return;

    await this.exportAspectsLoop<ElementUniqueAspect>(
      ElementUniqueAspect.classFullName,
      async (uniqueAspect) => {
        const isUpdate = this._aspectChanges?.updateIds.has(uniqueAspect.id)
          ? true
          : this._aspectChanges?.insertIds.has(uniqueAspect.id)
            ? false
            : undefined;
        await this._handler.onExportElementUniqueAspect(uniqueAspect, isUpdate);
        await this._handler.trackProgress();
      },
      ownerElementIds
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
      ownerElementIds
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
    ownerElementIds: ReadonlySet<Id64String>
  ): Promise<void> {
    for await (const aspect of this.queryAspects<T>(
      baseAspectClass,
      ownerElementIds
    )) {
      if (!(await this._handler.shouldExportElementAspect(aspect))) {
        continue;
      }
      await exportAspect(aspect);
    }
  }

  private async *queryAspects<T extends ElementAspect>(
    baseElementAspectClassFullName: string,
    ownerElementIds: ReadonlySet<Id64String>
  ) {
    const queryOwnerElementIds = new Set(ownerElementIds) as Id64Set;
    const populatedClassIds = await this.queryPopulatedAspectClassIds(
      baseElementAspectClassFullName,
      queryOwnerElementIds
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
      const queryParams = new QueryBinder()
        .bindId("classId", classId)
        .bindIdSet("ownerElementIds", queryOwnerElementIds);
      const aspectQueryReader = this._sourceDb.createQueryReader(
        `SELECT aspect.* FROM [${schemaName}]:[${className}] aspect
         INNER JOIN IdSet(:ownerElementIds) ids ON ids.id = aspect.Element.Id
         WHERE aspect.ECClassId = :classId
         ORDER BY aspect.Element.Id, aspect.ECInstanceId OPTIONS ENABLE_EXPERIMENTAL_FEATURES`,
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

  /** Scans which concrete subclasses of the given base aspect class have rows for the supplied owners. */
  private async queryPopulatedAspectClassIds(
    baseElementAspectClassFullName: string,
    ownerElementIds: Id64Set
  ): Promise<ReadonlySet<Id64String>> {
    const [schemaName, className] = baseElementAspectClassFullName.split(":");
    const populatedClassIds = new Set<Id64String>();
    const populatedClassesQueryReader = this._sourceDb.createQueryReader(
      `SELECT DISTINCT aspect.ECClassId as classId
       FROM [${schemaName}]:[${className}] aspect
       INNER JOIN IdSet(:ownerElementIds) ids ON ids.id = aspect.Element.Id
       OPTIONS ENABLE_EXPERIMENTAL_FEATURES`,
      new QueryBinder().bindIdSet("ownerElementIds", ownerElementIds),
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
