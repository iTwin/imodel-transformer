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
import { Id64Set, Id64String, Logger } from "@itwin/core-bentley";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import { ChangedInstanceOps } from "./ChangedInstanceIds";
import { ensureECSqlReaderIsAsyncIterableIterator } from "./ECSqlReaderAsyncIterableIteratorAdapter";
import {
  ElementAspectProps,
  QueryBinder,
  QueryRowFormat,
} from "@itwin/core-common";

const loggerCategory = TransformerLoggerCategory.IModelExporter;

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
  private readonly _excludedElementAspectClasses = new Set<
    typeof ElementAspect
  >();
  private readonly _aspectClasses = new Map<
    string,
    Promise<ReadonlyMap<Id64String, { schemaName: string; className: string }>>
  >();
  /** Aspect classes (by ECClassId) that have at least one row in the source iModel, per base class.
   * Populated lazily with one polymorphic scan per base class, reused across every owner batch.
   */
  private readonly _populatedAspectClassIds = new Map<
    string,
    Promise<ReadonlySet<Id64String>>
  >();
  /** ElementAspect classes excluded from source queries. */
  private readonly _excludedElementAspectClassFullNames = new Set<string>();
  private _aspectChanges: ChangedInstanceOps | undefined;

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
  public setAspectChanges(aspectChanges?: ChangedInstanceOps): void {
    this._aspectChanges = aspectChanges;
  }

  /** Clears the cached "which aspect classes are populated" scan.
   * Call this before each top-level exportAll()/exportChanges() so aspect classes
   * populated for the first time since the last scan aren't silently skipped on a
   * long-lived exporter. Declared-class metadata ({@link getAspectClasses}) does not
   * need this: the schema doesn't change mid-transform, but which classes have rows can.
   */
  public resetPopulatedAspectClassCache(): void {
    this._populatedAspectClassIds.clear();
  }

  /** Excludes an ElementAspect class from subsequent queries and export callbacks. */
  public excludeElementAspectClass(classFullName: string): void {
    this._excludedElementAspectClassFullNames.add(classFullName);
    this._excludedElementAspectClasses.add(
      this._sourceDb.getJsClass<typeof ElementAspect>(classFullName)
    );
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
      if (!(await this.shouldExportElementAspect(aspect))) {
        continue;
      }
      await exportAspect(aspect);
    }
  }

  private async shouldExportElementAspect(
    aspect: ElementAspect
  ): Promise<boolean> {
    for (const excludedElementAspectClass of this
      ._excludedElementAspectClasses) {
      if (aspect instanceof excludedElementAspectClass) {
        Logger.logInfo(
          loggerCategory,
          `Excluded ElementAspect by class: ${aspect.classFullName}`
        );
        return false;
      }
    }
    return this._handler.shouldExportElementAspect(aspect);
  }

  private async *queryAspects<T extends ElementAspect>(
    baseElementAspectClassFullName: string,
    elementIds?: ReadonlySet<Id64String>
  ) {
    const aspectClassNameIdMap = await this.getAspectClasses(
      baseElementAspectClassFullName
    );
    const populatedClassIds = await this.getPopulatedAspectClassIds(
      baseElementAspectClassFullName
    );
    const queryElementIds =
      elementIds === undefined ? undefined : (new Set(elementIds) as Id64Set);
    for (const [classId, { schemaName, className }] of aspectClassNameIdMap) {
      // Most schemas declare far more concrete aspect subclasses than any single
      // iModel ever populates. Skipping classes with zero rows anywhere in the
      // source avoids firing a query per declared class per owner batch.
      if (!populatedClassIds.has(classId)) continue;

      const classFullName = `${schemaName}:${className}`;
      if (this._excludedElementAspectClassFullNames.has(classFullName))
        continue;

      const queryParams = new QueryBinder().bindId("classId", classId);
      const elementFilter =
        queryElementIds === undefined
          ? ""
          : " AND InVirtualSet(:elementIds, Element.Id)";
      if (queryElementIds !== undefined) {
        queryParams.bindIdSet("elementIds", queryElementIds);
      }
      const aspectQueryReader = this._sourceDb.createQueryReader(
        `SELECT * FROM [${schemaName}]:[${className}]
         WHERE ECClassId = :classId${elementFilter}
         ORDER BY Element.Id, ECInstanceId`,
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
    baseElementAspectClassFullName: string
  ): Promise<ReadonlySet<Id64String>> {
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

  /** Scans which concrete subclasses of the given base aspect class have at least
   * one row in the source iModel. This is a single polymorphic query (the base
   * class query itself fans out across all populated concrete tables), not one
   * query per declared subclass.
   */
  private async queryPopulatedAspectClassIds(
    baseElementAspectClassFullName: string
  ): Promise<ReadonlySet<Id64String>> {
    const [schemaName, className] = baseElementAspectClassFullName.split(":");
    const populatedClassIds = new Set<Id64String>();
    const populatedClassesQueryReader = this._sourceDb.createQueryReader(
      `SELECT DISTINCT ECClassId as classId FROM [${schemaName}]:[${className}]`,
      undefined,
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
