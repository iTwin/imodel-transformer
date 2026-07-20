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

    let batchedElementMultiAspects: ElementMultiAspect[] = [];
    await this.exportAspectsLoop<ElementMultiAspect>(
      ElementMultiAspect.classFullName,
      async (multiAspect) => {
        if (batchedElementMultiAspects.length === 0) {
          batchedElementMultiAspects.push(multiAspect);
          return;
        }
        if (
          batchedElementMultiAspects[0].element.id !== multiAspect.element.id
        ) {
          await this._handler.onExportElementMultiAspects(
            batchedElementMultiAspects
          );
          await this._handler.trackProgress();
          batchedElementMultiAspects = [];
        }
        batchedElementMultiAspects.push(multiAspect);
      },
      elementIds
    );

    if (batchedElementMultiAspects.length > 0) {
      await this._handler.onExportElementMultiAspects(
        batchedElementMultiAspects
      );
      await this._handler.trackProgress();
    }
  }

  /** Sets the aspect changes used to distinguish inserted and updated unique aspects during change export. */
  public setAspectChanges(aspectChanges?: ChangedInstanceOps): void {
    this._aspectChanges = aspectChanges;
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
    const queryElementIds =
      elementIds === undefined ? undefined : (new Set(elementIds) as Id64Set);
    for (const [classId, { schemaName, className }] of aspectClassNameIdMap) {
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
        const row = rowProxy.toRow();
        const aspectProps: ElementAspectProps = {
          ...row,
          classFullName,
          className: undefined,
        };
        delete (aspectProps as any).className;
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
}
