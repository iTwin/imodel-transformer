/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ElementAspect,
  ElementMultiAspect,
  ElementUniqueAspect,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { ExportElementAspectsStrategy } from "./ExportElementAspectsStrategy";
import { ensureECSqlReaderIsAsyncIterableIterator } from "./ECSqlReaderAsyncIterableIteratorAdapter";
import {
  ElementAspectProps,
  QueryBinder,
  QueryRowFormat,
} from "@itwin/core-common";

/**
 * Detached ElementAspect export strategy for [[IModelExporter]].
 * This strategy exports all ElementAspects separately from the Elements that own them.
 *
 * @note Since aspects are exported separately from elements that own them, this strategy will export aspects of filtered out elements by default and
 * this needs to be handled by ElementAspectHandler
 * @internal
 */
export class DetachedExportElementAspectsStrategy extends ExportElementAspectsStrategy {
  public override async exportAllElementAspects(): Promise<void> {
    await this.exportAspectsLoop<ElementUniqueAspect>(
      ElementUniqueAspect.classFullName,
      async (uniqueAspect) => {
        const isInsertChange =
          this.aspectChanges?.insertIds.has(uniqueAspect.id) ?? false;
        const isUpdateChange =
          this.aspectChanges?.updateIds.has(uniqueAspect.id) ?? false;
        const doExport =
          this.aspectChanges === undefined || isInsertChange || isUpdateChange;
        if (doExport) {
          const isKnownUpdate = this.aspectChanges ? isUpdateChange : undefined;
          this.handler.onExportElementUniqueAspect(uniqueAspect, isKnownUpdate);
          await this.handler.trackProgress();
        }
      }
    );

    let batchedElementMultiAspects: ElementMultiAspect[] = [];
    await this.exportAspectsLoop<ElementMultiAspect>(
      ElementMultiAspect.classFullName,
      async (multiAspect) => {
        if (batchedElementMultiAspects.length === 0) {
          batchedElementMultiAspects.push(multiAspect);
          return;
        }

        // element id changed so all element's aspects are in the array and can be exported
        if (
          batchedElementMultiAspects[0].element.id !== multiAspect.element.id
        ) {
          this.handler.onExportElementMultiAspects(batchedElementMultiAspects);
          await this.handler.trackProgress();
          batchedElementMultiAspects = [];
        }

        batchedElementMultiAspects.push(multiAspect);
      }
    );

    if (batchedElementMultiAspects.length > 0) {
      // aspects that are left in the array have not been exported
      this.handler.onExportElementMultiAspects(batchedElementMultiAspects);
      await this.handler.trackProgress();
    }
  }

  private async exportAspectsLoop<T extends ElementAspect>(
    baseAspectClass: string,
    exportAspect: (aspect: T) => Promise<void>
  ) {
    for await (const aspect of this.queryAspects<T>(baseAspectClass)) {
      if (!this.shouldExportElementAspect(aspect)) {
        continue;
      }

      await exportAspect(aspect);
    }
  }

  private async *queryAspects<T extends ElementAspect>(
    baseElementAspectClassFullName: string
  ) {
    const aspectClassNameIdMap = new Map<
      Id64String,
      { schemaName: string; className: string }
    >();

    const optimizesAspectClassesSql = `
      SELECT c.ECInstanceId as classId, (ec_className(c.ECInstanceId, 's')) as schemaName, (ec_className(c.ECInstanceId, 'c')) as className
      FROM ECDbMeta.ClassHasAllBaseClasses r
      JOIN ECDbMeta.ECClassDef c ON c.ECInstanceId = r.SourceECInstanceId
      WHERE r.TargetECInstanceId = ec_classId(:baseClassName)
    `;
    const aspectClassesQueryReader = this.sourceDb.createQueryReader(
      optimizesAspectClassesSql,
      new QueryBinder().bindString(
        "baseClassName",
        baseElementAspectClassFullName
      )
    );
    const aspectClassesAsyncQueryReader =
      ensureECSqlReaderIsAsyncIterableIterator(aspectClassesQueryReader);
    for await (const rowProxy of aspectClassesAsyncQueryReader) {
      const row = rowProxy.toRow();
      aspectClassNameIdMap.set(row.classId, {
        schemaName: row.schemaName,
        className: row.className,
      });
    }

    for (const [classId, { schemaName, className }] of aspectClassNameIdMap) {
      const classFullName = `${schemaName}:${className}`;
      if (this.excludedElementAspectClassFullNames.has(classFullName)) continue;

      const getAspectPropsSql = `SELECT * FROM [${schemaName}]:[${className}] WHERE ECClassId = :classId ORDER BY Element.Id`;
      const aspectQueryReader = this.sourceDb.createQueryReader(
        getAspectPropsSql,
        new QueryBinder().bindId("classId", classId),
        { rowFormat: QueryRowFormat.UseJsPropertyNames }
      );
      const aspectAsyncQueryReader =
        ensureECSqlReaderIsAsyncIterableIterator(aspectQueryReader);
      let firstDone = false;
      for await (const rowProxy of aspectAsyncQueryReader) {
        const row = rowProxy.toRow();
        const aspectProps: ElementAspectProps = {
          ...row,
          classFullName,
          className: undefined,
        }; // add in property required by EntityProps
        if (!firstDone) {
          firstDone = true;
        }
        delete (aspectProps as any).className; // clear property from SELECT * that we don't want in the final instance
        const aspectEntity = this.sourceDb.constructEntity<T>(aspectProps);

        yield aspectEntity;
      }
    }
  }

  public override async exportElementAspectsForElement(
    _elementId: string
  ): Promise<void> {
    // All aspects are exported separately from their elements and don't need to be exported when element is exported.
  }
}
