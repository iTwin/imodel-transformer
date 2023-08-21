/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { ElementAspect, ElementMultiAspect, ElementUniqueAspect } from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { ElementAspectsHandler, ExportElementAspectsStrategy } from "./ExportElementAspectsStrategy";
import { ensureECSqlReaderIsAsyncIterableIterator } from "./ECSqlReaderAsyncIterableIteratorAdapter";
import { ElementAspectProps, QueryBinder, QueryRowFormat } from "@itwin/core-common";

/**
 * Handler for [[DetachedExportElementAspectsStrategy]]
 * @internal
 */
export interface DetachedElementAspectsHandler extends ElementAspectsHandler {
  onExportElementUniqueAspect(uniqueAspect: ElementUniqueAspect, isUpdate?: boolean | undefined): void;
  onExportElementMultiAspects(multiAspects: ElementMultiAspect[]): void;
  trackProgress(): Promise<void>;
}

/**
 * Detached ElementAspect export strategy for [[IModelExporter]].
 * This strategy exports all ElementAspects separately from the Elements that thei are connected with.
 * @internal
 */
export class DetachedExportElementAspectsStrategy extends ExportElementAspectsStrategy<DetachedElementAspectsHandler> {
  public async exportAllElementAspects(): Promise<void> {
    await this.exportAspectsLoop<ElementUniqueAspect>(ElementUniqueAspect.classFullName, async (uniqueAspect) => {
      const isInsertChange = this.aspectChanges?.insertIds.has(uniqueAspect.id) ?? false;
      const isUpdateChange = this.aspectChanges?.updateIds.has(uniqueAspect.id) ?? false;
      const doExport = this.aspectChanges === undefined || isInsertChange || isUpdateChange;
      if (doExport) {
        const isKnownUpdate = this.aspectChanges ? isUpdateChange : undefined;
        this.handler.onExportElementUniqueAspect(uniqueAspect, isKnownUpdate);
        await this.handler.trackProgress();
      }
    });

    let batchedElementMultiAspects: ElementMultiAspect[] = [];
    await this.exportAspectsLoop<ElementMultiAspect>(ElementMultiAspect.classFullName, async (multiAspect) => {
      if (batchedElementMultiAspects.length === 0) {
        batchedElementMultiAspects.push(multiAspect);
        return;
      }

      // element id changed so all element's aspects are in the array and can be exported
      if (batchedElementMultiAspects[0].element.id !== multiAspect.element.id) {
        this.handler.onExportElementMultiAspects(batchedElementMultiAspects);
        await this.handler.trackProgress();
        batchedElementMultiAspects = [];
      }

      batchedElementMultiAspects.push(multiAspect);
    });

    if (batchedElementMultiAspects.length > 0) {
      // aspects that are left in the array have not been exported
      this.handler.onExportElementMultiAspects(batchedElementMultiAspects);
      await this.handler.trackProgress();
    }
  }

  private async exportAspectsLoop<T extends ElementAspect>(baseAspectClass: string, exportAspect: (aspect: T) => Promise<void>) {
    for await (const aspect of this.queryAspects<T>(baseAspectClass)) {
      if (!this.shouldExportElementAspect(aspect)) {
        continue;
      }

      await exportAspect(aspect);
    }
  }

  private async *queryAspects<T extends ElementAspect>(baseElementAspectClassFullName: string) {
    const aspectClassNameIdMap = new Map<string, Id64String>();

    const getAspectClassesSql = `SELECT DISTINCT ECClassId as classId, (ec_classname(ECClassId)) as className FROM ${baseElementAspectClassFullName}`;
    const aspectClassesQueryReader = this.sourceDb.createQueryReader(getAspectClassesSql);
    const aspectClassesAsyncQueryReader = ensureECSqlReaderIsAsyncIterableIterator(aspectClassesQueryReader);
    for await (const rowProxy of aspectClassesAsyncQueryReader) {
      const row = rowProxy.toRow();
      aspectClassNameIdMap.set(row.className, row.classId);
    }

    for (const [className, classId] of aspectClassNameIdMap) {
      if(this._excludedElementAspectClassFullNames.has(className))
        continue;

      const getAspectPropsSql = `SELECT * FROM ${className} WHERE ECClassId = :classId ORDER BY Element.Id`;
      const aspectQueryReader = this.sourceDb.createQueryReader(getAspectPropsSql, new QueryBinder().bindId("classId", classId), { rowFormat: QueryRowFormat.UseJsPropertyNames });
      const aspectAsyncQueryReader = ensureECSqlReaderIsAsyncIterableIterator(aspectQueryReader);
      let firstDone = false;
      for await (const rowProxy of aspectAsyncQueryReader) {
        const row = rowProxy.toRow();
        const aspectProps: ElementAspectProps = { ...row, classFullName: row.className.replace(".", ":"), className: undefined }; // add in property required by EntityProps
        if (!firstDone) {
          firstDone = true;
        }
        (aspectProps as any).className = undefined; // clear property from SELECT * that we don't want in the final instance
        const aspectEntity = this.sourceDb.constructEntity<T>(aspectProps);

        yield aspectEntity;
      }
    }
  }

  public async exportElementAspects(_elementId: Id64String): Promise<void> { return Promise.resolve(); }
}
