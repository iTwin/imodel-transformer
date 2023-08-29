/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { ElementMultiAspect, ElementUniqueAspect } from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { ElementAspectsHandler, ExportElementAspectsStrategy } from "./ExportElementAspectsStrategy";

/**
 * Handler for [[ExportElementAspectsWithElementsStrategy]]
 * @internal
 */
export interface ElementAspectsWithElementsHandler extends ElementAspectsHandler {
  onExportElementUniqueAspect(uniqueAspect: ElementUniqueAspect, isUpdate?: boolean | undefined): void;
  onExportElementMultiAspects(multiAspects: ElementMultiAspect[]): void;
  trackProgress(): Promise<void>;
}

/**
 * ElementAspect export strategy for [[IModelExporter]].
 * This strategy exports ElementAspects together with their Elements.
 * @internal
 */
export class ExportElementAspectsWithElementsStrategy extends ExportElementAspectsStrategy<ElementAspectsWithElementsHandler> {
  public override async exportElementAspectsForElement(elementId: Id64String): Promise<void> {
    const _uniqueAspects = await Promise.all(this.sourceDb.elements
      ._queryAspects(elementId, ElementUniqueAspect.classFullName, this._excludedElementAspectClassFullNames)
      .filter((a) => this.shouldExportElementAspect(a))
      .map(async (uniqueAspect: ElementUniqueAspect) => {
        const isInsertChange = this.aspectChanges?.insertIds.has(uniqueAspect.id) ?? false;
        const isUpdateChange = this.aspectChanges?.updateIds.has(uniqueAspect.id) ?? false;
        const doExport = this.aspectChanges === undefined || isInsertChange || isUpdateChange;
        if (doExport) {
          const isKnownUpdate = this.aspectChanges ? isUpdateChange : undefined;
          this.handler.onExportElementUniqueAspect(uniqueAspect, isKnownUpdate);
          await this.handler.trackProgress();
        }
      }));

    const multiAspects = this.sourceDb.elements
      ._queryAspects(elementId, ElementMultiAspect.classFullName, this._excludedElementAspectClassFullNames)
      .filter((a) => this.shouldExportElementAspect(a));

    if (multiAspects.length > 0) {
      this.handler.onExportElementMultiAspects(multiAspects);
      return this.handler.trackProgress();
    }
  }
}
