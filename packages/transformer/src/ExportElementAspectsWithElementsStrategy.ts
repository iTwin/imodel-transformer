/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { ElementMultiAspect, ElementUniqueAspect } from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { ExportElementAspectsStrategy } from "./ExportElementAspectsStrategy";

/**
 * ElementAspect export strategy for [[IModelExporter]].
 * This strategy exports ElementAspects together with their Elements.
 * @internal
 */
export class ExportElementAspectsWithElementsStrategy extends ExportElementAspectsStrategy {
  public override async exportElementAspectsForElement(
    elementId: Id64String
  ): Promise<void> {
    const allUniqueAspects = this.sourceDb.elements._queryAspects(
      elementId,
      ElementUniqueAspect.classFullName,
      this.excludedElementAspectClassFullNames
    );
    for (const uniqueAspect of allUniqueAspects) {
      if (!(await this.shouldExportElementAspect(uniqueAspect))) {
        continue;
      }
      const isInsertChange =
        this.aspectChanges?.insertIds.has(uniqueAspect.id) ?? false;
      const isUpdateChange =
        this.aspectChanges?.updateIds.has(uniqueAspect.id) ?? false;
      const doExport =
        this.aspectChanges === undefined || isInsertChange || isUpdateChange;
      if (doExport) {
        const isKnownUpdate = this.aspectChanges ? isUpdateChange : undefined;
        await this.handler.onExportElementUniqueAspect(
          uniqueAspect,
          isKnownUpdate
        );
        await this.handler.trackProgress();
      }
    }

    const allMultiAspects = this.sourceDb.elements._queryAspects(
      elementId,
      ElementMultiAspect.classFullName,
      this.excludedElementAspectClassFullNames
    );
    const multiAspects: ElementMultiAspect[] = [];
    for (const a of allMultiAspects) {
      if (await this.shouldExportElementAspect(a)) {
        multiAspects.push(a);
      }
    }

    if (multiAspects.length > 0) {
      await this.handler.onExportElementMultiAspects(multiAspects);
      return this.handler.trackProgress();
    }
  }

  public override async exportAllElementAspects(): Promise<void> {
    // All aspects are exported with their owning elements and don't need to be exported separately.
  }
}
