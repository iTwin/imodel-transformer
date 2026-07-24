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
    const allAspects = this.sourceDb.elements.getAspects(
      elementId,
      undefined,
      this.excludedElementAspectClassFullNames
    );
    for (const uniqueAspect of allAspects) {
      if (
        !(uniqueAspect instanceof ElementUniqueAspect) ||
        !(await this.shouldExportElementAspect(uniqueAspect))
      ) {
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

    const multiAspects: ElementMultiAspect[] = [];
    for (const aspect of allAspects) {
      if (
        aspect instanceof ElementMultiAspect &&
        (await this.shouldExportElementAspect(aspect))
      ) {
        multiAspects.push(aspect);
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
