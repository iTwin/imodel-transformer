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
import { Id64String, Logger } from "@itwin/core-bentley";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import { ChangedInstanceOps } from "./IModelExporter";

const loggerCategory = TransformerLoggerCategory.IModelExporter;

/**
 * Handler for [[ExportElementAspectsStrategy]]
 * @internal
 */
export interface ElementAspectsHandler {
  shouldExportElementAspect(aspect: ElementAspect): boolean;
  onExportElementUniqueAspect(
    uniqueAspect: ElementUniqueAspect,
    isUpdate?: boolean | undefined
  ): void;
  onExportElementMultiAspects(multiAspects: ElementMultiAspect[]): void;
  trackProgress: () => Promise<void>;
}

/**
 * Base ElementAspect export strategy. Base export strategy includes state saving and loading and
 * ElementAspect filtering.
 * @internal
 */
export abstract class ExportElementAspectsStrategy {
  /** The set of classes of ElementAspects that will be excluded (polymorphically) from transformation to the target iModel. */
  protected _excludedElementAspectClasses = new Set<typeof ElementAspect>();
  /** The set of classFullNames for ElementAspects that will be excluded from transformation to the target iModel. */
  public readonly excludedElementAspectClassFullNames = new Set<string>();

  protected sourceDb: IModelDb;

  protected aspectChanges: ChangedInstanceOps | undefined;

  protected handler: ElementAspectsHandler;

  public constructor(sourceDb: IModelDb, handler: ElementAspectsHandler) {
    this.sourceDb = sourceDb;
    this.handler = handler;
  }

  public abstract exportElementAspectsForElement(
    _elementId: Id64String
  ): Promise<void>;
  public abstract exportAllElementAspects(): Promise<void>;

  protected shouldExportElementAspect(aspect: ElementAspect): boolean {
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
    // ElementAspect has passed standard exclusion rules, now give handler a chance to accept/reject
    return this.handler.shouldExportElementAspect(aspect);
  }

  public setAspectChanges(aspectChanges?: ChangedInstanceOps) {
    this.aspectChanges = aspectChanges;
  }

  public loadExcludedElementAspectClasses(
    excludedElementAspectClassFullNames: string[]
  ): void {
    (this.excludedElementAspectClassFullNames as any) = new Set(
      excludedElementAspectClassFullNames
    );
    this._excludedElementAspectClasses = new Set(
      excludedElementAspectClassFullNames.map((c) =>
        this.sourceDb.getJsClass(c)
      )
    );
  }

  public excludeElementAspectClass(classFullName: string): void {
    this.excludedElementAspectClassFullNames.add(classFullName); // allows non-polymorphic exclusion before query
    this._excludedElementAspectClasses.add(
      this.sourceDb.getJsClass<typeof ElementAspect>(classFullName)
    ); // allows polymorphic exclusion after query/load
  }
}
