/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { ElementAspect, IModelDb } from "@itwin/core-backend";
import { Id64String, Logger } from "@itwin/core-bentley";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";
import { ChangedInstanceOps, IModelExportHandler } from "./IModelExporter";

const loggerCategory = TransformerLoggerCategory.IModelExporter;

/**
 * Handler for [[ExportElementAspectsStrategy]]
 * @internal
 */
export interface ElementAspectsHandler {
  shouldExportElementAspect(aspect: ElementAspect): boolean;
  trackProgress: () => Promise<void>;
}

/**
 * Base ElementAspect export strategy. Base export strategy includes state saving and loading and
 * ElementAspect filtering.
 * @internal
 */
export abstract class ExportElementAspectsStrategy<T extends ElementAspectsHandler> {
  /** The set of classes of ElementAspects that will be excluded (polymorphically) from transformation to the target iModel. */
  protected _excludedElementAspectClasses = new Set<typeof ElementAspect>();
  /** The set of classFullNames for ElementAspects that will be excluded from transformation to the target iModel. */
  public readonly excludedElementAspectClassFullNames = new Set<string>();

  protected sourceDb: IModelDb;

  protected aspectChanges: ChangedInstanceOps | undefined;

  private _handler: T | undefined = undefined;
  protected get handler(): T {
    if (!this._handler) {
      throw new Error("Registered handler getter didn't return handler");
    }
    return this._handler;
  }

  public constructor(sourceDb: IModelDb) {
    this.sourceDb = sourceDb;
  }

  public async exportElementAspectsForElement(_elementId: Id64String): Promise<void> { }
  public async exportAllElementAspects(): Promise<void> { }

  protected shouldExportElementAspect(aspect: ElementAspect): boolean {
    for (const excludedElementAspectClass of this._excludedElementAspectClasses) {
      if (aspect instanceof excludedElementAspectClass) {
        Logger.logInfo(loggerCategory, `Excluded ElementAspect by class: ${aspect.classFullName}`);
        return false;
      }
    }
    // ElementAspect has passed standard exclusion rules, now give handler a chance to accept/reject
    return this.handler.shouldExportElementAspect(aspect);
  }

  public registerHandler(handler: () => IModelExportHandler, trackProgress: () => Promise<void>) {
    this._handler = {
      shouldExportElementAspect: (aspect: ElementAspect) => handler().shouldExportElementAspect(aspect),
      trackProgress,
    } as T; // casting here since we only assign partial T here and subclasses will have to assign the rest of properties
  }

  public setAspectChanges(aspectChanges?: ChangedInstanceOps) {
    this.aspectChanges = aspectChanges;
  }

  // public getExcludedElementAspectClasses(): Set<string> {
  //   return this._excludedElementAspectClassFullNames;
  // }

  public loadExcludedElementAspectClasses(excludedElementAspectClassFullNames: string[]): void {
    (this.excludedElementAspectClassFullNames as any) = new Set(excludedElementAspectClassFullNames);
    this._excludedElementAspectClasses = new Set(excludedElementAspectClassFullNames.map((c) => this.sourceDb.getJsClass(c)));
  }

  public excludeElementAspectClass(classFullName: string): void {
    this.excludedElementAspectClassFullNames.add(classFullName); // allows non-polymorphic exclusion before query
    this._excludedElementAspectClasses.add(this.sourceDb.getJsClass<typeof ElementAspect>(classFullName)); // allows polymorphic exclusion after query/load
  }
}
