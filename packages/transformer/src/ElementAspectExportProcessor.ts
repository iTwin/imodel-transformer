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
import { Id64Set, Id64String, StopWatch } from "@itwin/core-bentley";
import { QueryBinder } from "@itwin/core-common";
import { ChangedInstanceOps } from "./ChangedInstanceIds";
import { ensureECSqlReaderIsAsyncIterableIterator } from "./ECSqlReaderAsyncIterableIteratorAdapter";

/** Accumulated wall time and call count for {@link ElementAspectExportProcessor.exportAllElementAspects}.
 * For perf investigation only, not a stable API.
 * @internal
 */
export interface ElementAspectExportDiagnostics {
  wallMs: number;
  callCount: number;
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
  private static _diagnostics: ElementAspectExportDiagnostics = {
    wallMs: 0,
    callCount: 0,
  };

  /** Resets accumulated diagnostics. For perf investigation only, not a stable API.
   * @internal
   */
  public static resetDiagnostics(): void {
    this._diagnostics = { wallMs: 0, callCount: 0 };
  }

  /** Accumulated wall time and call count across every {@link exportAllElementAspects}
   * call since the last {@link resetDiagnostics}. For perf investigation only, not a
   * stable API.
   * @internal
   */
  public static get diagnostics(): Readonly<ElementAspectExportDiagnostics> {
    return this._diagnostics;
  }

  private readonly _excludedElementAspectClassFullNames = new Set<string>();
  private _expandedExcludedClassFullNames:
    | Promise<ReadonlySet<string>>
    | undefined;
  private _aspectChanges: ChangedInstanceOps | undefined;

  /** ElementAspect class names excluded from source queries. */
  public get excludedElementAspectClassFullNames(): ReadonlySet<string> {
    return this._excludedElementAspectClassFullNames;
  }

  public constructor(
    private readonly _sourceDb: IModelDb,
    private readonly _handler: ElementAspectExportProcessorHandler
  ) {}

  /** Exports accepted ElementAspects owned by the supplied element IDs.
   * Multi-aspects are emitted in one callback group per owner.
   */
  public async exportAllElementAspects(
    elementIds: ReadonlySet<Id64String>
  ): Promise<void> {
    const stopwatch = new StopWatch();
    stopwatch.start();
    try {
      if (elementIds.size > 0) await this.exportAspectsForOwners(elementIds);
    } finally {
      stopwatch.stop();
      ElementAspectExportProcessor._diagnostics.wallMs +=
        stopwatch.elapsedSeconds * 1000;
      ElementAspectExportProcessor._diagnostics.callCount++;
    }
  }

  private async exportAspectsForOwners(
    elementIds: ReadonlySet<Id64String>
  ): Promise<void> {
    const multiAspectsByOwner = new Map<Id64String, ElementMultiAspect[]>();
    const excludedAspectClassFullNames =
      await this.getExpandedExcludedClassFullNames();
    for await (const aspect of this._sourceDb.elements.getAspectsForElements({
      elementIds: new Set(elementIds) as Id64Set,
      excludedAspectClassFullNames,
      groupByOwner: true,
      usePrimaryConn: true,
    })) {
      if (!(await this._handler.shouldExportElementAspect(aspect))) continue;

      if (aspect instanceof ElementUniqueAspect) {
        const isInsertChange =
          this._aspectChanges?.insertIds.has(aspect.id) ?? false;
        const isUpdateChange =
          this._aspectChanges?.updateIds.has(aspect.id) ?? false;
        await this._handler.onExportElementUniqueAspect(
          aspect,
          isUpdateChange ? true : isInsertChange ? false : undefined
        );
        await this._handler.trackProgress();
        continue;
      }

      const multiAspect = aspect as ElementMultiAspect;
      const ownerAspects = multiAspectsByOwner.get(multiAspect.element.id);
      if (ownerAspects === undefined) {
        multiAspectsByOwner.set(multiAspect.element.id, [multiAspect]);
      } else {
        ownerAspects.push(multiAspect);
      }
    }

    for (const multiAspects of multiAspectsByOwner.values()) {
      await this._handler.onExportElementMultiAspects(multiAspects);
      await this._handler.trackProgress();
    }
  }

  /** Sets the aspect changes used to distinguish inserted and updated unique aspects during change export. */
  public setAspectChanges(aspectChanges?: ChangedInstanceOps): void {
    this._aspectChanges = aspectChanges;
  }

  /** Excludes an ElementAspect class and all of its subclasses from subsequent queries and export callbacks. */
  public excludeElementAspectClass(classFullName: string): void {
    this._excludedElementAspectClassFullNames.add(classFullName);
    this._expandedExcludedClassFullNames = undefined;
  }

  private async getExpandedExcludedClassFullNames(): Promise<
    ReadonlySet<string>
  > {
    if (this._expandedExcludedClassFullNames === undefined) {
      this._expandedExcludedClassFullNames =
        this.queryExpandedExcludedClassFullNames();
    }
    return this._expandedExcludedClassFullNames;
  }

  private async queryExpandedExcludedClassFullNames(): Promise<
    ReadonlySet<string>
  > {
    const excludedClassFullNames = new Set<string>();
    for (const classFullName of this._excludedElementAspectClassFullNames) {
      const reader = this._sourceDb.createQueryReader(
        `
          SELECT ec_classname(c.ECInstanceId, 's:c') AS classFullName
          FROM ECDbMeta.ClassHasAllBaseClasses r
          JOIN ECDbMeta.ECClassDef c ON c.ECInstanceId = r.SourceECInstanceId
          WHERE r.TargetECInstanceId = ec_classId(:excludedClassName)
        `,
        new QueryBinder().bindString("excludedClassName", classFullName),
        { usePrimaryConn: true }
      );
      for await (const row of ensureECSqlReaderIsAsyncIterableIterator(reader))
        excludedClassFullNames.add(row.toRow().classFullName);
    }
    return excludedClassFullNames;
  }
}
