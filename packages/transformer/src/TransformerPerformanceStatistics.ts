/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

/** Operations measured by [[IModelTransformer]] when performance statistics collection is enabled.
 * @beta
 */
export enum TransformerPerformanceOperation {
  /** Total time spent in [[IModelTransformer.process]], including initialization and all nested processing operations. */
  Process = "process",
  /** Total time spent exporting and importing schemas in [[IModelTransformer.processSchemas]]. This operation is not included in `process`. */
  Schemas = "schemas",
  /** Total time spent initializing the transformer, including any change data operations. */
  Initialization = "initialization",
  /** Time spent locating and downloading changesets. Only recorded when processing changes. */
  ChangeDataAcquisition = "changeDataAcquisition",
  /** Time spent parsing change data and preparing changed entities. Only recorded when processing changes. */
  ChangeDataProcessing = "changeDataProcessing",
  /** Time spent processing CodeSpecs. */
  CodeSpecs = "codeSpecs",
  /** Time spent processing fonts. */
  Fonts = "fonts",
  /** Time spent traversing and processing models and elements, including their transformation and import. */
  ElementsAndModels = "elementsAndModels",
  /** Time spent processing ElementAspects. */
  ElementAspects = "elementAspects",
  /** Time spent processing relationships. */
  Relationships = "relationships",
  /** Time spent deleting changed models, elements, and relationships. Only recorded when processing changes. */
  Deletions = "deletions",
  /** Time spent completing elements whose required references were initially unavailable. */
  DeferredElements = "deferredElements",
  /** Time spent completing ElementAspects whose required references were initially unavailable. */
  DeferredElementAspects = "deferredElementAspects",
  /** Time spent applying requested geometry optimizations. */
  GeometryOptimization = "geometryOptimization",
  /** Time spent computing and updating project extents. */
  ProjectExtents = "projectExtents",
  /** Time spent finalizing imports, provenance, and synchronization state. */
  Finalization = "finalization",
  /** Time spent saving target changes after incremental processing. */
  SaveChanges = "saveChanges",
}

/** Aggregated measurements for a transformer operation.
 * @beta
 */
export interface TransformerPerformanceMetric {
  /** Number of times the operation was measured. */
  readonly invocationCount: number;
  /** Sum of all measured durations for the operation. */
  readonly totalMilliseconds: number;
  /** Longest measured duration for a single invocation of the operation. */
  readonly maximumMilliseconds: number;
  /** Number of measured invocations that threw an error. */
  readonly failureCount: number;
}

/** A snapshot of performance measurements collected over the lifetime of an [[IModelTransformer]].
 * Aggregate operations can contain other measured operations. For example, `process` includes `initialization`,
 * entity processing, and finalization, so operation durations must not be summed to calculate wall-clock time.
 * Operations that were not executed are omitted rather than reported with zero values.
 * @beta
 */
export interface TransformerPerformanceStatistics {
  /** Version of this statistics schema. */
  readonly schemaVersion: 1;
  /** Measurements keyed by operation. Operations that were not executed are omitted. */
  readonly operations: Readonly<
    Partial<
      Record<TransformerPerformanceOperation, TransformerPerformanceMetric>
    >
  >;
}

interface MutableTransformerPerformanceMetric {
  invocationCount: number;
  totalMilliseconds: number;
  maximumMilliseconds: number;
  failureCount: number;
}

/** @internal */
export class TransformerPerformanceCollector {
  private readonly _metrics = new Map<
    TransformerPerformanceOperation,
    MutableTransformerPerformanceMetric
  >();

  public constructor(private readonly _now = () => performance.now()) {}

  public async measure<T>(
    operation: TransformerPerformanceOperation,
    action: () => Promise<T>
  ): Promise<T> {
    const start = this._now();
    let succeeded = false;
    try {
      const result = await action();
      succeeded = true;
      return result;
    } finally {
      this.record(operation, this._now() - start, succeeded);
    }
  }

  public measureSync<T>(
    operation: TransformerPerformanceOperation,
    action: () => T
  ): T {
    const start = this._now();
    let succeeded = false;
    try {
      const result = action();
      succeeded = true;
      return result;
    } finally {
      this.record(operation, this._now() - start, succeeded);
    }
  }

  public getStatistics(): TransformerPerformanceStatistics {
    const operations: Partial<
      Record<TransformerPerformanceOperation, TransformerPerformanceMetric>
    > = {};
    for (const [operation, metric] of this._metrics) {
      operations[operation] = Object.freeze({ ...metric });
    }
    return Object.freeze({
      schemaVersion: 1,
      operations: Object.freeze(operations),
    });
  }

  private record(
    operation: TransformerPerformanceOperation,
    duration: number,
    succeeded: boolean
  ): void {
    const metric = this._metrics.get(operation) ?? {
      invocationCount: 0,
      totalMilliseconds: 0,
      maximumMilliseconds: 0,
      failureCount: 0,
    };
    metric.invocationCount++;
    metric.totalMilliseconds += duration;
    metric.maximumMilliseconds = Math.max(metric.maximumMilliseconds, duration);
    if (!succeeded) metric.failureCount++;
    this._metrics.set(operation, metric);
  }
}

const exporterCollectors = new WeakMap<
  object,
  TransformerPerformanceCollector
>();

/** @internal */
export function setExporterPerformanceCollector(
  exporter: object,
  collector: TransformerPerformanceCollector | undefined
): void {
  if (collector === undefined) {
    exporterCollectors.delete(exporter);
  } else {
    exporterCollectors.set(exporter, collector);
  }
}

/** @internal */
export function clearExporterPerformanceCollector(
  exporter: object,
  collector: TransformerPerformanceCollector | undefined
): void {
  if (exporterCollectors.get(exporter) === collector) {
    exporterCollectors.delete(exporter);
  }
}

/** @internal */
export function getExporterPerformanceCollector(
  exporter: object
): TransformerPerformanceCollector | undefined {
  return exporterCollectors.get(exporter);
}
