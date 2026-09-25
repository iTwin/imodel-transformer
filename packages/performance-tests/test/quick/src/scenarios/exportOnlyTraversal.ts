/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Element as BisElement, Model } from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import {
  ExportAllOptions,
  IModelExporter,
  IModelExportHandler,
} from "@itwin/imodel-transformer";
import { canonicalSha256 } from "../fixtures/FixtureDescriptor.js";
import {
  PreparedDataset,
  requireStandaloneDataset,
} from "../fixtures/FixtureProvider.js";
import { hierarchyHeavyExportFixture } from "../fixtures/recipes/hierarchyHeavyExport.js";
import {
  BenchmarkRegistration,
  defineBenchmark,
} from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";

type ExportTraversal = NonNullable<ExportAllOptions["traversal"]>;

/**
 * Counts every element and model callback without doing any work, so the measurement
 * isolates the exporter's traversal (queries, element loads, callback dispatch) from
 * any import or transformation cost.
 */
class CountingExportHandler extends IModelExportHandler {
  public elementIds = new Set<Id64String>();
  public modelIds = new Set<Id64String>();
  public skippedElementCount = 0;

  public override async onExportElement(element: BisElement): Promise<void> {
    this.elementIds.add(element.id);
  }

  public override async onSkipElement(): Promise<void> {
    this.skippedElementCount++;
  }

  public override async onExportModel(model: Model): Promise<void> {
    this.modelIds.add(model.id);
  }
}

/**
 * Export-only benchmark: run `IModelExporter.exportAll` against the source with a no-op
 * counting handler and no target writes. Registered once per traversal mode so
 * `QUICK_PERF_SCENARIO=export-only-hierarchy-traversal` versus
 * `QUICK_PERF_SCENARIO=export-only-linear-traversal` is a reproducible A/B of
 * [ExportAllOptions.traversal]($transformer) over the same hierarchy-rich fixture.
 */
function exportOnlyTraversal(traversal: ExportTraversal) {
  return (dataset: PreparedDataset): BenchmarkScenario => {
    const { sourceDb } = requireStandaloneDataset(dataset);
    const handler = new CountingExportHandler();
    const exporter = new IModelExporter(sourceDb);
    exporter.registerHandler(handler);
    return {
      abort() {},
      async measure() {
        await exporter.exportAll({ traversal });
      },
      async finish() {
        // both traversals must observe the same entity sets; the digest ignores order
        return canonicalSha256({
          elementIds: [...handler.elementIds].sort(),
          modelIds: [...handler.modelIds].sort(),
          skippedElementCount: handler.skippedElementCount,
        });
      },
    };
  };
}

function exportOnlyTraversalScenario(
  traversal: ExportTraversal
): BenchmarkScenarioDefinition {
  return {
    id: `export-only-${traversal}-traversal`,
    defaultFixtureId: "hierarchy-heavy-export",
    capabilities: {
      topology: "standalone-source-and-empty-target",
      requiredClaims: ["export-only traversal"],
    },
    configuration: { traversal },
    factory: exportOnlyTraversal(traversal),
  };
}

export const exportOnlyHierarchyTraversalScenario =
  exportOnlyTraversalScenario("hierarchy");

export const exportOnlyLinearTraversalScenario =
  exportOnlyTraversalScenario("linear");

export const exportOnlyHierarchyTraversalBenchmark: BenchmarkRegistration =
  defineBenchmark({
    scenario: exportOnlyHierarchyTraversalScenario,
    fixtures: [hierarchyHeavyExportFixture],
  });

export const exportOnlyLinearTraversalBenchmark: BenchmarkRegistration =
  defineBenchmark({
    scenario: exportOnlyLinearTraversalScenario,
    // the fixture is registered once, by the hierarchy benchmark
  });
