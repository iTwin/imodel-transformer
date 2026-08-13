/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Guid } from "@itwin/core-bentley";
import { Code, IModel, PhysicalElementProps } from "@itwin/core-common";
import {
  ElementGroupsMembers,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { configureFixture, defineFixtureRecipe } from "../FixtureRecipe.js";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import { quickPath } from "../../support/paths.js";

export interface RelationshipHeavyTransformParameters {
  readonly elementCount: number;
  readonly relationshipsPerElement: number;
}

/** Every twentieth element deliberately has no FederationGuid so relationship export
 * exercises both the fedguid-present and fedguid-absent endpoint paths.
 */
const missingFederationGuidStride = 20;

function distribution(
  parameters: Readonly<RelationshipHeavyTransformParameters>
): FixtureDistribution {
  if (
    !Number.isSafeInteger(parameters.elementCount) ||
    parameters.elementCount < missingFederationGuidStride
  )
    throw new Error(
      `Relationship-heavy elementCount must be a safe integer of at least ${missingFederationGuidStride}`
    );
  if (
    !Number.isSafeInteger(parameters.relationshipsPerElement) ||
    parameters.relationshipsPerElement < 1 ||
    parameters.relationshipsPerElement >= parameters.elementCount
  )
    throw new Error(
      "Relationship-heavy relationshipsPerElement must be a positive safe integer smaller than elementCount"
    );
  return {
    base: {
      aspects: 0,
      elements: parameters.elementCount,
      geometricElements: 0,
      relationships:
        parameters.elementCount * parameters.relationshipsPerElement,
    },
    operations: {
      aspects: { deletes: 0, inserts: 0, updates: 0 },
      elements: { deletes: 0, inserts: 0, updates: 0 },
      relationships: { deletes: 0, inserts: 0, updates: 0 },
      geometryUpdates: 0,
      sourceChangesets: 0,
    },
  };
}

function deterministicFederationGuid(index: number): string {
  return index % missingFederationGuidStride === 0
    ? Guid.empty // Guid.empty makes insertElement persist no FederationGuid
    : `00000000-0000-4000-8000-${index.toString().padStart(12, "0")}`;
}

async function queryCount(db: IModelDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(
      `Relationship-heavy fixture count query returned no row: ${ecsql}`
    );
  return reader.current.cnt as number;
}

/**
 * Creates a standalone source dominated by relationship content: one physical model,
 * one spatial category, `elementCount` non-geometric Generic.PhysicalObjects, and
 * `relationshipsPerElement` ElementGroupsMembers relationships per element with
 * deterministic member priorities. Every twentieth element has no FederationGuid.
 * The default configured fixture contains 5,000 elements and 30,000 relationships.
 */
export const relationshipHeavyTransformRecipe = defineFixtureRecipe({
  id: "relationship-heavy-transform",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "relationshipHeavyTransform.ts"),
    ],
    values: { content: "element-groups-members-ring-v1" },
  },
  distribution,
  async createSeed(fileName, context) {
    const { elementCount, relationshipsPerElement } = context.parameters;
    const db = SnapshotDb.createEmpty(fileName, {
      rootSubject: { name: context.descriptor.id },
    });
    try {
      const elementIds = withEditTxn(
        db,
        "create relationship-heavy elements",
        (txn) => {
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "QuickRelationshipHeavyCategory",
            {}
          );
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "QuickRelationshipHeavyModel"
          );
          return Array.from({ length: elementCount }, (_, index) => {
            const props: PhysicalElementProps = {
              classFullName: PhysicalObject.classFullName,
              model: modelId,
              category: categoryId,
              code: Code.createEmpty(),
              federationGuid: deterministicFederationGuid(index),
              userLabel: `RelHeavy-${index}`,
            };
            return txn.insertElement(props);
          });
        }
      );
      withEditTxn(db, "create relationship-heavy relationships", (txn) => {
        for (let index = 0; index < elementCount; index++) {
          for (let offset = 0; offset < relationshipsPerElement; offset++) {
            const relationship = ElementGroupsMembers.create(
              db,
              elementIds[index],
              elementIds[(index + offset + 1) % elementCount],
              offset
            );
            txn.insertRelationship(relationship.toJSON());
          }
        }
      });
    } finally {
      db.close();
    }
  },
  async applySourceChangesets() {},
  async validate(db, context) {
    const { elementCount, relationshipsPerElement } = context.parameters;
    const expected = {
      elements: elementCount,
      relationships: elementCount * relationshipsPerElement,
      elementsWithoutFederationGuid: Math.ceil(
        elementCount / missingFederationGuidStride
      ),
      relationshipPriorityLevels: relationshipsPerElement,
    };
    const actual = {
      elements: await queryCount(
        db,
        "SELECT count(*) cnt FROM Generic.PhysicalObject"
      ),
      relationships: await queryCount(
        db,
        "SELECT count(*) cnt FROM bis.ElementGroupsMembers"
      ),
      elementsWithoutFederationGuid: await queryCount(
        db,
        "SELECT count(*) cnt FROM Generic.PhysicalObject WHERE FederationGuid IS NULL"
      ),
      relationshipPriorityLevels: await queryCount(
        db,
        "SELECT count(DISTINCT MemberPriority) cnt FROM bis.ElementGroupsMembers"
      ),
    };
    if (JSON.stringify(actual) !== JSON.stringify(expected))
      throw new Error(
        `Relationship-heavy fixture distribution mismatch: expected=${JSON.stringify(
          expected
        )}, actual=${JSON.stringify(actual)}`
      );
  },
});

export const relationshipHeavyTransformFixture = configureFixture(
  relationshipHeavyTransformRecipe,
  {
    id: "relationship-heavy-transform",
    version: 1,
    label: "relationship-heavy standalone transformation",
    scenarioClaims: ["full transformation"],
    topology: "standalone-source-and-empty-target",
    seed: 15485863,
    parameters: { elementCount: 5000, relationshipsPerElement: 6 },
  }
);

export const relationshipHeavyTransformDescriptor =
  relationshipHeavyTransformFixture.descriptor;
