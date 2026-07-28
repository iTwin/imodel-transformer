/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  BriefcaseDb,
  ExternalSourceAspect,
  PhysicalObject,
} from "@itwin/core-backend";
import { IModel, QueryBinder } from "@itwin/core-common";
import { YawPitchRollAngles } from "@itwin/core-geometry";
import { DatasetDescriptor } from "../DatasetDescriptor";
import { canonicalSha256 } from "../FixtureManifest";
import { createBoxGeometry } from "../recipes/balancedIncremental";

function normalizedGeometryStream(geometry: unknown): unknown {
  if (!Array.isArray(geometry)) return geometry;
  return geometry.filter((entry: unknown) => {
    if (typeof entry !== "object" || entry === null) return true;
    const properties = Object.keys(entry);
    if (properties.length !== 1 || properties[0] !== "header") return true;
    const header = (entry as { header?: unknown }).header;
    return !(
      typeof header === "object" &&
      header !== null &&
      Object.keys(header).length === 1 &&
      (header as { flags?: unknown }).flags === 0
    );
  });
}

function normalizedAngles(value: unknown): unknown {
  if (typeof value === "number") return Number(value.toFixed(12));
  if (Array.isArray(value)) return value.map(normalizedAngles);
  if (typeof value === "object" && value !== null)
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        normalizedAngles(entry),
      ])
    );
  return value;
}

async function queryValues(
  db: BriefcaseDb,
  ecsql: string,
  propertyName: string
): Promise<unknown[]> {
  const values: unknown[] = [];
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  while (await reader.step()) values.push(reader.current[propertyName]);
  return values;
}

async function queryRecords(
  db: BriefcaseDb,
  ecsql: string,
  propertyNames: readonly string[]
): Promise<unknown[]> {
  const values: unknown[] = [];
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  while (await reader.step()) {
    values.push(
      Object.fromEntries(
        propertyNames.map((propertyName) => [
          propertyName,
          reader.current[propertyName],
        ])
      )
    );
  }
  return values;
}

async function queryGeometryRecords(db: BriefcaseDb): Promise<unknown[]> {
  const values: unknown[] = [];
  const reader = db.createQueryReader(
    "SELECT ECInstanceId id FROM Generic.PhysicalObject WHERE UserLabel IS NOT NULL ORDER BY UserLabel",
    undefined,
    { usePrimaryConn: true }
  );
  while (await reader.step()) {
    const element = db.elements.getElement<PhysicalObject>({
      id: reader.current.id as string,
      wantGeometry: true,
    });
    const range = element.calculateRange3d();
    values.push({
      angles: normalizedAngles(element.placement.angles.toJSON()),
      geometry: normalizedGeometryStream(element.geom),
      hasGeometry: element.geom !== undefined,
      label: element.userLabel,
      origin: element.placement.origin.toJSON(),
      range: range.isNull
        ? undefined
        : {
            high: range.high.toJSON(),
            low: range.low.toJSON(),
          },
    });
  }
  return values;
}

async function queryCount(db: BriefcaseDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(`Count query returned no rows: ${ecsql}`);
  return reader.current.cnt as number;
}

async function queryGeometryUpdateCount(db: BriefcaseDb): Promise<number> {
  const reader = db.createQueryReader(
    "SELECT ECInstanceId id FROM Generic.PhysicalObject WHERE UserLabel LIKE 'updated-%'",
    undefined,
    { usePrimaryConn: true }
  );
  let count = 0;
  while (await reader.step()) {
    const element = db.elements.getElement<PhysicalObject>({
      id: reader.current.id as string,
      wantGeometry: true,
    });
    const match = /^updated-(\d+)$/.exec(element.userLabel ?? "");
    if (!match) continue;
    const index = Number(match[1]);
    const unitOffset = index % 240;
    if (unitOffset >= 6) continue;
    const range = element.calculateRange3d();
    const expectedLength = unitOffset + 2;
    const expectedAngles = YawPitchRollAngles.createDegrees(
      0,
      0,
      unitOffset
    ).toJSON();
    if (
      element.placement.origin.x !== index + 1000 ||
      element.placement.origin.y !== index ||
      element.placement.origin.z !== 0
    )
      continue;
    const actualAnglesHash = canonicalSha256(
      normalizedAngles(element.placement.angles.toJSON())
    );
    const expectedAnglesHash = canonicalSha256(
      normalizedAngles(expectedAngles)
    );
    if (actualAnglesHash !== expectedAnglesHash)
      throw new Error(
        `Geometry update angle mismatch for ${element.userLabel}: expected=${expectedAnglesHash}, actual=${actualAnglesHash}`
      );
    const actualGeometryHash = canonicalSha256(
      normalizedGeometryStream(element.geom)
    );
    const expectedGeometryHash = canonicalSha256(
      normalizedGeometryStream(createBoxGeometry(expectedLength))
    );
    if (actualGeometryHash !== expectedGeometryHash)
      throw new Error(
        `Geometry update stream mismatch for ${element.userLabel}: expected=${expectedGeometryHash}, actual=${actualGeometryHash}`
      );
    if (
      range.isNull ||
      range.high.x <= range.low.x ||
      range.high.y <= range.low.y ||
      range.high.z <= range.low.z
    )
      throw new Error(
        `Geometry update has an invalid range: ${element.userLabel}`
      );
    count++;
  }
  return count;
}

export async function assertFixtureDistribution(
  db: BriefcaseDb,
  descriptor: DatasetDescriptor
): Promise<void> {
  const expected = {
    elements:
      descriptor.distribution.base.elements +
      descriptor.distribution.operations.elements.inserts -
      descriptor.distribution.operations.elements.deletes,
    multiAspects:
      descriptor.distribution.base.elements +
      descriptor.distribution.operations.aspects.inserts -
      descriptor.distribution.operations.elements.deletes,
    relationships:
      descriptor.distribution.base.relationships +
      descriptor.distribution.operations.relationships.inserts -
      descriptor.distribution.operations.relationships.deletes,
    relationshipUpdates:
      descriptor.distribution.operations.relationships.updates,
    elementUpdates: descriptor.distribution.operations.elements.updates,
    aspectUpdates: descriptor.distribution.operations.aspects.updates,
    geometryUpdates: descriptor.distribution.operations.geometryUpdates,
    uniqueAspects:
      descriptor.distribution.base.elements -
      descriptor.distribution.operations.elements.deletes,
  };
  const actual = {
    elements: await queryCount(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject"
    ),
    multiAspects: await queryCount(
      db,
      "SELECT count(*) cnt FROM QuickPerf.BalancedMultiAspect"
    ),
    relationships: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.ElementGroupsMembers"
    ),
    relationshipUpdates: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.ElementGroupsMembers WHERE MemberPriority>=1000"
    ),
    elementUpdates: await queryCount(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject WHERE UserLabel LIKE 'updated-%'"
    ),
    aspectUpdates: await queryCount(
      db,
      "SELECT count(*) cnt FROM QuickPerf.BalancedMultiAspect WHERE Payload LIKE 'updated-aspect-%'"
    ),
    geometryUpdates: await queryGeometryUpdateCount(db),
    uniqueAspects: await queryCount(
      db,
      "SELECT count(*) cnt FROM QuickPerf.BalancedUniqueAspect"
    ),
  };
  if (JSON.stringify(actual) !== JSON.stringify(expected))
    throw new Error(
      `Fixture distribution mismatch: expected=${JSON.stringify(
        expected
      )}, actual=${JSON.stringify(actual)}`
    );
  if (
    db.changeset.index !== descriptor.distribution.operations.sourceChangesets
  )
    throw new Error(
      `Expected ${descriptor.distribution.operations.sourceChangesets} source changesets, got ${db.changeset.index}`
    );
}

export async function assertSynchronizationProvenance(
  sourceDb: BriefcaseDb,
  targetDb: BriefcaseDb
): Promise<void> {
  const binder = new QueryBinder()
    .bindId("elementId", IModel.rootSubjectId)
    .bindId("scopeId", IModel.rootSubjectId)
    .bindString("kind", ExternalSourceAspect.Kind.Scope)
    .bindString("identifier", sourceDb.iModelId);
  const reader = targetDb.createQueryReader(
    `SELECT Identifier identifier,Version version
     FROM bis.ExternalSourceAspect
     WHERE Element.Id=:elementId
       AND Scope.Id=:scopeId
       AND Kind=:kind
       AND Identifier=:identifier`,
    binder,
    { usePrimaryConn: true }
  );
  if (!(await reader.step()))
    throw new Error("Target synchronization scope provenance was not found");
  const identifier = reader.current.identifier as string;
  const version = reader.current.version as string;
  if (await reader.step())
    throw new Error(
      "Target contains duplicate synchronization scope provenance"
    );
  if (identifier !== sourceDb.iModelId)
    throw new Error(
      `Synchronization source mismatch: expected=${sourceDb.iModelId}, actual=${identifier}`
    );
  const expectedVersion = `${sourceDb.changeset.id};${sourceDb.changeset.index}`;
  if (version !== expectedVersion)
    throw new Error(
      `Synchronization version mismatch: expected=${expectedVersion}, actual=${version}`
    );
}

async function semanticContent(db: BriefcaseDb) {
  return {
    aspects: await queryRecords(
      db,
      "SELECT a.Payload payload,a.Sequence sequence,e.UserLabel owner FROM QuickPerf.BalancedMultiAspect a JOIN bis.Element e ON e.ECInstanceId=a.Element.Id ORDER BY e.UserLabel,a.Payload",
      ["owner", "payload", "sequence"]
    ),
    elementLabels: await queryValues(
      db,
      "SELECT UserLabel label FROM Generic.PhysicalObject WHERE UserLabel IS NOT NULL ORDER BY UserLabel",
      "label"
    ),
    geometry: await queryGeometryRecords(db),
    relationships: await queryRecords(
      db,
      "SELECT s.UserLabel sourceLabel,t.UserLabel targetLabel,r.MemberPriority priority FROM bis.ElementGroupsMembers r JOIN bis.Element s ON s.ECInstanceId=r.SourceECInstanceId JOIN bis.Element t ON t.ECInstanceId=r.TargetECInstanceId ORDER BY s.UserLabel,t.UserLabel,r.MemberPriority",
      ["priority", "sourceLabel", "targetLabel"]
    ),
    uniqueAspects: await queryRecords(
      db,
      "SELECT a.Payload payload,a.Sequence sequence,e.UserLabel owner FROM QuickPerf.BalancedUniqueAspect a JOIN bis.Element e ON e.ECInstanceId=a.Element.Id ORDER BY e.UserLabel,a.Payload",
      ["owner", "payload", "sequence"]
    ),
  };
}

export async function semanticDigest(db: BriefcaseDb): Promise<string> {
  return canonicalSha256(await semanticContent(db));
}

export async function assertSemanticallyEqual(
  sourceDb: BriefcaseDb,
  targetDb: BriefcaseDb
): Promise<string> {
  const [sourceContent, targetContent] = await Promise.all([
    semanticContent(sourceDb),
    semanticContent(targetDb),
  ]);
  const sourceDigest = canonicalSha256(sourceContent);
  const targetDigest = canonicalSha256(targetContent);
  if (sourceDigest !== targetDigest)
    throw new Error(
      `Fixture semantic digest mismatch: source=${sourceDigest}, target=${targetDigest}, sourceCounts=${JSON.stringify(
        Object.fromEntries(
          Object.entries(sourceContent).map(([key, values]) => [
            key,
            values.length,
          ])
        )
      )}, targetCounts=${JSON.stringify(
        Object.fromEntries(
          Object.entries(targetContent).map(([key, values]) => [
            key,
            values.length,
          ])
        )
      )}, categoryHashes=${JSON.stringify(
        Object.fromEntries(
          Object.keys(sourceContent).map((key) => [
            key,
            {
              source: canonicalSha256(
                sourceContent[key as keyof typeof sourceContent]
              ),
              target: canonicalSha256(
                targetContent[key as keyof typeof targetContent]
              ),
            },
          ])
        )
      )}`
    );
  return sourceDigest;
}
