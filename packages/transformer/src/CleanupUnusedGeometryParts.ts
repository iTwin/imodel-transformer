import { IModelDb } from "@itwin/core-backend";
import { DbResult, Id64String } from "@itwin/core-bentley";
import { ElementGeometry } from "@itwin/core-common";

/**
 * delete all geometry parts that are not referenced by any geometric elements.
 * This will be replaced by a more integrated approach
 * @internal
 */
export function cleanupUnusedGeometryParts(db: IModelDb) {
  const unusedGeomPartIds = queryUnusedGeomParts(db);

  db.elements.deleteDefinitionElements([...unusedGeomPartIds]);
}

/**
 * queryEntityIds maxes out at 10K, we may need everything during this temporary solution
 * since geometry parts may reference each other
 */
function queryUnusedGeomParts(db: IModelDb) {
  const usedGeomParts = new Set<Id64String>();

  const allGeomElemIdsQuery = `
    SELECT ECInstanceId
    FROM bis.GeometricElement
  `;

  db.withPreparedStatement(allGeomElemIdsQuery, (geomElemIdStmt) => {
    while (geomElemIdStmt.step() === DbResult.BE_SQLITE_ROW) {
      const geomElemId = geomElemIdStmt.getValue(0).getId();
      db.elementGeometryRequest({
        elementId: geomElemId,
        skipBReps: true, // breps contain no references to geometry parts
        onGeometry(geomInfo) {
          for (const entry of new ElementGeometry.Iterator(geomInfo)) {
            const maybeGeomPart = entry.toGeometryPart();
            if (maybeGeomPart)
              usedGeomParts.add(maybeGeomPart);
          }
        },
      });
    }
  });

  const unusedGeomPartIds = db.withPreparedStatement(`
      SELECT ECInstanceId
      FROM bis.GeometryPart
      WHERE NOT InVirtualSet(?, ECInstanceId)
    `,
  (stmt) => {
    const ids = new Set<Id64String>();
    stmt.bindIdSet(1, [...usedGeomParts]);
    while (stmt.step() === DbResult.BE_SQLITE_ROW) {
      const id = stmt.getValue(0).getId();
      ids.add(id);
    }
    return ids;
  });

  return unusedGeomPartIds;
}
