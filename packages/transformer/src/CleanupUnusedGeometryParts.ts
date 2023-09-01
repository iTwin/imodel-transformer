import { IModelDb } from "@itwin/core-backend";
import { DbResult, Id64String } from "@itwin/core-bentley";
import { ElementGeometry } from "@itwin/core-common";

/**
 * delete all geometry parts that are not referenced by any geometric elements.
 * This will be replaced by a more integrated approach
 * @internal
 */
export function cleanupUnusedGeometryParts(db: IModelDb) {
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

  // NOTE: maybe (negligbly?) faster to do custom query with `NOT InVirtualSet()`
  const unusedGeomPartIds = db.queryEntityIds({ from: "bis.GeometryPart" });
  for (const usedGeomPartId of usedGeomParts)
    unusedGeomPartIds.delete(usedGeomPartId);
  db.elements.deleteDefinitionElements([...unusedGeomPartIds]);
}
