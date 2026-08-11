/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/**
 * Test helpers for materializing Element instances from the polymorphic
 * instance query `SELECT $ FROM bis.Element` (issue #7 investigation).
 *
 * `SELECT $` returns raw ECSQL instance JSON, which differs from the
 * ElementProps shape `getElement` produces. These helpers perform the
 * post-processing needed for parity, characterized and asserted by
 * `standalone/BulkElementMaterialization.test.ts`:
 * - property names arrive camelCased by USE_JS_PROP_NAMES but must be run
 *   through `ECJsNames.toJsName` (like ElementAspectExportProcessor does),
 * - `className` ("Schema.Class") becomes `classFullName` ("Schema:Class"),
 *   and nested `relClassName` values need the same separator fix,
 * - nav props (`model`, `codeSpec`, `codeScope`, `category`) flatten to ids,
 * - flat `origin`/`yaw`/`pitch`/`roll`/`rotation`/`bBoxLow`/`bBoxHigh`
 *   reassemble into `placement` (3d or 2d),
 * - `jsonProperties` and SubCategory `properties` are JSON strings to parse,
 * - `geometryStream` is the raw binary flatbuffer, NOT the
 *   GeometryStreamProps JSON that `wantGeometry: true` yields, so it is
 *   surfaced separately instead of being mapped to `geom`.
 */

import { IModelDb } from "@itwin/core-backend";
import { assert, Id64String } from "@itwin/core-bentley";
import {
  Base64EncodedString,
  ECJsNames,
  ElementProps,
  Placement2dProps,
  Placement3dProps,
} from "@itwin/core-common";
import { ensureECSqlReaderIsAsyncIterableIterator } from "../../ECSqlReaderAsyncIterableIteratorAdapter";

export interface NavPropValue {
  id: Id64String;
  relClassName?: string;
}

/** Parse one raw `SELECT $` row (with USE_JS_PROP_NAMES) into a normalized
 * property bag, the same way ElementAspectExportProcessor does. */
export function parseInstanceRow(
  rawInstance: unknown
): Record<string, unknown> {
  const parsed: unknown =
    typeof rawInstance === "string"
      ? JSON.parse(rawInstance, Base64EncodedString.reviver)
      : rawInstance;
  assert(
    typeof parsed === "object" && parsed !== null && !Array.isArray(parsed),
    "expected an Element instance query to return an object"
  );
  const row: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(parsed)) {
    const ecPropertyName =
      key.length === 0 ? key : key[0].toUpperCase() + key.substring(1);
    row[ECJsNames.toJsName(ecPropertyName)] = value;
  }
  return row;
}

/** `$` renders class references as "Schema.Class"; element props use "Schema:Class" */
const fixClassSeparator = (name: string) => name.replace(".", ":");

/** Recursively normalize relClassName values in auto-handled passthrough props */
function normalizeRelClassNames(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalizeRelClassNames);
  if (typeof value === "object" && value !== null) {
    const result: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) {
      result[key] =
        key === "relClassName" && typeof entry === "string"
          ? fixClassSeparator(entry)
          : normalizeRelClassNames(entry);
    }
    return result;
  }
  return value;
}

/** Convert a parsed `$` row into ElementProps comparable with
 * `getElement({ wantGeometry: false }).toJSON()`.
 * Custom-handled properties (code, placement, geometry, SubCategory
 * appearance) need explicit reassembly; everything else passes through. */
export function instanceRowToElementProps(row: Record<string, unknown>): {
  props: ElementProps;
  geometryStream: unknown;
} {
  const consumed = new Set<string>();
  const take = <T>(key: string): T | undefined => {
    consumed.add(key);
    return row[key] as T | undefined;
  };

  const className = take<string>("className");
  assert(typeof className === "string", "instance row must have className");

  const props: Record<string, unknown> = {
    id: take<Id64String>("id"),
    classFullName: fixClassSeparator(className),
    // ElementProps.model is a plain Id64String; `$` returns a nav-prop object
    model: take<NavPropValue>("model")?.id,
    code: {
      spec: take<NavPropValue>("codeSpec")?.id,
      scope: take<NavPropValue>("codeScope")?.id,
      // native getElementJson returns "" for NULL CodeValue; `$` omits it
      value: take<string>("codeValue") ?? "",
    },
  };

  const parent = take<NavPropValue>("parent");
  if (parent !== undefined)
    props.parent = {
      ...parent,
      ...(parent.relClassName !== undefined && {
        relClassName: fixClassSeparator(parent.relClassName),
      }),
    };

  const federationGuid = take<unknown>("federationGuid");
  if (federationGuid !== undefined) props.federationGuid = federationGuid;

  const userLabel = take<string>("userLabel");
  if (userLabel !== undefined) props.userLabel = userLabel;

  // JsonProperties is a string ECProperty; element props hold the parsed object
  const jsonProperties = take<string>("jsonProperties");
  if (jsonProperties !== undefined)
    props.jsonProperties = JSON.parse(jsonProperties);

  // custom-handled: SubCategory persists appearance in its Properties column
  if (className === "BisCore.SubCategory") {
    const appearanceJson = take<string>("properties");
    if (appearanceJson !== undefined && appearanceJson !== "")
      props.appearance = JSON.parse(appearanceJson);
  }

  // custom-handled: GeometricElement flat origin/rotation props -> placement
  if ("origin" in row) {
    const origin = take<object>("origin");
    const bboxLow = take<object>("bBoxLow");
    const bboxHigh = take<object>("bBoxHigh");
    const bbox =
      bboxLow !== undefined && bboxHigh !== undefined
        ? { low: bboxLow, high: bboxHigh }
        : undefined;
    if ("rotation" in row) {
      const placement: Placement2dProps = {
        origin: origin as Placement2dProps["origin"],
        angle: take<number>("rotation") ?? 0,
        ...(bbox && { bbox }),
      };
      props.placement = placement;
    } else {
      const placement: Placement3dProps = {
        origin: origin as Placement3dProps["origin"],
        angles: {
          yaw: take<number>("yaw") ?? 0,
          pitch: take<number>("pitch") ?? 0,
          roll: take<number>("roll") ?? 0,
        },
        ...(bbox && { bbox }),
      };
      props.placement = placement;
    }
    const category = take<NavPropValue>("category");
    if (category !== undefined) props.category = category.id;
    consumed.add("inSpatialIndex");
    const typeDefinition = take<NavPropValue>("typeDefinition");
    if (typeDefinition !== undefined)
      props.typeDefinition = normalizeRelClassNames(typeDefinition);
  }

  // geometry comes back under a different name/format than ElementProps.geom;
  // surfaced separately (raw flatbuffer, not GeometryStreamProps)
  const geometryStream = take<unknown>("geometryStream");
  // GeometryPart bounding box (custom-handled alongside its geometry)
  const partBBoxLow = take<object>("bBoxLow");
  const partBBoxHigh = take<object>("bBoxHigh");
  if (partBBoxLow !== undefined && partBBoxHigh !== undefined)
    props.bbox = { low: partBBoxLow, high: partBBoxHigh };

  // present in `$` output but not part of ElementProps
  consumed.add("lastMod");

  // remaining auto-handled properties pass through unchanged (modulo
  // relClassName separator normalization inside nav-prop values)
  for (const [key, value] of Object.entries(row)) {
    if (consumed.has(key)) continue;
    props[key] = normalizeRelClassNames(value);
  }

  return { props: props as unknown as ElementProps, geometryStream };
}

export const selectAllElementInstancesECSql =
  "SELECT $ FROM bis.Element OPTIONS USE_JS_PROP_NAMES DO_NOT_TRUNCATE_BLOB";

export async function queryAllInstanceRows(
  db: IModelDb
): Promise<Map<Id64String, Record<string, unknown>>> {
  const reader = db.createQueryReader(
    selectAllElementInstancesECSql,
    undefined,
    { usePrimaryConn: true }
  );
  const rows = new Map<Id64String, Record<string, unknown>>();
  for await (const rowProxy of ensureECSqlReaderIsAsyncIterableIterator(
    reader
  )) {
    const row = parseInstanceRow(rowProxy[0]);
    assert(typeof row.id === "string");
    rows.set(row.id, row);
  }
  return rows;
}
