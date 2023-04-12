import * as assert from "assert";

import {
  DisplayStyle,
  DisplayStyle3d,
  Element,
  SpatialViewDefinition,
  Subject,
  ViewDefinition,
} from "@itwin/core-backend";
import * as BackendExports from "@itwin/core-backend";
import { CompressedId64Set, Id64String, isSubclassOf } from "@itwin/core-bentley";
import { Id64Utils, Id64UtilsArg } from "./Id64Utils";
import { IModelCloneContext } from "./IModelCloneContext";
import { DisplayStyle3dProps, DisplayStyleProps, ElementProps, SpatialViewDefinitionProps } from "@itwin/core-common";

interface TrackedJsonPropData<T extends Id64UtilsArg> {
  [propPath: string]: {
    get(e: ElementProps): T | undefined;
    remap(e: ElementProps, remap: (prev: T) => T): void;
  }
}

/**
 * non-inherited tracked json properties introduced by a class.
 * This is used to generate the inherited list.
 * @internal
 */
const classSpecificTrackedJsonProperties = new Map<
  abstract new (...a: any[]) => Element,
  TrackedJsonPropData<Id64UtilsArg>
>([
  [Element, {
    "targetRelInstanceId": {
      get: (e: ElementProps) => e.jsonProperties.targetRelInstanceId,
      remap: (e: ElementProps, remap) => {
        if (e.jsonProperties.targetRelInstanceId)
          e.jsonProperties.targetRelInstanceId = remap(e.jsonProperties.targetRelInstanceId);
      },
    },
  }],
  [DisplayStyle3d, {
    "styles.environment.sky.image.texture": {
      get: (e: DisplayStyle3dProps) => e.jsonProperties?.styles?.environment?.sky?.image?.texture,
      remap: (e: DisplayStyle3dProps, remap) => {
        if (e.jsonProperties?.styles?.environment?.sky?.image?.texture)
          e.jsonProperties.styles.environment.sky.image.texture = remap(e.jsonProperties.styles.environment.sky.image.texture) as Id64String;
      },
    },
      // FIXME: this is a compressed id set!
    "styles.excludedElements": {
      get: (e: DisplayStyle3dProps) => e.jsonProperties?.styles?.excludedElements,
      remap: (e: DisplayStyle3dProps, remap) => {
        if (e.jsonProperties?.styles?.excludedElements) {
          if (typeof e.jsonProperties.styles.excludedElements === "string") {
            const decompressed = CompressedId64Set.decompressArray(e.jsonProperties.styles.excludedElements);
            e.jsonProperties.styles.excludedElements = CompressedId64Set.compressArray(decompressed.map(remap) as Id64String[]);
          } else /** Array.isArray */ {
            e.jsonProperties.styles.excludedElements.forEach((id, i, a) => a[i] = remap(id) as Id64String)
          }
        }
      },
    },
  }],
  [DisplayStyle as any, {
    "styles.subCategoryOvr.*.subCategory": {
      get: (e: DisplayStyleProps) =>
        e.jsonProperties?.styles?.subCategoryOvr?.map((ovr: any) => ovr?.subCategory),
      remap: (e: DisplayStyleProps, remap) => {
        if (e.jsonProperties?.styles?.subCategoryOvr)
          (e.jsonProperties.styles.subCategoryOvr as { subCategory: Id64String }[])
            .forEach((ovr) => ovr.subCategory && (ovr.subCategory = remap(ovr.subCategory) as Id64String));
      },
    },
    "styles.modelOvr.*.modelId": {
      get: (e: DisplayStyleProps) =>
        e.jsonProperties?.styles?.modelOvr?.map((ovr: any) => ovr.modelId),
      remap: (e: DisplayStyleProps, remap) => {
        if (e.jsonProperties?.styles?.modelOvr)
          (e.jsonProperties?.styles?.modelOvr as { modelId: Id64String }[] | undefined)
            ?.forEach((ovr) => ovr.modelId && (ovr.modelId = remap(ovr.modelId) as Id64String));
      },
    },
  }],
  [ViewDefinition as any, {
    "viewDetails.acs": {
      get: (e: SpatialViewDefinitionProps) => e.jsonProperties?.viewDetails?.acs,
      remap: (e: SpatialViewDefinitionProps, remap) => {
        if (e.jsonProperties?.viewDetails?.acs)
          e.jsonProperties.viewDetails.acs = remap(e.jsonProperties.viewDetails.acs) as Id64String;
      },
    },
  }],
]);

/** inherited tracked json properties for every class */
const trackedJsonProperties = new Map<abstract new (...args: any[]) => Element, TrackedJsonPropData<Id64UtilsArg>>([]);

/** @internal */
export interface PotentialUntrackedJsonPropValue {
  /** path from the element */
  propPath: string;
  /** path including info about the element where it was found, for debug printing */
  debugPath?: string;
  value: Id64String | CompressedId64Set
  type: "Id64String" | "CompressedId64Set";
}

export const TrackedJsonProperties = {
  get(entityClass: abstract new (...args: any[]) => Element): TrackedJsonPropData<Id64UtilsArg> {
    const cached = trackedJsonProperties.get(entityClass);
    if (cached) return cached;
    return cacheTrackedJsonProperties(entityClass as typeof Element);
  },

  _remapJsonProps(sourceElement: ElementProps, findTargetElementId: IModelCloneContext["findTargetElementId"]) {
    const elemClassTrackedJsonProps = cacheTrackedJsonProperties(sourceElement.constructor as typeof Element);
    for (const trackedJsonProp of Object.values(elemClassTrackedJsonProps)) {
      // TODO: support CompressedId64Set
      trackedJsonProp.remap(sourceElement, findTargetElementId as (id: Id64UtilsArg) => Id64UtilsArg);
    }
  },

  /** @internal */
  _hexIdOrCompressedSetPattern: /^((?<hex>0x)|(?<compressed>\+\d))/,

  /** @internal */
  _findPotentialUntrackedJsonProperties(element: Element): PotentialUntrackedJsonPropValue[] {
    const results: PotentialUntrackedJsonPropValue[] = [];

    const recurse = (obj: any, path: string) => {
      outer: for (const [key, val] of Object.entries(obj)) {
        let match: RegExpMatchArray | null;
        if (typeof val === "string" && (match = this._hexIdOrCompressedSetPattern.exec(val))) {
          const subPath = `${path}.${key}`;
          for (const [cls, trackedProps] of classSpecificTrackedJsonProperties.entries()) {
            if (isSubclassOf(element.constructor as typeof Element, cls as typeof Element)) {
              const trackedKey = subPath.slice("jsonProperties.".length);
              if (trackedKey in trackedProps) continue outer;
            }
          }
          const debugPath = `<${element.id}|"${element.code.value}"(${element.classFullName})>.${subPath}`;
          results.push({
            debugPath,
            propPath: subPath,
            value: val,
            type: match.groups?.hex ? "Id64String" : "CompressedId64Set",
          });
        } else if (typeof val === "object") {
          recurse(val, `${path}.${key}`);
        }
      }
    };

    recurse(element.jsonProperties, "jsonProperties");

    return results;
  }
};

// NOTE: this currently ignores the ability that core's requiredReferenceKeys has to allow custom js classes
// to introduce new requiredReferenceKeys, which means if that were used, this would break. Currently un-used however.
function cacheTrackedJsonProperties(cls: typeof Element): TrackedJsonPropData<Id64UtilsArg> {
  const classTrackedJsonProps: TrackedJsonPropData<Id64UtilsArg> = {};

  let baseClass = cls;
  while (baseClass !== null) {
    const baseClassRequiredRefs = classSpecificTrackedJsonProperties.get(baseClass);
    Object.assign(classTrackedJsonProps, baseClassRequiredRefs);
    baseClass = Object.getPrototypeOf(baseClass);
  }

  trackedJsonProperties.set(cls, classTrackedJsonProps);

  return classTrackedJsonProps;
}

const bisCoreClasses = Object.values(BackendExports).filter(
  (v): v is typeof Element => v && "prototype" in v && isSubclassOf(v as any, Element)
);

for (const bisCoreClass of bisCoreClasses) {
  cacheTrackedJsonProperties(bisCoreClass)
}

