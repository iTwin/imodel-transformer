import {
    DisplayStyle3d,
  Element,
  SpatialViewDefinition,
  Subject,
  ViewDefinition,
} from "@itwin/core-backend";
import * as BackendExports from "@itwin/core-backend";
import { CompressedId64Set, Id64Arg, Id64String, isSubclassOf } from "@itwin/core-bentley";

type TrackedJsonPropData = Record<string, { get(e: Element): Id64Arg }>;

/** @internal */
const classSpecificTrackedJsonProperties = new Map<
  abstract new (...a: any[]) => Element,
  TrackedJsonPropData
>([
  [Element, {
    "targetRelInstanceId": { get: (e: Element) => e.jsonProperties.targetRelInstanceId },
  }],
  [Subject, {
    "Subject.Job": { get: (e: Subject) => e.jsonProperties.Subject.Job }
  }],
  [DisplayStyle3d, {
    "styles.environment.sky.image.texture": { get: (e: DisplayStyle3d) => e.jsonProperties.styles.environment.sky.image.texture },
    "styles.excludedElements": { get: (e: DisplayStyle3d) => e.jsonProperties.styles.excludedElements },
    "styles.subCategoryOvr.*.subCategory": { get: (e: DisplayStyle3d) => e.jsonProperties.styles.subCategoryOvr.map((ovr: any) => ovr.subCategory) },
  }],
  [ViewDefinition as any, {
    "viewDetails.acs": { get: (e: SpatialViewDefinition) => e.jsonProperties.viewDetails.acs },
  }],
]);

/** inherited tracked json properties for every class */
const trackedJsonProperties = new Map<abstract new (...args: any[]) => Element, TrackedJsonPropData>([]);

/** @internal */
export interface PotentialUntrackedJsonPropValue {
  /** path from the element */
  propPath: string;
  /** path including info about the element where it was found, for debug printing */
  debugPath: string;
  value: Id64String | CompressedId64Set
  type: "Id64String" | "CompressedId64Set";
}

export const TrackedJsonProperties = {
  get(entityClass: abstract new (...args: any[]) => Element): TrackedJsonPropData {
    const cached = trackedJsonProperties.get(entityClass);
    if (cached) return cached;
    return cacheTrackedJsonProperties(entityClass as typeof Element);
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
          console.error(`${debugPath} contained id ${val} but that json property isn't known to the transformer`);
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
function cacheTrackedJsonProperties(cls: typeof Element) {
  const classTrackedJsonProps = {};

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

