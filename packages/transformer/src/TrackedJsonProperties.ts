import {
    DisplayStyle3d,
  Element,
  Entity,
  GeometricElement,
  SpatialViewDefinition,
  Subject,
  ViewDefinition,
} from "@itwin/core-backend";
import * as BackendExports from "@itwin/core-backend";
import { Id64Arg, isSubclassOf } from "@itwin/core-bentley";

type TrackedJsonPropData = Record<string, { get(e: Element): Id64Arg }>;

/** @internal */
const classSpecificTrackedJsonProperties = new Map<
  abstract new (...a: any[]) => Entity,
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
    "styles.subCategoryOvr.[*].subCategory": { get: (e: DisplayStyle3d) => e.jsonProperties.styles.subCategoryOvr.map((ovr: any) => ovr.subCategory) },
  }],
  [ViewDefinition as any, {
    "viewDetails.acs": { get: (e: SpatialViewDefinition) => e.jsonProperties.viewDetails.acs },
  }],
]);


/** inherited reference keys for each bis core class */
const trackedJsonProperties = new Map<abstract new (...args: any[]) => Entity, TrackedJsonPropData>([]);

export const TrackedJsonProperties = {
  get(entityClass: abstract new (...args: any[]) => Entity): TrackedJsonPropData {
    const cached = trackedJsonProperties.get(entityClass);
    if (cached) return cached;
    return cacheTrackedJsonProperties(entityClass as typeof Entity);
  }
};

// NOTE: this currently ignores the ability that core's requiredReferenceKeys has to allow custom js classes
// to introduce new requiredReferenceKeys, which means if that were used, this would break. Currently un used however.
function cacheTrackedJsonProperties(cls: typeof Entity) {
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

