import { Element, ClassRegistry, IModelDb, } from "@itwin/core-backend";
import * as BackendExports from "@itwin/core-backend";
import { CompressedId64Set, Id64String, isSubclassOf } from "@itwin/core-bentley";
import { Id64UtilsArg } from "./Id64Utils";
import { IModelCloneContext } from "./IModelCloneContext";
import {
  DisplayStyle3dProps,
  DisplayStyleProps,
  ElementProps,
  SectionDrawingProps,
  SpatialViewDefinitionProps,
  RenderMaterialProps,
  TextureMapProps
} from "@itwin/core-common";

interface TrackedJsonPropData<T extends Id64UtilsArg> {
  /**
   * `propPathPattern` is in 'simple property wildcard' syntax, which means a dot
   * separated list of strings or '*' which matches any string for that property access.
   * e.g. 'hello.*.world.*.test' matches 'hello.0.world.0-was-an-array-ICanContain$$Whatever.test'
   * @see simplePropertyWildcardMatch
   */
  [propPathPattern: string]: {
    get(e: ElementProps): T | undefined;
    remap(e: ElementProps, remap: (prev: T) => T): void;
  }
}

// TODO: document where some of these render material maps come from, propose adding them to core
// FIXME: check native code for other map types
declare module "@itwin/core-common" {
  export interface RenderMaterialAssetMapsProps {
    Bump?: TextureMapProps;
    Diffuse?: TextureMapProps;
    Finish?: TextureMapProps;
    GlowColor?: TextureMapProps;
    Reflect?: TextureMapProps;
    Specular?: TextureMapProps;
    TranslucencyColor?: TextureMapProps;
    TransparentColor?: TextureMapProps;
    Displacement?: TextureMapProps;
  }
}

/**
 * non-inherited tracked json properties introduced by a class.
 * This is used to generate the inherited list.
 * @internal
 */
const classSpecificTrackedJsonProperties = new Map<
  string,
  TrackedJsonPropData<Id64UtilsArg>
>([
  // cannot use Element.classFullName because IModelHost is not necessarily initialized at import time
  ["BisCore:Element", {
    "targetRelInstanceId": {
      get: (e: ElementProps) => e.jsonProperties?.targetRelInstanceId,
      remap: (e: ElementProps, remap) => {
        if (e.jsonProperties?.targetRelInstanceId)
          e.jsonProperties.targetRelInstanceId = remap(e.jsonProperties.targetRelInstanceId);
      },
    },
  }],
  ["BisCore:DisplayStyle3d", {
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
  ["BisCore:DisplayStyle", {
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
  ["BisCore:ViewDefinition", {
    "viewDetails.acs": {
      get: (e: SpatialViewDefinitionProps) => e.jsonProperties?.viewDetails?.acs,
      remap: (e: SpatialViewDefinitionProps, remap) => {
        if (e.jsonProperties?.viewDetails?.acs)
          e.jsonProperties.viewDetails.acs = remap(e.jsonProperties.viewDetails.acs) as Id64String;
      },
    },
  }],
  ["BisCore:SectionLocation", {
    // TODO: document where these json props come from
    "spatialViewId": {
      get: (e: SectionDrawingProps) => (e.jsonProperties as any)?.spatialViewId,
      remap: (e: SpatialViewDefinitionProps, remap) => {
        if ((e.jsonProperties as any)?.spatialViewId)
          (e.jsonProperties as any).spatialViewId = remap((e.jsonProperties as any).spatialViewId) as Id64String;
      },
    },
    "drawingViewId": {
      get: (e: SectionDrawingProps) => (e.jsonProperties as any)?.drawingViewId,
      remap: (e: SpatialViewDefinitionProps, remap) => {
        if ((e.jsonProperties as any)?.drawingViewId)
          (e.jsonProperties as any).drawingViewId = remap((e.jsonProperties as any).drawingViewId) as Id64String;
      },
    },
  }],
  ["BisCore:RenderMaterial", {
    // TODO: test random configured tracked props
    "materialAssets.renderMaterial.Map.Bump.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Bump?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Bump?.TextureId)
          e.jsonProperties.materialAssets.renderMaterial.Map.Bump.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.Bump.TextureId) as Id64String;
      }
    },
    // FIXME: this won't work if the json was already remapped in native code! Looks like I will need to rebase
    // on the non-native-context branch
    "materialAssets.renderMaterial.Map.Normal.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Normal?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Normal?.TextureId)
          e.jsonProperties.materialAssets.renderMaterial.Map.Normal.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.Normal.TextureId) as Id64String;
      }
    },
    "materialAssets.renderMaterial.Map.Diffuse.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Diffuse?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Diffuse?.TextureId) {
          e.jsonProperties.materialAssets.renderMaterial.Map.Diffuse.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.Diffuse.TextureId) as Id64String;
        }
      }
    },
    "materialAssets.renderMaterial.Map.Finish.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Finish?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Finish?.TextureId) {
          e.jsonProperties.materialAssets.renderMaterial.Map.Finish.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.Finish.TextureId) as Id64String;
        }
      }
    },
    "materialAssets.renderMaterial.Map.GlowColor.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.GlowColor?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.GlowColor?.TextureId) {
          e.jsonProperties.materialAssets.renderMaterial.Map.GlowColor.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.GlowColor.TextureId) as Id64String;
        }
      }
    },
    "materialAssets.renderMaterial.Map.Pattern.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Pattern?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Pattern?.TextureId) {
          e.jsonProperties.materialAssets.renderMaterial.Map.Pattern.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.Pattern.TextureId) as Id64String;
        }
      }
    },
    "materialAssets.renderMaterial.Map.Reflect.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Reflect?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Reflect?.TextureId) {
          e.jsonProperties.materialAssets.renderMaterial.Map.Reflect.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.Reflect.TextureId) as Id64String;
        }
      }
    },
    "materialAssets.renderMaterial.Map.Specular.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Specular?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Specular?.TextureId) {
          e.jsonProperties.materialAssets.renderMaterial.Map.Specular.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.Specular.TextureId) as Id64String;
        }
      }
    },
    "materialAssets.renderMaterial.Map.TranslucencyColor.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.TranslucencyColor?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.TranslucencyColor?.TextureId) {
          e.jsonProperties.materialAssets.renderMaterial.Map.TranslucencyColor.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.TranslucencyColor.TextureId) as Id64String;
        }
      }
    },
    "materialAssets.renderMaterial.Map.TransparentColor.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.TransparentColor?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.TransparentColor?.TextureId) {
          e.jsonProperties.materialAssets.renderMaterial.Map.TransparentColor.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.TransparentColor.TextureId) as Id64String;
        }
      }
    },
    "materialAssets.renderMaterial.Map.Displacement.TextureId": {
      get: (e: RenderMaterialProps) => e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Displacement?.TextureId,
      remap: (e: RenderMaterialProps, remap) => {
        if (e.jsonProperties?.materialAssets?.renderMaterial?.Map?.Displacement?.TextureId)
          e.jsonProperties.materialAssets.renderMaterial.Map.Displacement.TextureId
            = remap(e.jsonProperties.materialAssets.renderMaterial.Map.Displacement.TextureId) as Id64String;
      }
    },
  }],
]);

/**
 * Returns if a 'simple-property-syntax' pattern matches an input string
 * `propPathPattern` is in 'simple property wildcard' syntax, which means a dot
 * separated list of strings or '*' which matches any string for that property access.
 * e.g. 'hello.*.world.*Test' matches 'hello.0.world.0-was-an-array-ICanContain$$Whatever-butMustEndInTest'
 */
function simplePropertyWildcardMatch(pattern: string, s: string) {
  const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const wildcarded = escaped.replace(/\*/g, "[^.]*");
  const regex = new RegExp("^" + wildcarded + "$", "g");
  return regex.test(s);
}

/** @internal */
export interface PotentialUntrackedJsonPropValue {
  /** path from the element, e.g. jsonProperties.x.y.0.z */
  propPath: string;
  /** path including info about the element where it was found, for debug printing */
  debugPath?: string;
  value: Id64String | CompressedId64Set
  type: "Id64String" | "CompressedId64Set";
}

const bisCoreClasses = Object.values(BackendExports).filter(
  (v): v is typeof Element => v && "prototype" in v && isSubclassOf(v as any, Element)
);

export class TrackedJsonProperties {
  /** inherited tracked json properties for every class */
  private _trackedJsonProperties = new Map<string, TrackedJsonPropData<Id64UtilsArg>>([]);

  public constructor(
    private _iModel: IModelDb,
  ) {
    // NOTE: this currently ignores the ability that core's requiredReferenceKeys has to allow custom js classes
    // to introduce new requiredReferenceKeys, which means if that were used, this would break. Currently un-used however.
    for (const bisCoreClass of bisCoreClasses) {
      this._cacheTrackedJsonProperties(bisCoreClass.classFullName)
    }
  }

  private _cacheTrackedJsonProperties(classFullName: string): TrackedJsonPropData<Id64UtilsArg> {
    const classTrackedJsonProps: TrackedJsonPropData<Id64UtilsArg> = {};

    let currClass = this._iModel.getJsClass(classFullName);
    while (currClass !== null) {
      if (currClass.schema === undefined)
        break;
      const baseClassRequiredRefs = classSpecificTrackedJsonProperties.get(currClass.classFullName);
      Object.assign(classTrackedJsonProps, baseClassRequiredRefs);
      currClass = Object.getPrototypeOf(currClass);
    }

    this._trackedJsonProperties.set(classFullName, classTrackedJsonProps);

    return classTrackedJsonProps;
  }

  get(classFullName: string): TrackedJsonPropData<Id64UtilsArg> {
    const cached = this._trackedJsonProperties.get(classFullName);
    if (cached) return cached;
    return this._cacheTrackedJsonProperties(classFullName);
  }

  /** @internal */
  public _remapJsonProps(sourceElement: ElementProps, findTargetElementId: (id: Id64String, path: string) => Id64String) {
    const elemClassTrackedJsonProps = this.get(sourceElement.classFullName);
    for (const [propPath, propData] of Object.entries(elemClassTrackedJsonProps)) {
      // FIXME: support remapping CompressedId64Set
      propData.remap(sourceElement, (id: Id64UtilsArg) => findTargetElementId(id as Id64String, propPath));
    }
  }

  private static _hexIdOrCompressedSetPattern = /^((?<hex>0x)|(?<compressed>\+\d))/;

  /** @internal */
  public _findPotentialUntrackedJsonProperties(element: Element): PotentialUntrackedJsonPropValue[] {
    const results: PotentialUntrackedJsonPropValue[] = [];

    const recurse = (obj: any, path: string) => {
      outer:
      for (const [key, val] of Object.entries(obj)) {
        let match: RegExpMatchArray | null;
        if (typeof val === "string" && (match = TrackedJsonProperties._hexIdOrCompressedSetPattern.exec(val))) {
          const subPath = `${path}.${key}`;
          for (const [classFullName, trackedPropsPatterns] of classSpecificTrackedJsonProperties.entries()) {
            const cls = element.iModel.getJsClass(classFullName) as typeof Element;
            if (isSubclassOf(element.constructor as typeof Element, cls)) {
              const trackedKey = subPath.slice("jsonProperties.".length); // tracked props patterns don't include the root jsonProperties key
              const tracked = Object.keys(trackedPropsPatterns).some((pat) =>
                simplePropertyWildcardMatch(pat, trackedKey));
              if (tracked) continue outer;
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
}

