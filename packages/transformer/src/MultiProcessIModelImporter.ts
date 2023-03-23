
import { IModelImporter, IModelImportOptions } from "./IModelImporter";
import * as child_process from "child_process";
import { IModelDb } from "@itwin/core-backend";

export interface MultiProcessImporterOptions extends IModelImportOptions {
  // TODO: implement
  /** the path to a module with a default export of an IModelImporter class to load */
  importerClassModulePath: string;
}

/** @internal */
const forwardedMethods = [
  "importModel",
  "importElement",
  "importRelationship",
  "importElementMultiAspects",
  "importElementUniqueAspect",
  "deleteElement",
  "deleteModel",
] as const;

/** @internal */
export type ForwardedMethods = (typeof forwardedMethods)[number];

/** @internal */
export enum Messages {
  Init,
  SetOption,
  CallMethod,
  Finalize,
}

/** @internal */
export type Message =
  | {
      type: Messages.Init;
      importerInitOptions: MultiProcessImporterOptions;
    }
  | {
      type: Messages.SetOption;
      key: keyof IModelImporter["options"];
      value: any;
    }
  | {
      type: Messages.CallMethod;
      method: ForwardedMethods;
      args: any;
    }
  | {
      type: Messages.Finalize;
    }
;

export class MultiProcessIModelImporter extends IModelImporter {
  private _worker: child_process.ChildProcess;

  public constructor(targetDb: IModelDb, options: MultiProcessImporterOptions) {
    super(targetDb, options);

    const pathName = targetDb.pathName;
    targetDb.close(); // close it, the spawned process will need the write lock

    this._worker = child_process.fork(require.resolve("./MultiProcessEntry"),
      [pathName], 
      {
        serialization: "advanced", // allow transferring of binary geometry efficiently
      }
    );

    (this as { options: IModelImportOptions }).options = new Proxy(this.options, {
      set: (obj, key, val, recv) => {
        this._worker.send({
          type: Messages.SetOption,
          key: key,
          value: val,
        } as Message)
        return Reflect.set(obj, key, val, recv);
      }
    });

    for (const key of forwardedMethods) {
      Object.defineProperty(this, key, {
        value: (...args: Parameters<IModelImporter[typeof key]>): void => {
          this._worker.send({
            type: Messages.CallMethod,
            method: key,
            args,
          } as Message);
        },
        writable: false,
        enumerable: false,
        configurable: false,
      });
    }
  }
}

