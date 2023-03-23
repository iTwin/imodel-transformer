
import { IModelImporter, IModelImportOptions } from "./IModelImporter";
import * as child_process from "child_process";
import { IModelDb } from "@itwin/core-backend";

enum Messages {
  SetOption,
  CallMethod
}

type Message =
  | {
      type: Messages.SetOption;
      key: string;
      value: any;
    }
  | {
      type: Messages.CallMethod;
      args: any;
    }
;

export interface MultiProcessImporterOptions extends IModelImportOptions {
  // TODO: implement
  /** the path to a module with a default export of an IModelImporter class to load */
  importerClassModulePath: string;
}

export class MultiProcessIModelImporter extends IModelImporter {
  private _worker: child_process.ChildProcess;

  public constructor(targetDb: IModelDb, options: MultiProcessImporterOptions) {
    super(targetDb, options);

    targetDb.close(); // close it, the spawned process will need the write lock

    this._worker = child_process.fork(require.resolve("./MultiProcessEntry"), {
      serialization: "advanced", // allow transferring of binary geometry efficiently
    });

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

    const forwarded = [
      "importModel",
      "importElement",
      "importRelationship",
      "importElementMultiAspects",
      "importElementUniqueAspect",
      "deleteElement",
      "deleteModel",
    ] as const;

    for (const key of forwarded) {
      Object.defineProperty(this, key, {
        value: (...args: Parameters<IModelImporter[typeof key]>): void => {
          this._worker.send({
            type: Messages.CallMethod,
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

