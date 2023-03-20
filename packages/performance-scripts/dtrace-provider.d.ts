
// TODO: contribute to @types/dtrace-provider

declare module "dtrace-provider" {
  export type CType = "int" | "char *"

  type CTypeToJsType<T extends CType>
    = T extends "int" ? number
    : T extends "char *" ? string
    : never;

  type CTypeArgsToJsTypeArgs<T extends readonly CType[]>
    = { [K in keyof T]: CTypeToJsType<K> } & { length: T["length"] }

  export interface Probe<CTypes extends readonly CType[]> {
    fire<ExtraArgs extends readonly any[]>(
      callback: () => [...CTypeArgsToJsTypeArgs<CTypes>, ...ExtraArgs],
      ...additionalArgs: ExtraArgs
    );
  }

  export interface DTraceProvider {
    addProbe<T extends readonly CType[]>(name: string, ...ctypes: T): Probe<T>;
    //fire(probeName: string, callback: () => any[]);
  }

  export function createDTraceProvider(name: string): DTraceProvider;
}
