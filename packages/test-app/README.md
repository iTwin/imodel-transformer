# imodel-transformer

## About this Application

The application contained within this directory is a test application for transforming iModels.

Example command line:

`npm start -- --sourceFile <sourceIModelFileName> --targetFile <targetIModelFileName>`

The target file must be a `StandaloneDb`, you can use the utility script `test-apps/display-test-app/src/backend/SetToStandalone.ts`
to turn a snapshot into a `StandaloneDb` for testing purposes.
You can also perform a transform onto an empty target by specifying `--targetDestination <path>` instead.

To get usage help run:

`npm start -- --help`

## Profiling `transformer.process()`

Use the `@bentley/hook-profiler` preload to profile transformation calls without
changing the app's execution path. `PROFILE_TYPE=pause` pauses immediately
before and after `transformer.process()` and prints the process ID for an
external profiler:

```powershell
pnpm --filter @bentley/hook-profiler build
pnpm --filter test-app build
$env:NODE_OPTIONS = "--require @bentley/hook-profiler"
$env:PROFILE_TYPE = "pause"
$env:FUNCTIONS = 'require("@itwin/imodel-transformer").IModelTransformer.prototype.process'
pnpm --filter test-app start -- `
  --sourceSnapshot C:\iModels\source.bim `
  --targetDestination C:\iModels\target.bim
```

Set `PROFILE_TYPE=js-cpu` instead to capture a V8 `.js.cpuprofile`
automatically. Set `FUNCTIONS` to
`IModelTransformer.prototype.processElement` when profiling isolated-element
runs. Pause mode requires an interactive terminal. Profiling changes execution
characteristics, so use a separate unprofiled run for benchmark timings.

If you want to connect to an online iModel (e.g. using the `--sourceIModelId` argument),
you will need to setup a .env file, see the `.env.template` file for an example and link to a setup guide.

## imodel-transformer as a sample application

This application demonstrates the following:

- Using the `IModelTransformer` API
- Calling the `ITwinRegistry` API to get _context_ (project or asset) information
- Querying `IModelHub` to get iModel information
- How to handle either _briefcase_ or _snapshot_ iModel in the same application
- Using [yargs](http://yargs.js.org/) to handle command line arguments

## imodel-transformer as a support tool

In addition to being an example of how to use the `IModelTransformer` API within an application, `imodel-transformer` is also a useful support tool.
It can be used to investigate issues reported against the `IModelTransformer` API.
A common scenario is a generic report (with minimal details) of IModelTransformer throwing an Error before running to successful completion.
Here are the steps that can be used:

- Ask to be invited to the project/asset context that contains the iModel. This is straightforward in _QA_ and _DEV_ but may require more legwork for a user iModel in _PROD_.
- After being invited, your name should show up in the "Team Members" list. If it does not, you may not have the required permissions to pull a briefcase of the iModel.
- Get the GUID of the iTwinId and the GUID of the iModelId. Both should be available in the URL used by Design Review to view the iModel.
- Optionally, you can turn on verbose iModel transformation-related logging with the `--logTransformer` (or `-v`) option.
- Set the appropriate options either on the command line or by editing the `imodel-transformer (app)` launch configuration within `launch.json` (in the root imodeljs directory)
  - `--hub qa` (or 'dev' or 'prod')
  - `--logTransformer`
  - `--sourceITwinId <iTwin GUID>`
  - `--sourceIModelId <iModel GUID>`
  - `--targetDestination <full path to file on local computer>`

A common strategy is to run with verbose logging on to find the problem element or spot where the problem occurs.
Once the problem area has been identified, you can employ various strategies to set a conditional breakpoint.
One possibility is to edit the `onTransformElement` method in `Transformer.ts` to add a `if (sourceElement.getDisplayLabel() === "x")` or `if (sourceElement.id === "x")` conditional (using information from the log output) around a "hit problem area" log function call and then set a breakpoint on that log message.
After rebuilding, re-running, and hitting the breakpoint, you can then step into the core IModelTransformer methods to see what is really going on.

### isolating bad elements

You can pass a comma-separated list of element ids to the `--isolateElements` (such as `id1,id2`) to transform to the specified target
a filtered iModel containing only the path to the given elements/models specified by the given ids. This can be
useful to create smaller reproduction iModels with less data that still contain problematic elements. You can also isolate the entire subtrees
of these elements by using `--isolateTrees` instead of `--isolateElements`.
