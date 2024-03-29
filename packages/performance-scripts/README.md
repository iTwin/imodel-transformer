# (iTwin Transformer) performance-scripts

To use this package, you should require it before anything else. One easy way to do that is,
to set the `NODE_OPTIONS` environment variable like so:

```sh
NODE_OPTIONS='--require performance-scripts'
```

Then run your program. There are other required options but they are explained when you don't
supply them, so for the full list of options, please run:

```sh
npm install -g @itwin/performance-scripts
NODE_OPTIONS='-r @itwin/performance-scripts' node
```

This package allows you to hook into function calls with a few different supported profilers, listed
by not supplying the `PROFILE_TYPE` environment variable.

## Reading profiles

We recommend reading profiles with [https://speedscope.app](https://speedscope.app) since it can
handle both Linux native perf profiles and V8 JavaScript CPU profiles. Sqlite profiles are themselves
sqlite databases and as such must be opened in a sqlite database explorer.

If you don't like the idea of using a hosted third-party website, there is a fully local version of
speedscope that you can install, e.g. through `npx speedscope my-profile.cpuprofile`. You can also use
profiler-specific viewers like the chrome devtools profiler viewer.

## bin/

The bin folder contains short scripts for generic performance statistic gathering. They are exposed
as scripts but may be platform specific when the name contains a platform.

## Potential profilers worth adding

- Valgrind's callgrind
- Windows profilers

## Caveats

### direct references within a module

Due to limitations of JavaScript and not using some horrible thing like Babel, the injection will
not work if the hooked functions are being used directly within a module. That means:

```js
//script.js
export function a() {

}

export function b() {
  return a();
}
```

The call to `a` in `b` will not be profiled even with `FUNCTIONS='require("./script.js").a'` because `b` will
look up the name `a` local to the module. You can do the following if you really really want to profile that case:

```js
//script.js
export function a() {

}

export function b() {
  return module.exports.a();
}
```

But you should probably at that point just import `runWithJsCpuProfiler` (or whatever profiler type you want)
and manually wrap the profile code.

In practice, we're usually trying to profile at the level of a consumed export where this is usually
not the case. But you can always bail out to importing the `runWith*` functions if you need them.

