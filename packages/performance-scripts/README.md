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

We recommend reading profiles either with chrome devtools, or [https://speedscope.app](https://speedscope.app).
If you don't like the idea of using a hosted thirdparty website, there is a fully local version you can
install, e.g. through `npx speedscope my-profile.cpuprofile`.

