
# (iTwin Transformer) performance-scripts

To use this package, you should require it before anything else. One easy way to do that is

Set the `NODE_OPTIONS` environment variable like so:

```sh
NODE_OPTIONS='--require performance-scripts'
```

Then run your program.
You must also set in the environment the 'PROFILE_TYPE' variable.

This package will hook into calls to `processAll`, `processChanges`, and `processSchemas`
and generate profiles for them depending on which kind `PROFILE_TYPE` you have selected.

Run without setting `PROFILE_TYPE` for a list of valid profile types.

