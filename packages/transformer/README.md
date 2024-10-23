# @itwin/imodel-transformer

Copyright © Bentley Systems, Incorporated. All rights reserved. See LICENSE.md for license terms and full copyright notice.

## Description

The __@itwin/imodel-transformer__ package contains classes that handle traversing iModels for exporting and importing their parts.

## Documentation

See the [iTwin.js](https://www.itwinjs.org) documentation for more information.

## Versioning

This package, for the time being, relies on @internal APIs in iTwin.js, and therefore has very strict peerDependencies versions.
We perform a version check at runtime to ensure this. Every new iTwin.js version must be validated, and fixes are rarely ported
to old versions currently, you must request this in an issue. Removing Dependencies on internal APIs is ongoing.
You can find the latest @itwin/imodel-transformer version for your iTwin.js version by copy and pasting this into a bash shell

(use git bash on windows).

```sh
MY_ITWINJS_VERSION="3.6.0" # edit me
pnpm -s --package=semver -c dlx node <<EOF
json=""
require("https").get("https://registry.npmjs.org/@itwin/imodel-transformer", r=>r.setEncoding("utf8").on("data", d=>json+=d).on("end", ()=>{
  semver=require(process.env.PATH.split(":").find(x=>x.includes(".bin"))+"/../semver")
  console.log(Object.entries(JSON.parse(json).versions)
              .filter(([,v])=>semver.satisfies("$MY_ITWINJS_VERSION", v.peerDependencies["@itwin/core-backend"]))
              .map(([k,v])=>k).reverse())
}))
EOF
```

## Environment Variables

### TRANSFORMER_NO_STRICT_DEP_CHECK

Set this variable to `1` to disable strict dependency checks in `packages/transformer/src/transformer.ts`.

### SUGGEST_TRANSFORMER_VERSIONS

Set this variable to `1` to enable `packages/transformer/src/transformer.ts` to suggest compatible versions if dependencies are not in the specified peer dependency range of this package.

```dotenv
# Disable strict dependency checks in transformer.ts
TRANSFORMER_NO_STRICT_DEP_CHECK=1

# Enable version suggestions in transformer.ts
SUGGEST_TRANSFORMER_VERSIONS=1
