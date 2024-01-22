#!/usr/bin/env bash
# upgrade all itwin dependencies
pnpm dlx pnpm@7 -r update $( git ls-files '*package.json' | xargs -i jq '.dependencies|keys[]' {} | grep itwin | sed 's/"//g')
git stash push '*package.json'
# reinstall old package.json on new updated dev deps
pnpm dlx pnpm@7 install
