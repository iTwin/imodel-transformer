#!/usr/bin/env bash

clear
pnpm build
rm -f /tmp/out.bim

if [[ ! -e /tmp/after-step1.bim ]]; then
  echo create standalone up to changeset 7
  node lib/Main.js --hub qa -v \
    --sourceITwinId 99d0fc37-34c2-4fbd-bbb8-e8bdfdb647e5 \
    --sourceIModelId 4969e80e-3578-4e3e-9527-344ca804b0c2 \
    --targetITwinId a098abac-7c33-4810-a613-eabc3c9c324a \
    --targetIModelId 15b0210a-fea5-4215-b3d0-87b58671a823 \
    --targetStandaloneDestination /tmp/out.bim \
    --sourceStartChangesetIndex 1 \
    --sourceEndChangesetIndex 7 | sed 's/^/[..7] /g'

  if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
    cp /tmp/out.bim /tmp/after-step1.bim
  fi
fi

echo processChanges in standalone from 8-end
FORCE_BRIEFCASE_NAME=/tmp/briefcase2.bim node --inspect-brk lib/Main.js --hub qa -v \
  --sourceITwinId 99d0fc37-34c2-4fbd-bbb8-e8bdfdb647e5 \
  --sourceIModelId 4969e80e-3578-4e3e-9527-344ca804b0c2 \
  --targetFile /tmp/after-step1.bim \
  --sourceStartChangesetIndex 8 | sed 's/^/[8..] /g'

