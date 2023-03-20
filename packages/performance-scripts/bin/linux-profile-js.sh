#!/usr/bin/env bash
# you might need to run as super user to have permissions to probe the process

# default options
OUTPUT_FILE="${OUTPUT_FILE:=profile_$(date -Iseconds).js.cpuprofile}"

if [[ -z "$@" ]]; then
  echo "Usage: [OUTPUT_FILE=$OUTPUT_FILE] $0 [ARGS_FOR_NODE]"
  echo "Example: $0 myscript.js --scriptArg1"
fi

node --cpu-prof --cpu-prof-name $OUTPUT_FILE $@
chmod 666 $OUTPUT_FILE

echo "pnpx speedscope $OUTPUT_FILE; # run to view"

rm -f *-v8.log

