#!/usr/bin/env bash
# you might need to run as super user to have permissions to probe the process

if [[ "$(id -u)" != "0" ]]; then
  echo "Warning, you are not root. perf will probably fail."
fi

# default options
HERTZ="${HERTZ:=999}"
# default wait 1000ms to skip initialization of dependencies, it does not seem to be possible to wait
# for a custom event even with dtrace-provider, so just doing this for now
WAIT_STARTUP_MS="${WAIT_STARTUP_MS:=0}"
OUTPUT_FILE="${OUTPUT_FILE:=profile_$(date -Iseconds).cpp.cpuprofile}"

if [[ -z "$@" ]]; then
  echo "Usage: [HERTZ=999] [WAIT_STARTUP_MS=0] [OUTPUT_FILE=$OUTPUT_FILE] $0 [ARGS_FOR_NODE]"
  echo "Example: WAIT_STARTUP_MS=1000 $0 myscript.js --scriptArg1"
fi

perf record -F $HERTZ -g -D $WAIT_STARTUP_MS \
  node --perf-prof --interpreted-frames-native-stack $@
perf script > $OUTPUT_FILE
rm -f perf.data
chmod 666 $OUTPUT_FILE

echo "pnpx speedscope $OUTPUT_FILE; # run to view"

rm -f *-v8.log

