#!/usr/bin/env bash
# you might need to run as super user to have permissions to probe the process

INPUT_FILES=($@)
CASES=(--getNativeDbOnly --insertAspect)

HERTZ=999

# TODO: not sure it's possible on default kernel config but dtrace could make us start profiling after a custom userspace event
WAIT_STARTUP_MS=1000

function cpu_profile_cpp() {
  # wait 1000ms to skip initialization of dependencies
  perf record -F $HERTZ -g -D $WAIT_STARTUP_MS \
    node --perf-basic-prof --interpreted-frames-native-stack $(dirname $0)/nativedb-check.js $2 $1
  perf script > $3
  rm -f perf.data
  chmod 666 $3
}

function cpu_profile_js() {
  node --cpu-prof --cpu-prof-name $3 $(dirname $0)/nativedb-check.js $2 $1
  chmod 666 $3
}

for INPUT_FILE in ${INPUT_FILES[@]}; do
  for CASE in ${CASES[@]}; do
    for TYPE in cpp js; do
      OUTPUT_FILE="${INPUT_FILE}_$CASE.$TYPE.cpuprofile"
      CMD="cpu_profile_$TYPE"
      $CMD $INPUT_FILE $CASE $OUTPUT_FILE
      echo "pnpx speedscope $OUTPUT_FILE; # run to view"
    done
  done
done

rm -f *-v8.log

