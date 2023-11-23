#!/usr/bin/bash

NODE='/home/mike/.local/share/pnpm/node'

case="selectfrom-$(git rev-parse --short HEAD)"

for file in /home/mike/work/bad-aspect-old.bim /home/mike/work/Juergen.Hofer.Bad.Normals.bim /home/mike/work/shell-noobstruction.bim
do
  rm -f /tmp/out.bim
  filebase="${case}_$(basename $file)"
  echo $filebase
  strace -c $NODE -r source-map-support/register ../test-app/lib/Main.js --sourceFile $file --targetDestination /tmp/out.bim --noProvenance --danglingReferencesBehavior ignore -F |& tee $filebase.strace;
  /usr/bin/time -v $NODE -r source-map-support/register ../test-app/lib/Main.js --sourceFile $file --targetDestination /tmp/out.bim --noProvenance --danglingReferencesBehavior ignore -F |& tee $filebase.usrtime;
  $NODE --cpu-prof --cpu-prof-name=$filebase.js.cpuprofile --cpu-prof-interval=10000 -r source-map-support/register ../test-app/lib/Main.js --sourceFile $file --targetDestination /tmp/out.bim --noProvenance --danglingReferencesBehavior ignore -F;
  sudo bash -c "perf record -F 50 -g $NODE --perf-basic-prof --interpreted-frames-native-stack -r source-map-support/register ../test-app/lib/Main.js   --sourceFile $file --targetDestination /tmp/out.bim --noProvenance --danglingReferencesBehavior ignore -F && chmod +r perf.data && mv perf.data $filebase.perf.cpuprofile";
  rm -f /tmp/out.bim
  chown mike $filebase*
  chmod +r $filebase*
done
