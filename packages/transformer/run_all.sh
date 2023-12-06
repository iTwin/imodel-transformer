#!/usr/bin/bash

NODE='/home/mike/.local/share/pnpm/node'

global_transform_args=(--noProvenance --danglingReferencesBehavior ignore)
global_node_args=("-r" source-map-support/register)

# change this to '"-F" ""' for both new and old slow transformer
# for transform_args in "-F";
for transform_args in "-F" "";
do
  for file in /home/mike/work/bad-aspect-old.bim /home/mike/work/Juergen.Hofer.Bad.Normals.bim /home/mike/work/shell-noobstruction.bim
  do
    rm -f /tmp/out.bim

    if [[ "$transform_args" = "-F" ]]
    then
      case="selectfrom-$(git rev-parse --short HEAD)"
    else
      case="oldtform"
    fi

    filebase="${case}_$(basename $file)"
    echo $filebase

    test -e $filebase.strace || strace -c $NODE ${global_node_args[@]} ../test-app/lib/Main.js --sourceFile $file --targetDestination /tmp/out.bim ${global_transform_args[@]} $transform_args |& tee $filebase.strace;
    test -e $filebase.usrtime || /usr/bin/time -v $NODE ${global_node_args[@]} ../test-app/lib/Main.js --sourceFile $file --targetDestination /tmp/out.bim ${global_transform_args[@]} $transform_args |& tee $filebase.usrtime;
    test -e $filebase.js.cpuprofile || $NODE --cpu-prof --cpu-prof-name=$filebase.js.cpuprofile --cpu-prof-interval=10000 ${global_node_args[@]} ../test-app/lib/Main.js --sourceFile $file --targetDestination /tmp/out.bim ${global_transform_args[@]} $transform_args
    # sudo bash -c "perf record -F 50 -g $NODE --perf-basic-prof --interpreted-frames-native-stack -r source-map-support/register ../test-app/lib/Main.js   --sourceFile $file --targetDestination /tmp/out.bim --noProvenance --danglingReferencesBehavior ignore -F && chmod +r perf.data && mv perf.data $filebase.perf.cpuprofile";
    test -e $filebase.heaptrack.zst || (
      heaptrack $NODE ${global_node_args[@]} ../test-app/lib/Main.js --sourceFile $file --targetDestination /tmp/out.bim ${global_transform_args[@]} $transform_args &&
      mv heaptrack.*.zst $filebase.heaptrack.zst
    )

    rm -f /tmp/out.bim
    chown mike $filebase*
    chmod +r $filebase*
  done
done
