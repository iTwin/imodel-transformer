NODE='/home/mike/.local/share/pnpm/node'

for file in /home/mike/work/bad-aspect-old.bim /home/mike/work/Juergen.Hofer.Bad.Normals.bim /home/mike/work/shell-noobstruction.bim
do
  rm -f /tmp/out.bim
  strace -c $NODE -r source-map-support/register ../test-app/lib/Main.js --sourceFile $file --targetDestination /tmp/out.bim --noProvenance --danglingReferencesBehavior ignore -F |& tee json-in-js_$(basename $file).strace;
  /usr/bin/time -v $NODE -r source-map-support/register ../test-app/lib/Main.js --sourceFile $file --targetDestination /tmp/out.bim --noProvenance --danglingReferencesBehavior ignore -F |& tee json-in-js_$(basename $file).usrtime;
  $NODE --cpu-prof --cpu-prof-name=json-in-js_$(basename $file).js.cpuprofile --cpu-prof-interval=50000 -r source-map-support/register ../test-app/lib/Main.js --sourceFile $file --targetDestination /tmp/out.bim --noProvenance --danglingReferencesBehavior ignore -F;
  sudo bash -c "perf record -F 10 -g $NODE --perf-basic-prof --interpreted-frames-native-stack -r source-map-support/register ../test-app/lib/Main.js   --sourceFile $file --targetDestination /tmp/out.bim --noProvenance --danglingReferencesBehavior ignore -F && chmod +r perf.data && mv perf.data json-in-js_$(basename $file).perf.cpuprofile";
done
