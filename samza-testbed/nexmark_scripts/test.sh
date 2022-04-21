#!/usr/bin/env bash
#APP_DIR="$(dirname $(pwd))"
#
#function main(){
##    mem=$1
#    echo "$mem"
#}
#mem_list="500 750 1000 1250 1500 1750 2000"
#for mem in $mem_list; do
#    main $mem
#done

#${APP_DIR}/testbed_1.0.0/target/bin/kill-all.sh

#mem=333

#cp -rf /home/drg/projects/hadoop/hadoop-verticalScaling/hadoop-dist/target/hadoop-3.3.0-SNAPSHOT/logs/userlogs/* /home/drg/tools/
#
#sed -ri "s|(cluster-manager.container.memory.mb=)[0-9]*|cluster-manager.container.memory.mb=$mem|" ../testbed_1.0.0/src/main/config/nexmark-q1.properties
python draw_groundTruth.py /hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/logs/userlogs/
mem=1
echo "$mem"
let mem=2
echo "$mem"
