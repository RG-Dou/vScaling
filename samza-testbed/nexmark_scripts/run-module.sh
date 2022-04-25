#!/usr/bin/env bash
Root_Dir="$(dirname $(dirname $(pwd)))"
APP_DIR=$Root_Dir/samza-testbed
Hadoop_Dir=$Root_Dir/Hadoop-vScaling/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT
Tool_Dir=$Root_Dir/tools
Topic_shell=$Tool_Dir/kafka/bin/kafka-topics.sh
IS_COMPILE=1
HOST="localhost"
APP=$1
Policy="default"
CYCLE=300
BASE=4000
RATE=4000
CPU_SWITCH="true"
MEM_SWITCH="true"
ARRIVAL_SWITCH="true"

if [ $2 == 'CPU Scheduling' ];
then
  MEM_SWITCH="false"
elif [ $2 == 'Memory Scheduling' ]; then
  CPU_SWITCH="false"
elif [ $2 == 'Both with Current Arrival Rate' ]; then
  ARRIVAL_SWITCH="false"
elif [ $2 == 'Static' ]; then
  CPU_SWITCH="false"
  MEM_SWITCH="false"
fi

MEM=1250

# sh $Tool_Dir/script/cleanKafka.sh

function delete_topic() {
    $Topic_shell --delete --zookeeper ${HOST}:2181 --topic $1
}

function create_topic() {
    $Topic_shell --create --zookeeper ${HOST}:2181 --topic $1 --partitions 128 --replication-factor 1
}

function clearEnv() {
    delete_topic auctions
    delete_topic persons
    delete_topic bids
    delete_topic nexmark-q${APP}-changelog
    delete_topic __samza_coordinator_nexmark-q${APP}_1
    delete_topic nexmark-q${APP}-1-join-join-L
    delete_topic nexmark-q${APP}-1-join-join-R
    delete_topic nexmark-q${APP}-1-partition_by-auction
    delete_topic nexmark-q${APP}-1-partition_by-person
    delete_topic __samza_coordinator_nexmark-q${APP}_1
    delete_topic results
#    python -c 'import time; time.sleep(1)'

    create_topic auctions
    create_topic persons
    create_topic bids
}


function configAppSrc() {
    sed -i "s/^\(verticalscaling.cpu.switch\)=\(true\|false\)/\1=$CPU_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}-effect.properties
    sed -i "s/^\(verticalscaling.mem.switch\)=\(true\|false\)/\1=$MEM_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}-effect.properties
    sed -i "s/^\(verticalscaling.processed_arrival_rate.switch\)=\(true\|false\)/\1=$ARRIVAL_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}-effect.properties
    sed -ri "s|(cluster-manager.container.memory.mb=)[0-9]*|cluster-manager.container.memory.mb=$MEM|" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}-effect.properties
    sed -i "s/^\(verticalscaling.cpu.algorithm\)=\(default\|elasticutor\)/\1=$Policy/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}-effect.properties
}

function compile() {
    cd ${APP_DIR}/testbed_1.0.0/
    mvn clean package
    cd target
    tar -zvxf *-dist.tar.gz
    cd ${APP_DIR}
}

function uploadHDFS() {
    $Hadoop_Dir/bin/hdfs dfs -rm  hdfs://${HOST}:9000/testbed-nexmark/*-dist.tar.gz
    $Hadoop_Dir/bin/hdfs dfs -mkdir hdfs://${HOST}:9000/testbed-nexmark
    $Hadoop_Dir/bin/hdfs dfs -put  ${APP_DIR}/testbed_1.0.0/target/*-dist.tar.gz hdfs://${HOST}:9000/testbed-nexmark
}

function compileGenerator() {
    cd ${APP_DIR}/kafka_producer/
    mvn clean package
    cd ${APP_DIR}
}

function generateAuction() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaAuctionGenerator \
        -host $BROKER -topic auctions -rate $RATE -cycle $CYCLE -base $BASE &
}

function generateBid() {
    echo $RATE
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaBidGeneratorEffect \
        -host $BROKER -topic bids -rate $RATE -cycle $CYCLE -base $BASE &
}

function generatePerson() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaPersonGenerator \
        -host $BROKER -topic persons -rate $RATE -cycle $CYCLE -base $BASE &
}

function runApp() {
    OUTPUT=`${APP_DIR}/testbed_1.0.0/target/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}-effect.properties | grep 'application_.*$'`
    app=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
    appid=${app#application_}
    echo "${APP_DIR}"
    echo "assigned app id is: $appid"
}

function runAppStatic() {
    OUTPUT=`${APP_DIR}/testbed_1.0.0/target/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}-static.properties | grep 'application_.*$'`
    app=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
    appid=${app#application_}
    echo "assigned app id is: $appid"
}

# shellcheck disable=SC2120
function killApp() {
    $Hadoop_Dir/bin/yarn application -kill $app
    $Tool_Dir/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q${APP}-1
    $Tool_Dir/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q1-1
#    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q2-1
#    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q3-1
#    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q5-1
#    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q8-1
    rm -rf $Tool_Dir/results/effect/$2
    mkdir -rf $Tool_Dir/results/effect/$2
    cp -rf ${Hadoop_Dir}/logs/userlogs/* $Tool_Dir/results/effect/$2
}

function killGenerator() {
    kill -9 $(jps | grep Generator | awk '{print $1}')
}

configAppSrc
clearEnv
if [ ${IS_COMPILE} == 1 ]
then
    compile
    compileGenerator
    uploadHDFS
fi


function main(){
    # wait for app start
    runApp

    python -c 'import time; time.sleep(100)'
    for j in {1..4}
    do
          generateBid
    done


    python -c 'import time; time.sleep(1800)'

    killGenerator
    pwd
#    python ${APP_DIR}/nexmark_scripts/draw_groundTruth.py ${Hadoop_Dir}/logs/userlogs/application_${appid}

    killApp
    killGenerator

}

main
