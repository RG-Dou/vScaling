#!/usr/bin/env bash
Hadoop_Dir="/home/drg/projects/work2/hadoop-dir/Hadoop-YarnVerticalScaling"
APP_DIR="$(dirname $(pwd))"

IS_COMPILE=1
HOST="giraffe"
APP=$1
CYCLE=300
BASE=$5
RATE=1000

CPU_SWITCH=$2
MEM_SWITCH=$3
ARRIVAL_SWITCH=$4

MEM=$6

~/tools/cleanKafka.sh

function clearEnv() {
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic auctions
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic persons
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic bids

    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q${APP}-changelog
   # ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q3-changelog
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_nexmark-q${APP}_1
   # ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q1-changelog
   # ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q5-changelog
   # ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-changelog
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q${APP}-1-join-join-L
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q${APP}-1-join-join-R
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q${APP}-1-partition_by-auction
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q${APP}-1-partition_by-person
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_nexmark-q${APP}_1
   # ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_nexmark-q1_1
   # ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_nexmark-q5_1
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic results
#    python -c 'import time; time.sleep(1)'

    ~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic auctions --partitions 128 --replication-factor 1
    ~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic persons --partitions 128 --replication-factor 1
    ~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic bids --partitions 128 --replication-factor 1
}


function configAppSrc() {
    sed -i "s/^\(verticalscaling.cpu.switch\)=\(true\|false\)/\1=$CPU_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties
    sed -i "s/^\(verticalscaling.mem.switch\)=\(true\|false\)/\1=$MEM_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties
    sed -i "s/^\(verticalscaling.processed_arrival_rate.switch\)=\(true\|false\)/\1=$ARRIVAL_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties
    sed -ri "s|(cluster-manager.container.memory.mb=)[0-9]*|cluster-manager.container.memory.mb=$MEM|" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties
}

function compile() {
    cd ${APP_DIR}/testbed_1.0.0/
    mvn clean package
    cd target
    tar -zvxf *-dist.tar.gz
    cd ${APP_DIR}
}

function uploadHDFS() {
    ~/cluster/yarn/bin/hdfs dfs -rm  hdfs://${HOST}:9000/testbed-nexmark/*-dist.tar.gz
    ~/cluster/yarn/bin/hdfs dfs -mkdir hdfs://${HOST}:9000/testbed-nexmark
    ~/cluster/yarn/bin/hdfs dfs -put  ${APP_DIR}/testbed_1.0.0/target/*-dist.tar.gz hdfs://${HOST}:9000/testbed-nexmark
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
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaBidGenerator \
        -host $BROKER -topic bids -rate $RATE -cycle $CYCLE -base $BASE &
}

function generatePerson() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaPersonGenerator \
        -host $BROKER -topic persons -rate $RATE -cycle $CYCLE -base $BASE &
}

function runApp() {
    OUTPUT=`${APP_DIR}/testbed_1.0.0/target/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties | grep 'application_.*$'`
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

function killApp() {
    ~/cluster/yarn/bin/yarn application -kill $app
    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q${APP}-1
    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q1-1
#    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q2-1
#    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q3-1
#    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q5-1
#    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q8-1
    cp -rf ${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/logs/userlogs/* /home/drg/results/
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

    BROKER=${HOST}:9092

    if [[ ${APP} == 1 ]] || [[ ${APP} == 5 ]] || [[ ${APP} == 2 ]] || [[ ${APP} == 11 ]];
    then
        for j in {1..1}
        do
            generateBid
        done
    elif [[ ${APP} == 8 ]] || [[ ${APP} == 3 ]];
    then
        for j in {1..5}
        do
            generateAuction
        done
        for j in {1..5}
        do
            generatePerson
        done
    fi

    python -c 'import time; time.sleep(1800)'

    killGenerator
    pwd
    python ${APP_DIR}/nexmark_scripts/draw_groundTruth.py ${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/logs/userlogs/application_${appid}

    killApp
    killGenerator

}

main
