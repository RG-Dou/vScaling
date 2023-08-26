#!/usr/bin/env bash
Root_Dir="$(dirname $(dirname $(dirname $(pwd))))"
APP_DIR=$Root_Dir/samza-testbed
Hadoop_Dir=$Root_Dir/Hadoop-vScaling/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT
Tool_Dir=$Root_Dir/tools
Topic_shell=$Tool_Dir/kafka/bin/kafka-topics.sh
#Topic_shell=~/tools/kafka/bin/kafka-topics.sh
DATA_DIR=/data/drg_data/work2/results/

# For running config
IS_COMPILE=$1
HOST=$2
APP=$3
DURATION=$4

# For source config
CYCLE=$5
BASE=$6
RATE=$7

# For policy config
Policy=$8
CPU_SWITCH=$9
MEM_SWITCH=${10}
ARRIVAL_SWITCH=${11}

# For resource config
MEM=${12}
CPU=${13}
NUM_EXECUTORS=${14}

# For store config
PARENT_DIR=${15}
CHILD_DIR=${16}


function delete_topic() {
    $Topic_shell --delete --zookeeper ${HOST}:2181 --topic $1
}

function create_topic() {
  while true; do
      create_output=$($Topic_shell --create --zookeeper ${HOST}:2181 --topic $1 --partitions 128 --replication-factor 1 2>&1)
      if [[ $create_output == *" already exists"* ]]; then
        echo "Topic $1 does not exist. Deleting and recreating..."
        delete_topic $1 # Use the provided variable $1
        sleep 2
      elif [[ $create_output == *"Created topic"* ]]; then
        echo "Topic $1 created successfully."
        break
      else
        echo "An error occurred while creating the topic."
        echo "$create_output"
        exit 1
      fi
  done
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
    delete_topic nexmark-q${APP}-1-window-count
    delete_topic __samza_coordinator_nexmark-q${APP}_1
    delete_topic results
    python -c 'import time; time.sleep(10)'

    create_topic auctions
    create_topic persons
    create_topic bids
}


function configAppSrc() {
    # the number of executors
    sed -ri "s|(job.container.count=)[0-9]*|job.container.count=$NUM_EXECUTORS|" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties

    # the policy
    sed -i "s/^\(verticalscaling.cpu.switch\)=\(true\|false\)/\1=$CPU_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties
    sed -i "s/^\(verticalscaling.mem.switch\)=\(true\|false\)/\1=$MEM_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties
    sed -i "s/^\(verticalscaling.processed_arrival_rate.switch\)=\(true\|false\)/\1=$ARRIVAL_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties
    sed -i "s/^\(verticalscaling.cpu.algorithm\)=\(default\|elasticutor\|memorySaving\)/\1=$Policy/" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties

    # resources
    sed -ri "s|(cluster-manager.container.memory.mb=)[0-9]*|cluster-manager.container.memory.mb=$MEM|" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties
    sed -ri "s|(cluster-manager.container.cpu.cores=)[0-9]*|cluster-manager.container.cpu.cores=$CPU|" ${APP_DIR}/testbed_1.0.0/src/main/config/nexmark-q${APP}.properties
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
    $Hadoop_Dir/bin/yarn application -kill $app
    $Tool_Dir/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q${APP}-1
#    $Tool_Dir/zookeeper/bin/zkCli.sh delete /brokers/ids/0
    path1=$DATA_DIR/$PARENT_DIR
    if [ ! -d "$path1" ]; then
        mkdir -p $path1
        echo "Created $path1"
    fi
    path2=$path1/$CHILD_DIR
    if [ -d "$path2" ]; then
        rm -r $path2
        echo "Deleted $path2"
    fi
    mkdir -p $path2
#    rm -rf $Tool_Dir/results/effect/$MODULE
#    mkdir $Tool_Dir/results/effect/$MODULE
    cp -rf ${Hadoop_Dir}/logs/userlogs/$app $path2
}

function killGenerator() {
    kill -9 $(jps | grep Generator | awk '{print $1}')
}

bash $Tool_Dir/script/cleanKafka.sh
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

    python -c "import time; time.sleep(${DURATION})"

    killGenerator
    pwd

    killApp
    killGenerator

}

main
