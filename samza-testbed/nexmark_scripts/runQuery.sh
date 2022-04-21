#!/usr/bin/env bash
APP_DIR="$(dirname $(pwd))"

IS_COMPILE=$1
HOST=$2
APP=$3

function clearEnv() {
    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic auctions
    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic persons
    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-join-join-R
    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-join-join-L
    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-partition_by-auction
    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-partition_by-person
    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_nexmark-q8_1
    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic results
    python -c 'import time; time.sleep(1)'

    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic auctions --partitions 8 --replication-factor 1  --config message.timestamp.type=LogAppendTime
    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic persons --partitions 8 --replication-factor 1 --config message.timestamp.type=LogAppendTime
}

function configApp() {
    sed -i -- 's/localhost/'${HOST}'/g' ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties
}

function compile() {
    cd ${APP_DIR}/testbed_1.0.0/
    mvn clean package
    cd target
    tar -zvxf *-dist.tar.gz
    cd ${APP_DIR}
}

function uploadHDFS() {
    ~/samza-hello-samza/deploy/yarn/bin/hdfs dfs -rm  hdfs://${HOST}:9000/testbed/*-dist.tar.gz
    ~/samza-hello-samza/deploy/yarn/bin/hdfs dfs -mkdir hdfs://${HOST}:9000/testbed
    ~/samza-hello-samza/deploy/yarn/bin/hdfs dfs -put  ${APP_DIR}/testbed_1.0.0/target/*-dist.tar.gz hdfs://${HOST}:9000/testbed
}

function compileGenerator() {
    cd ${APP_DIR}/kafka_producer/
    mvn clean package
    cd ${APP_DIR}
}

function generateAuction() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaAuctionGenerator \
        -host $BROKER -topic auctions -rate $RATE -cycle $CYCLE &
}

function generateBid() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaBidGenerator \
        -host $BROKER -topic bids -rate $RATE -cycle $CYCLE &
}

function generatePerson() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaPersonGenerator \
        -host $BROKER -topic persons -rate $RATE -cycle $CYCLE &
}

function runApp() {
    OUTPUT=`${APP_DIR}/testbed_1.0.0/target/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties | grep 'application_.*$'`
    appid=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
    echo "assigned app id is: $appid"
}

function killApp() {
#    ~/samza-hello-samza/deploy/yarn/bin/yarn application -kill $appid
    kill -9 $(jps | grep Generator | awk '{print $1}')
    ${APP_DIR}/testbed_1.0.0/target/bin/kill-all.sh
}


if [ ${IS_COMPILE} == 1 ]
then
    compile
    compileGenerator
    uploadHDFS
fi

configApp
runApp

# wait for app start
python -c 'import time; time.sleep(5)'

BROKER=${HOST}:9092
RATE=50000
CYCLE=60

if [[ ${APP} == 1 ]]
then
    generateBid
elif [[ ${APP} == 8 ]]
then
    clearEnv
    generateAuction
    generatePerson
fi


# run 120s
python -c 'import time; time.sleep(300)'
killApp
