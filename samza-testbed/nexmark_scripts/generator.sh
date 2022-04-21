#!/usr/bin bash
APP_DIR="$(dirname $(pwd))"
Hadoop_Dir="/home/drg/projects/work2/hadoop-dir/Hadoop-YarnVerticalScaling"
# sh ... 0 localhost 3

IS_COMPILE=$1
HOST="giraffe"
#APP=$3

BROKER=${HOST}:9092
RATE=0
BASE=2600
CYCLE=60

App_List="2"
Mem_List="3000"
#em_List="750 900 950 1000 1100 1150 1450 1600 1650 1700 1750 1900 2000 2050 2200 2250 2300 2400"
#Mem_List="700 750 800 850 900 950 1000 1050 1100 1150 1200 1250 1300 1350 1400 1450 1500 1550 1600 1650 1700 1750 1800 1850 1900 1950 2000 2050 2100 2150 2200 2250 2300 2400 2500"
CPU_List="1"

function clearEnv() {
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic auctions
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic persons
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic bids
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q2-changelog
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_nexmark-q2_1
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q1-changelog
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q5-changelog
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-changelog
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-join-join-L
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-join-join-R
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-partition_by-auction
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-partition_by-person
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_nexmark-q8_1
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_nexmark-q1_1
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_nexmark-q5_1
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic results
#    python -c 'import time; time.sleep(1)'

#    ~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic auctions --partitions 8 --replication-factor 1  --config message.timestamp.type=LogAppendTime
#    ~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic persons --partitions 8 --replication-factor 1 --config message.timestamp.type=LogAppendTime
    ~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic auctions --partitions 16 --replication-factor 1
    ~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic persons --partitions 16 --replication-factor 1
    ~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic bids --partitions 48 --replication-factor 1
}

function configApp() {
#    sed -i -- 's/localhost/'${HOST}'/g' ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties
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
    echo "assigned app id is: $appid"
}

function killApp() {
#    ~/samza-hello-samza/deploy/yarn/bin/yarn application -kill $appid
    kill -9 $(jps | grep Generator | awk '{print $1}')
    ${APP_DIR}/testbed_1.0.0/target/bin/kill-all.sh
    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q${APP}-1
    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q2-1
    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q5-1
    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-nexmark-q8-1
    cp -rf ${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/logs/userlogs/* /data/drg_data/results/
}

~/tools/cleanKafka.sh
function main(){
    clearEnv
    configApp $APP $CPU $MEM

    if [[ ${IS_COMPILE} == 1 ]];
    then
        compile
        compileGenerator
        uploadHDFS
    fi

    runApp $APP

    # wait for app start
    python -c 'import time; time.sleep(5)'

    if [[ ${APP} == 1 ]] || [[ ${APP} == 5 ]] || [[ ${APP} == 2 ]];
    then
	for j in {1..1}
        do
            generateBid
    	done
    elif [[ ${APP} == 8 ]] || [[ ${APP} == 3 ]];
    then
	for j in {1..1}
        do
            generateAuction
    	done
	for j in {1..1}
        do
            generatePerson
    	done
    fi
    # run 1200s
    python -c 'import time; time.sleep(1800)'
    killApp
}


for APP in $App_List; do
    for CPU in $CPU_List; do
        for MEM in $Mem_List; do
            main $APP $CPU $MEM
        done
    done
done

