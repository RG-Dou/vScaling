#!/usr/bin bash
APP_DIR="$(dirname $(pwd))"
Hadoop_Dir="/home/drg/projects/work2/hadoop-dir/Hadoop-YarnVerticalScaling"
# sh ... 0 localhost 3
IS_COMPILE=1
HOST="giraffe"

BROKER=${HOST}:9092
RATE=0
BASE=1000
CYCLE=60

function clearEnv() {
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic position-reports
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic stop-detection
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic stopped-cars-changelog
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic all-reports-changelog
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic results

    ~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic position-reports --partitions 8 --replication-factor 1
    #~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic stop-detection --partitions 16 --replication-factor 1
}

function compile() {
    cd ${APP_DIR}/testbed_1.0.0/
    mvn clean package
    cd target
    tar -zvxf *-dist.tar.gz
    cd ${APP_DIR}
}

function uploadHDFS() {
    ~/cluster/yarn/bin/hdfs dfs -rm  hdfs://${HOST}:9000/testbed-ad/*-dist.tar.gz
    ~/cluster/yarn/bin/hdfs dfs -mkdir hdfs://${HOST}:9000/testbed-ad
    ~/cluster/yarn/bin/hdfs dfs -put  ${APP_DIR}/testbed_1.0.0/target/*-dist.tar.gz hdfs://${HOST}:9000/testbed-ad
}

function compileGenerator() {
    cd ${APP_DIR}/kafka_producer/
    mvn clean package
    cd ${APP_DIR}
}

function generateReport() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.AD.ReportGenerator \
        -host $BROKER -interval 100 -fileName /home/drg/tools/datadriverTestData/datafile3hours.dat -keySize 8 &
}

function runApp() {
    OUTPUT=`${APP_DIR}/testbed_1.0.0/target/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://${APP_DIR}/testbed_1.0.0/target/config/accident-detection.properties | grep 'application_.*$'`
    app=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
    appid=${app#application_}
    echo "assigned app id is: $appid"
}

function killApp() {
#    ~/samza-hello-samza/deploy/yarn/bin/yarn application -kill $appid
    kill -9 $(jps | grep Generator | awk '{print $1}')
    ${APP_DIR}/testbed_1.0.0/target/bin/kill-all.sh
    ~/tools/zookeeper/bin/zkCli.sh deleteall /app-accident-detection-861
    #cp -rf ${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/logs/userlogs/* /data/drg_data/results/
}

~/tools/cleanKafka.sh
function main(){
    clearEnv

    if [[ ${IS_COMPILE} == 1 ]];
    then
        compile
        compileGenerator
        uploadHDFS
    fi

    runApp
    python -c 'import time; time.sleep(120)'

    generateReport
    # run 1200s
    python -c 'import time; time.sleep(620)'
    killApp
}

main
