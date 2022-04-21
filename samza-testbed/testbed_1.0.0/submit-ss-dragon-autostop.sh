#!/usr/bin/env bash

deletechanelog=1
isupload=1
iscompile=1
job=1

CPU_SWITCH=$1
MEM_SWITCH=$2
ARRIVAL_SWITCH=$3
Hadoop_Dir="/home/drg/projects/work2/hadoop-dir/Hadoop-YarnVerticalScaling/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/"
APP_DIR="$(dirname $(pwd))"

HOST="giraffe"


function runApp() {
	java -cp /home/drg/projects/work2/samza-benchmark/samza-testbed/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.SSE.SSERealRateGenerator -host localhost:9092 -topic stock_sb -fp /home/drg/tools/SSE_data/sb-opening-50ms.txt -interval 50 &
}

function killApp() {
	kill -9 $(jps | grep SSERealRateGenerator | awk '{print $1}')
	~/cluster/yarn/bin/yarn application -kill ${appid}
	~/tools/zookeeper/bin/zkCli.sh deleteall /app-stock-exchange-861
	cp -rf ${Hadoop_Dir}/logs/userlogs/* /home/drg/results/
}




~/tools/cleanKafka.sh
#Check whether they are equal
if [ $deletechanelog == 1 ]
then
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic stock-exchange-buy-changelog
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic stock-exchange-sell-changelog
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_stock-exchange_162
    ~/tools/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic stock_sb
    python -c 'import time; time.sleep(20)'
    ~/tools/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic stock_sb --partitions 64 --replication-factor 1 #--config message.timestamp.type=LogAppendTime

    sed -i "s/^\(verticalscaling.cpu.switch\)=\(true\|false\)/\1=$CPU_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/stock-exchange-ss-dragon-backlogdelay.properties
    sed -i "s/^\(verticalscaling.mem.switch\)=\(true\|false\)/\1=$MEM_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/stock-exchange-ss-dragon-backlogdelay.properties
    sed -i "s/^\(verticalscaling.processed_arrival_rate.switch\)=\(true\|false\)/\1=$ARRIVAL_SWITCH/" ${APP_DIR}/testbed_1.0.0/src/main/config/stock-exchange-ss-dragon-backlogdelay.properties


fi

if [ $iscompile == 1 ] 
then 
    mvn clean package
fi

cd target

if [ $isupload == 1 ]
then
  ~/cluster/yarn/bin/hdfs dfs -rm  hdfs://${HOST}:9000/testbed/*-dist.tar.gz
  ~/cluster/yarn/bin/hdfs dfs -mkdir hdfs://${HOST}:9000/testbed
  ~/cluster/yarn/bin/hdfs dfs -put  *-dist.tar.gz hdfs://${HOST}:9000/testbed
fi

tar -zvxf *-dist.tar.gz

if [ $job == 1 ]
then
OUTPUT=`./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-exchange-ss-dragon-backlogdelay.properties | grep 'application_.*$'`
cd ..
appid=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
echo "$appid"

python -c 'import time; time.sleep(60)'

#~/tools/generate.sh localhost:9092
#java -cp /home/drg/projects/work2/samza-benchmark/samza-testbed/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.SSE.SSERealRateGenerator -host localhost:9092 -topic stock_sb -fp /home/drg/tools/SSE_data/sb-opening-50ms.txt -interval 50

runApp

#Kill
python -c 'import time; time.sleep(1800)'
python draw_groundTruth.py ${Hadoop_Dir}/logs/userlogs/${appid}

killApp
#kill -9 $(jps | grep SSERealRateGenerator | awk '{print $1}')
#~/cluster/yarn/bin/yarn application -kill ${appid}
#~/tools/zookeeper/bin/zkCli.sh deleteall /app-stock-exchange-861
#cp -rf ${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/logs/userlogs/* /home/drg/results/
fi


if [ $job == 2 ]
then 
./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-average-task.properties
cd ..
fi


if [ $job == 3 ]
then 
./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-exchange.properties
./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-average-task.properties
cd ..
fi
