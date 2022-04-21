deletechanelog=$4
isupload=$3
iscompile=$2
job=$1

#Check whether they are equal
if [ $deletechanelog == 1 ]
then
    echo skip...
    # ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper giraffe:2181 --topic stock-exchange-buy-changelog
    # ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper giraffe:2181 --topic stock-exchange-sell-changelog
    # ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper giraffe:2181 --topic __samza_coordinator_stock-exchange_162
    # ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper giraffe:2181 --topic stock_sb
    # python -c 'import time; time.sleep(360)'
    # ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --zookeeper giraffe:2181 --create --topic stock_sb --partitions 64 --replication-factor 1 #--config message.timestamp.type=LogAppendTime

    # awk -F"=" 'BEGIN{OFS=FS} $1=="job.id"{$2=$2+1}1' src/main/config/stock-exchange-ss-giraffe.properties > properties.tmp
    # mv properties.tmp src/main/config/stock-exchange-ss-giraffe.properties
fi

if [ $iscompile == 1 ] 
then 
    echo skip...
    # mvn clean package
fi

cd target

if [ $isupload == 1 ]
then
  ~/cluster/yarn/bin/hdfs dfs -rm  hdfs://giraffe:9000/testbed/*-dist.tar.gz
  ~/cluster/yarn/bin/hdfs dfs -mkdir hdfs://giraffe:9000/testbed
  ~/cluster/yarn/bin/hdfs dfs -put  *-dist.tar.gz hdfs://giraffe:9000/testbed
fi

tar -zvxf *-dist.tar.gz

if [ $job == 1 ]
then
OUTPUT=`./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-exchange-flamingo.properties | grep 'application_.*$'`
cd ..
appid=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
echo "$appid"

#python -c 'import time; time.sleep(60)'

#ssh giraffe << EOF
    #/home/samza/generate.sh giraffe:9092
#EOF

#Kill
python -c 'import time; time.sleep(10)'
pass=8520
echo $pass | sudo -S /yarn/bin/yarn application -kill ${appid}
#pass=8520
#echo $pass | ssh -tt giraffe "sudo /yarn/bin/yarn application -kill $appid"
#ssh giraffe << EOF
# /home/samza/clearKafkaAndZooKeeper.sh
#EOF

#./generate.sh giraffe:9092
#java -cp ../kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.SSE.SSERealRateGenerator -topic stock_sb -fp /home/samza/SSE_data/sb.txt
#./consumer.sh localhost:9092 stock_cj
#./consumer.sh giraffe:9092 stock_cj
#./consumer.sh alligator:9092,buffalo:9092 stock_cj
fi
if [ $job == 2 ]
then 
./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-average-task.properties
cd ..
#java -cp ../kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.SSE.SSERealRateGenerator -topic stock_cj -fp /home/samza/SSE_data/sb.txt
#./consumer.sh localhost:9092 stock_price
#./consumer.sh giraffe:9092 stock_price
#./consumer.sh alligator:9092,buffalo:9092 stock_price
fi
if [ $job == 3 ]
then 
./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-exchange.properties
./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-average-task.properties
cd ..
#java -cp ../kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.SSE.SSERealRateGenerator -topic stock_sb -fp /home/samza/SSE_data/sb.txt
#./consumer.sh localhost:9092 stock_price
#./consumer.sh giraffe:9092 stock_price
#./consumer.sh alligator:9092,buffalo:9092 stock_price
fi
