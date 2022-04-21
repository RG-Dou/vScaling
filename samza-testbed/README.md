## monitor to measure throughput
monitor every stage throughput

## setup

### dependency:
Samza(install in Samza offical website)
download hello-samza, and run <code>grid install all</code>

### package
go into monitor directory
run <code>mvn clean package</code>
go to /target and run <code>tar -zvxf <package name></code>

## run monitor

cd <package name>, run:
<code>bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/window.properties</code>
or run:
<code>bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/filter.properties</code>

## see result

### requirement

A Kafka Stream to consume: WordCount(use test-generator to generate, see another repo)

### read result from Kafka topic 'windowStage'

cd Kafka Dir
run this to see all topics in Kafka:
<code>bin/kafka-topics.sh --list --zookeeper localhost:2181</code>

run this to see windowStage:
<code>bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic windowStage --from-beginning</code>