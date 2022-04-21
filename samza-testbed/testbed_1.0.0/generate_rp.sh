java -cp ../kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.SSE.SSERealRateRPGenerator -host $1 -topic stock_sb -fp /home/samza/SSE_data/sb-50ms.txt -interval 50
