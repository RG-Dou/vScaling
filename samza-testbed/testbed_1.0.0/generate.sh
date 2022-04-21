#!/usr/bin bash
java -cp ../kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.SSE.SSERealRateGenerator -host localhost:9092 -topic stock_sb -fp /home/drg/data/SSE_data/sb-50ms.txt -interval 50 --rate 1000 --more
