java -cp /home/drg/projects/work2/samza-benchmark/samza-testbed/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.SSE.SSERealRateGenerator -host $1 -topic stock_sb -fp /home/drg/tools/SSE_data/sb-50ms.txt -interval 50

python -c 'import time; time.sleep(10)'
kill -9 $(jps | grep SSERealRateGenerator | awk '{print $1}')
