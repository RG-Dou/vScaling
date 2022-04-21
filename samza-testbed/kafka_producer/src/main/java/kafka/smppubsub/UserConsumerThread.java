package kafka.smppubsub;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

class UserConsumerThread implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public UserConsumerThread(String brokers, String groupId, String topic) {
        Properties prop = createConsumerConfig(brokers, groupId);
        this.consumer = new KafkaConsumer<>(prop);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    private static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void run() {
        while (true) {
            Set<TopicPartition> assignedPartitions = consumer.assignment();
            consumer.seekToEnd(assignedPartitions);
            for (TopicPartition partition : assignedPartitions) {
//				System.out.println(vertexID + ": " + consumer.position(partition));
                long endPosition = consumer.position(partition);
                long recentMessagesStartPosition = endPosition - 1 < 0 ? 0 : endPosition - 1;
                consumer.seek(partition, recentMessagesStartPosition);
            }

            ConsumerRecords<String, String> records = consumer.poll(1);
            for (final ConsumerRecord record : records) {

//                long cur = System.nanoTime();
                long cur = System.currentTimeMillis();
                String combinedKey = (String) record.key();
//                while ((System.nanoTime() - cur) < 100) {}
                String value = (String) record.value();
                long ts = Long.valueOf(value.split(",")[0]);
                long latency = System.currentTimeMillis() - ts;
//                System.out.println(latency);
//                System.out.println(Long.valueOf(combinedKey.split("\\|")[1]));
//                System.out.println("timestamp: " + record.timestamp()+ " type: " + record.timestampType());
                System.out.println("Partition: " + record.partition() + ", Offset: " + record.offset() + " , Latency: " + latency);
            }
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}