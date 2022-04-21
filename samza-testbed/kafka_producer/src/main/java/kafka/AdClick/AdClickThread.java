package kafka.AdClick;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdClickThread implements Runnable {

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();

    public AdClickThread(String brokers, String groupId, String topic) {
        Properties prop = createProducerConfig(brokers, groupId);
        this.producer = new KafkaProducer<String, byte[]>(prop);
        this.topic = topic;

    }

    private static Properties createProducerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "AdClickProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return props;
    }


    private static final List<String> userIds = Arrays.asList("user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9");
    private static final List<String> adIds = Arrays.asList("ad1", "ad2", "ad3", "ad4", "ad5");
    private static final List<String> pageIds = Arrays.asList("google.com", "baidu.com", "yahoo.com", "bing.com", "bilibili.com", "hadoop.com", "spark.com", "samza.com");

    private static ObjectNode randomAdClickEvent() {
        // In a real app you might want to take advantage of Jackson's data binding features.
        // Since Jackson is not the focus of this example, let's just build the JSON manually.
        ObjectNode clickEvent = objectMapper.createObjectNode();
        clickEvent.put("userId", userIds.get(random.nextInt(userIds.size())));
        clickEvent.put("adId", adIds.get(random.nextInt(adIds.size())));
        clickEvent.put("pageId", pageIds.get(random.nextInt(pageIds.size())));
        return clickEvent;
    }

    @Override
    public void run() {

        while (true) {
            ObjectNode clickEvent = randomAdClickEvent();
            try {

                String key = UUID.randomUUID().toString();
                byte[] valueJson = objectMapper.writeValueAsBytes(clickEvent);
                ProducerRecord<String, byte[]> newRecord = new ProducerRecord<String, byte[]>(topic, key, valueJson);

                RecordMetadata md = producer.send(newRecord).get();
                System.out.println("Published " + md.topic() + "/" + md.partition() + "/" + md.offset()
                        + " (key=" + key + ") : " + clickEvent);
                Thread.sleep(1000);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
