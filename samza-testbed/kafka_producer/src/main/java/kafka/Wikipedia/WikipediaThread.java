package kafka.Wikipedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class WikipediaThread implements Runnable {

    private String TOPIC;
    private int speed;
    private static KafkaProducer<String, String> producer;

    public WikipediaThread(String input, String brokers, int speed) {
	this.speed = speed;
        this.TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        int sent_sentences = 0;
        long cur = 0;
        long start = 0;
        long interval = 0;
        int counter = 0;
        try {
            while(true) {
                stream = new FileReader("/home/myc/workspace/cs5101-experiment/src/main/java/generator/wikipedia-raw.json");
                br = new BufferedReader(stream);

                interval = 1000000000 / this.speed;
                start = System.nanoTime();

                while ((sCurrentLine = br.readLine()) != null) {
                    cur = System.nanoTime();
                    ProducerRecord<String,String> newRecord = new ProducerRecord<>(TOPIC, String.valueOf(System.currentTimeMillis()), sCurrentLine);
                    producer.send(newRecord);
                    counter++;

                    while ((System.nanoTime() - cur) < interval) {}
                    if (System.nanoTime() - start >= 1000000000) {
                        System.out.println("output rate: " + counter);
                        counter = 0;
                        start = System.nanoTime();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(stream != null) stream.close();
                if(br != null) br.close();
            } catch(IOException ex) {
                ex.printStackTrace();
            }
        }
        producer.close();
    }
}
