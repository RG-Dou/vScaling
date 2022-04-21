package kafka.Wikipedia;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class WikipediaGenerator {
    private String TOPIC;

    private static KafkaProducer<String, String> producer;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public WikipediaGenerator(String input) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(props);

    }

    public void generate(String file, int speed) throws InterruptedException {

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

                interval = 1000000000 / 10;
                start = System.nanoTime();

                while ((sCurrentLine = br.readLine()) != null) {
                    cur = System.nanoTime();

//                    ObjectNode pageViewEvent = wikipediaEvent(sCurrentLine);
//                    JSONObject wiki = new JSONObject(sCurrentLine);
//                    ProducerRecord<String, byte[]> newRecord = new ProducerRecord<>(TOPIC, String.valueOf(System.currentTimeMillis()), wiki.toString().getBytes());
                    ProducerRecord<String,String> newRecord = new ProducerRecord<>(TOPIC, String.valueOf(System.currentTimeMillis()), sCurrentLine);
                    producer.send(newRecord);
                    counter++;

                    while ((System.nanoTime() - cur) < interval) {
                    }
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
        //logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));
    }

    public static void main(String[] args) throws InterruptedException {
        String TOPIC = new String("wikipedia");
        String file = new String("wikipedia-raw");
        int speed = 1;
        if (args.length > 0) {
            //TOPIC = args[0];
            //file = args[1];
            speed = Integer.parseInt(args[0]);
        }
//        new WikipediaGenerator(TOPIC).generate(file, speed);

        // Start group of User Consumer Thread
        WikipediaThread wikipediaProducer = new WikipediaThread("wikipedia", "kafka-server-1:9092", speed);
        WikipediaThread wikinewsProducer = new WikipediaThread("wikinews","kafka-server-1:9092", speed);
        WikipediaThread wiktionaryProducer = new WikipediaThread("wiktionary","kafka-server-1:9092", speed);

        Thread t1 = new Thread(wikipediaProducer);
        t1.start();

        Thread t2 = new Thread(wikinewsProducer);
        t2.start();

        Thread t3 = new Thread(wiktionaryProducer);
        t3.start();
    }
}
