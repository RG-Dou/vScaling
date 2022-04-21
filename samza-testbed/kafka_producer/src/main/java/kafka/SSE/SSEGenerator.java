package kafka.SSE;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * SSE generaor
 */
public class SSEGenerator {

    private String TOPIC;

    private static KafkaProducer<String, String> producer;

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    public SSEGenerator(String input) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "kafka.SSE.SSEPartitioner");
        producer = new KafkaProducer<String, String>(props);

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
            stream = new FileReader("/root/SSE-kafka-producer/partition1.txt");
            br = new BufferedReader(stream);

            interval = 1000000000/speed;
            start = System.nanoTime();

            while ((sCurrentLine = br.readLine()) != null) {

                cur = System.nanoTime();
                if (sCurrentLine.equals("end")) {
                    continue;
                }

                if (sCurrentLine.split("\\|").length < 10) {
                    continue;
                }

                ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, sCurrentLine.split("\\|")[Sec_Code], sCurrentLine);
                producer.send(newRecord);
                counter++;

                while ((System.nanoTime() - cur) < interval) {}
                if (System.nanoTime() - start >= 1000000000) {
                    System.out.println("output rate: " + counter);
                    counter = 0;
                    start = System.nanoTime();
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
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        String TOPIC = params.get("topic", "stock_sb");
        String file = params.get("fp", "partition1");
        int speed = params.getInt("rate", 100);

        new SSEGenerator(TOPIC).generate(file, speed);
    }
}

