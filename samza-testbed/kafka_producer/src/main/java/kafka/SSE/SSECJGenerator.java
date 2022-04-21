package kafka.SSE;

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
public class SSECJGenerator {

    private String TOPIC;

    private static KafkaProducer<String, String> producer;

    private static final int Trade_No = 0;
    private static final int Trade_Date = 1;
    private static final int Trade_Time = 2;
    private static final int Trade_Time_Dec =3;
    private static final int Order_Time = 4;
    private static final int Order_Time_Dec = 5;
    private static final int Order_No = 6;
    private static final int Trade_Price = 7;
    private static final int Trade_Amt = 8;
    private static final int Trade_Vol = 9;
    private static final int Sec_Code = 10;
    private static final int PBU_ID = 11;
    private static final int Acct_ID = 12;
    private static final int Trade_Dir= 13;
    private static final int Order_PrtFil_Code= 14;
    private static final int Tran_Type= 15;
    private static final int Trade_Type = 16;
    private static final int Proc_Type = 17;
    private static final int Order_Type = 18;
    private static final int Stat_PBU_ID = 19;
    private static final int Credit_Type = 20;

    public SSECJGenerator(String input) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "generator.SSEPartitioner");
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
            stream = new FileReader("/root/SSE-kafka-producer/"+file+".txt");
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
        String TOPIC = new String("stock_order");
        String file = new String("sort_CJ");
        int speed = 1;
        if (args.length > 0) {
//            TOPIC = args[0];
//            file = args[1];
            speed = Integer.parseInt(args[0]);
        }
        new SSECJGenerator(TOPIC).generate(file, speed);
    }
}

