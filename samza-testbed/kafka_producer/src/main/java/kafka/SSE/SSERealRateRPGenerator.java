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

import static java.lang.Thread.sleep;

/**
 * SSE generaor
 */
public class SSERealRateRPGenerator {

    private String TOPIC;

    private static KafkaProducer<String, String> producer;

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    public SSERealRateRPGenerator(String input, String brokers) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "kafka.SSE.SSERRPartitioner");
        producer = new KafkaProducer<String, String>(props);

    }

    public void generate(String FILE, int REPEAT, int INTERVAL) throws InterruptedException {

        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        int sent_sentences = 0;
        long cur = 0;
        long start = 0;
        int counter = 0;

        int noRecSleepCnt = 0;

        try {
            stream = new FileReader(FILE);
            br = new BufferedReader(stream);

            start = System.currentTimeMillis();

            while ((sCurrentLine = br.readLine()) != null) {

                if (sCurrentLine.equals("end")) {
                    noRecSleepCnt++;
                    if (counter > 2500 && noRecSleepCnt % (1000/INTERVAL) == 0) {
                        counter = 0;
                        System.out.println("timestamp :" + noRecSleepCnt + " rate: " + counter);
                    }
                    //System.out.println("output rate: " + counter + " per " + INTERVAL + "ms");
                    // counter = 0;
                    cur = System.currentTimeMillis();
                   if (cur-start < INTERVAL) {
                        //sleep(INTERVAL - (cur - start));
                    } else {
                        System.out.println("rate exceeds" + INTERVAL + "ms.");
                    }
                    start = System.currentTimeMillis();
                }

                if (sCurrentLine.split("\\|").length < 10) {
                    continue;
                }

                for (int i=0; i< REPEAT; i++) {
//                    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, sCurrentLine);
//                    producer.send(newRecord);
                    counter++;
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
        String FILE = params.get("fp", "/home/samza/SSE_data/sb.txt");
        int REPEAT = params.getInt("repeat", 1);
        String BROKERS = params.get("host", "camel:9092");
        int INTERVAL = params.getInt("interval", 1000);

        System.out.println(TOPIC + FILE + REPEAT + BROKERS);

        new SSERealRateRPGenerator(TOPIC, BROKERS).generate(FILE, REPEAT, INTERVAL);
    }
}

