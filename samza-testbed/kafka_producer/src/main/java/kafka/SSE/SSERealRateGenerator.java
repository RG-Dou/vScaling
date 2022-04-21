package kafka.SSE;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;
import static java.lang.Thread.sleep;

/**
 * SSE generaor
 */
public class SSERealRateGenerator {

    private String TOPIC;

    private static KafkaProducer<String, String> producer;

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Last_Upd_Time = 2;
    private static final int Order_Price = 3;
    private static final int Order_Exec_Vol = 4;
    private static final int Order_Vol = 5;
    private static final int Sec_Code = 6;
    private static final int Trade_Dir = 7;
//    private static final int Sec_Code = 11;

    private int continuousAuction = 93000;

    public SSERealRateGenerator(String input, String brokers) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "kafka.SSE.SSEPartitioner");
        producer = new KafkaProducer<String, String>(props);

    }

    public void generate(String FILE, int REPEAT, int INTERVAL, int PARTITIONSIZE) throws InterruptedException {

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
        int sleepCnt = 0;

        try {
            stream = new FileReader(FILE);
            br = new BufferedReader(stream);

            while ((sCurrentLine = br.readLine()) != null){
                String[] orderArr = sCurrentLine.split("\\|");
                if(orderArr.length >= 7) {
                    int curTime = Integer.parseInt(sCurrentLine.split("\\|")[Last_Upd_Time].replace(":", ""));
                    if (curTime >= continuousAuction)
                        break;
                }
            }
            

            Thread.sleep(60000);
            start = System.currentTimeMillis();

            while ((sCurrentLine = br.readLine()) != null) {

//                if (counter == 2) {
//                    Thread.sleep(600000);
//                }


                if (sCurrentLine.equals("CALLAUCTIONEND")) {
                    // dont let later process be affected
                    sleepCnt += 60000/INTERVAL;
                    System.out.println("output rate: " + counter + " per " + INTERVAL + "ms");
                    counter = 0;

                    Thread.sleep(30000);
                    for (int partition=0; partition<PARTITIONSIZE; partition++) {
                        ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, partition, String.valueOf(partition), sCurrentLine);
                        producer.send(newRecord);
                    }
                    Thread.sleep(30000);
                }

                if (sCurrentLine.equals("end")) {
                    sleepCnt++;
                    if (counter == 0) {
                        noRecSleepCnt++;
                        System.out.println("no record in this sleep !" + noRecSleepCnt);
                    }
                    System.out.println("output rate: " + counter + " per " + INTERVAL + "ms");
                    counter = 0;
                    cur = System.currentTimeMillis();
                   if (cur < sleepCnt*INTERVAL + start) {
                       sleep((sleepCnt*INTERVAL + start) - cur);
                   } else {
                        System.out.println("rate exceeds" + INTERVAL + "ms.");
                    }
//                    start = System.currentTimeMillis();
                }

                String[] orderArr = sCurrentLine.split("\\|");

                if (orderArr.length < 7) {
                    continue;
                }


//                String date = orderArr[2] + "  " + orderArr[3] + "," + orderArr[4].split("\\.")[1].substring(0,3);
//                System.out.println(getTime(date).getTime());

                for (int i=0; i< REPEAT; i++) {
//                    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, null, getTime(date).getTime(), sCurrentLine.split("\\|")[Sec_Code], sCurrentLine);
                    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, null, System.currentTimeMillis(), sCurrentLine.split("\\|")[Sec_Code], sCurrentLine);
                   // ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, sCurrentLine.split("\\|")[Sec_Code], sCurrentLine);
                    producer.send(newRecord);
                    counter++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
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
        String FILE = params.get("fp", "/home/drg/tools/SSE_data/sb-opening-50ms.txt");
        int REPEAT = params.getInt("repeat", 1);
        String BROKERS = params.get("host", "localhost:9092");
        int INTERVAL = params.getInt("interval", 1000);
        int PARTITIONSIZE = params.getInt("partition", 64);

        System.out.println(TOPIC + FILE + REPEAT + BROKERS + PARTITIONSIZE);

        new SSERealRateGenerator(TOPIC, BROKERS).generate(FILE, REPEAT, INTERVAL, PARTITIONSIZE);
    }

    private static Date getTime(String time) throws ParseException {
        return new SimpleDateFormat("yyyyMMdd HH:mm:ss,SSS").parse(time);
    }
}

