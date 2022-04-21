package kafka.AD;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static java.lang.Thread.sleep;

/**
 * SSE generaor
 */
public class ReportGenerator {

    private String TOPIC;

    private static KafkaProducer<Long, String> producer;

    private volatile boolean running = true;
    private long eventsCountSoFar = 0;
    private final String fileName;
    private int keySize;
    private final List<Integer> hotKeys;
    private int INTERVAL;

    private static final int typeNum = 0;
    private static final int timeNum = 1;
    private static final int vIdNum = 2;

    public ReportGenerator(String input, String BROKERS, String fileName, int INTERVAL, int keySize) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERS);
        props.put("client.id", "PositionReport");
        props.put("batch.size", "163840");
        props.put("linger.ms", "10");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "kafka.AD.ReportPartitioner");
//        props.put("partitioner.class", "generator.SSEPartitioner");
        producer = new KafkaProducer<Long, String>(props);
        TOPIC = input;
        this.fileName = fileName;
        this.keySize = keySize;
        this.INTERVAL = INTERVAL;
//        hotKeys = Arrays.asList(0,2,4,6,8,10,12,14,16,18,21,23,25,27,29,31,33,35,37,39,41,43,45);
        hotKeys = Arrays.asList(0,2,4,6,8,10,12,14,16,18);
    }

    public void generate() throws InterruptedException {

        String sCurrentLine;
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        long cur = 0;
        long start = 0;
        int counter = 0;

        try {
            stream = new FileReader(fileName);
            br = new BufferedReader(stream);

            Thread.sleep(60000);
            start = System.currentTimeMillis();

            ArrayList<String> batch = new ArrayList<>();
            Long currentTime = 0L;
            while ((sCurrentLine = br.readLine()) != null) {
                String[] items = sCurrentLine.split(",");
                if (items[typeNum].equals("0")){
                    Long timeEpoch = Long.parseLong(items[timeNum]);
                    if (timeEpoch.equals(currentTime)){
                        batch.add(sCurrentLine);
                    }
                    else {
                        int numBatch = sendBatch(batch);
                        counter += numBatch;
                        currentTime = timeEpoch;

                        cur = System.currentTimeMillis();
                        if (cur < timeEpoch*INTERVAL + start) {
                            sleep((timeEpoch*INTERVAL + start) - cur);
                        } else {
                            System.out.println("rate exceeds" + INTERVAL + "ms.");
                        }

                        System.out.println("output rate: " + numBatch + " per " + INTERVAL + "ms");

                        batch.clear();
                        batch.add(sCurrentLine);
                    }
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

    private int sendBatch(ArrayList<String> batch){
        int numTuples = 0;
        for (String tuple : batch){
            Long key = Long.parseLong(tuple.split(",")[vIdNum]);
            ProducerRecord<Long, String> newRecord = new ProducerRecord<>(TOPIC, null, System.currentTimeMillis(), key, tuple);
            producer.send(newRecord);
            numTuples ++;
        }
        return numTuples;
    }

    public static void main(String[] args) throws InterruptedException {
        final ParameterTool params = ParameterTool.fromArgs(args);

        String BROKERS = params.get("host", "localhost:9092");
        String TOPIC = params.get("topic", "position-reports");
        int INTERVAL = params.getInt("interval", 1000);
//        int cycle = params.getInt("cycle", 360);
//        int base = params.getInt("base", 0);
        String fileName = params.get("fileName", "/home/drg/");
        int keySize = params.getInt("keySize", 16);

        new ReportGenerator(TOPIC, BROKERS, fileName, INTERVAL, keySize).generate();
    }
}

