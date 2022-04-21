package kafka.AD;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * SSE generaor
 */
public class CarReportGenerator {

    private String TOPIC;

    private static KafkaProducer<Long, String> producer;

    private volatile boolean running = true;
    private long eventsCountSoFar = 0;
    private int rate;
    private int cycle;
    private int base;
    private int keySize;
    private final List<Integer> hotKeys;
    private final int hotRatio = 50;

    public CarReportGenerator(String input, String BROKERS, int rate, int cycle, int base, int keySize) {
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
        this.rate = rate;
        this.cycle = cycle;
        this.base = base;
        this.keySize = keySize;
//        hotKeys = Arrays.asList(0,2,4,6,8,10,12,14,16,18,21,23,25,27,29,31,33,35,37,39,41,43,45);
        hotKeys = Arrays.asList(0,2,4,6,8,10,12,14,16,18);
    }

    public void generate() throws InterruptedException {
        int epoch = 0;
        int tupleCounter = 0;

        long emitStartTime = System.currentTimeMillis();

        int curRate = rate + base;

        System.out.println("++++++enter warm up");
        warmup();
        System.out.println("++++++end warm up");
        long start = System.currentTimeMillis();

        while (running) {

            emitStartTime = System.currentTimeMillis();

            if (emitStartTime >= start + (epoch + 1) * 1000L) {
                epoch = (int)((emitStartTime - start)/1000);
                curRate = base + changeRate(epoch);
                System.out.println("report epoch: " + epoch%cycle + " current rate is: " + curRate);
                System.out.println("report epoch: " + epoch + " actual current rate is: " + tupleCounter);
                tupleCounter = 0;
            }

            for (int i = 0; i < curRate / 20; i++) {
                int vehicleId = new Random().nextInt(keySize);
//                int rndInt = new Random().nextInt(hotRatio);
//                if (!hotKeys.contains(vehicleId) && rndInt > 0){
//                    continue;
//                }
                String report = generateReports(vehicleId);

                ProducerRecord<Long, String> newRecord = new ProducerRecord<Long, String>(TOPIC, vehicleId, (long) vehicleId, report);
                producer.send(newRecord);
//                for (int j = 0; j < 2; j ++){
//                    vehicleId = j * 2;
//                    report = generateReports(vehicleId);
//                    ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, (long) vehicleId, report);
//                    producer.send(newRecord1);
//                }
//                int rndInt = new Random().nextInt(2);
//                if (rndInt == 0){
//                    vehicleId = 2 * 2;
//                    report = generateReports(vehicleId);
//                    ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, (long) vehicleId, report);
//                    producer.send(newRecord1);
//                }
                tupleCounter++;
                eventsCountSoFar++;
            }

            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000/20) {
                Thread.sleep(1000/20 - emitTime);
            }
        }
        producer.close();
    }

    private void warmup() throws InterruptedException {
        long emitStartTime = 0;
        long warmupStart = System.currentTimeMillis();
        int curRate = rate + base;
        while (System.currentTimeMillis()-warmupStart < 600000) {
            emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < curRate / 20; i++) {
                int vehicleId = new Random().nextInt(keySize);
//                int rndInt = new Random().nextInt(3);
//                if (rndInt > 0){
//                    continue;
//                }
//                if (!hotKeys.contains(vehicleId)){
//                    continue;
//                }
                String report = generateReports(vehicleId);

                ProducerRecord<Long, String> newRecord = new ProducerRecord<Long, String>(TOPIC, (long) vehicleId, report);
                producer.send(newRecord);

//                for (int j = 0; j < 5; j ++){
//                    int rndInt = new Random().nextInt(hotKeys.size());
//                    vehicleId = hotKeys.get(rndInt);
//                    report = generateReports(vehicleId);
//                    ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, (long) vehicleId, report);
//                    producer.send(newRecord1);
//                }
//                int rndInt = new Random().nextInt(2);
//                if (rndInt == 0){
////                    vehicleId = 2 * 2;
////                    report = generateReports(vehicleId);
////                    ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, (long) vehicleId, report);
////                    producer.send(newRecord1);
////                }
////                else {
//                    vehicleId = 1;
//                    report = generateReports(vehicleId);
//                    ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, (long) vehicleId, report);
//                    producer.send(newRecord1);
//                }

                eventsCountSoFar++;
            }

            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000/20) {
                Thread.sleep(1000/20 - emitTime);
            }
        }
    }

    private String generateReports(int vehicleId){
        Random rnd = new Random();
        long time = System.currentTimeMillis();
        long position = rnd.nextInt(10000);
        double speed = rnd.nextDouble()*100;
        if (rnd.nextInt(2) == 0)
            speed = 0.0;
        return time + ";" + vehicleId + ";" + position + ";" + speed;
    }

    private int changeRate(int epoch) {
        double sineValue = Math.sin(Math.toRadians(epoch*360/cycle)) + 1;
        System.out.println(sineValue);
        Double curRate = (sineValue * rate);
        return curRate.intValue();
    }

    public static void main(String[] args) throws InterruptedException {
        final ParameterTool params = ParameterTool.fromArgs(args);

        String BROKERS = params.get("host", "localhost:9092");
        String TOPIC = params.get("topic", "position-reports");
        int rate = params.getInt("rate", 1000);
        int cycle = params.getInt("cycle", 360);
        int base = params.getInt("base", 0);
        int keySize = params.getInt("keySize", 16);

        new CarReportGenerator(TOPIC, BROKERS, rate, cycle, base, keySize).generate();
    }
}

