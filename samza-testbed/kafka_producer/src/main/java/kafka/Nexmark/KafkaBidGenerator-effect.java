package kafka.Nexmark;

import kafka.Nexmark.refactored.ZipfGenerator;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * SSE generaor
 */
public class KafkaBidGenerator {

    private String TOPIC;

    private static KafkaProducer<Long, String> producer;

    private volatile boolean running = true;
    private final GeneratorConfig config;
    private long eventsCountSoFar = 0;
    private int rate;
    private int cycle;
    private int base;
    private ZipfGenerator zipfGenerator;

    public KafkaBidGenerator(String input, String BROKERS, int rate, int cycle, int base) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERS);
        props.put("client.id", "Bid");
        props.put("batch.size", "163840");
        props.put("linger.ms", "10");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //drg
//        props.put("partitioner.class", "kafka.Nexmark.BidPartitioner");
//        props.put("partitioner.class", "generator.SSEPartitioner");
        producer = new KafkaProducer<Long, String>(props);
        TOPIC = input;
        this.rate = rate;
        this.cycle = cycle;
        this.base = base;
        NexmarkConfiguration nexconfig = NexmarkConfiguration.DEFAULT;
        nexconfig.hotBiddersRatio=1;
        nexconfig.hotAuctionRatio=1;
        nexconfig.hotSellersRatio=1;
        nexconfig.numInFlightAuctions=1;
        nexconfig.numEventGenerators=1;
        nexconfig.avgPersonByteSize=100;
        config = new GeneratorConfig(nexconfig, 1, 1000L, 0, 1);

        zipfGenerator = new ZipfGenerator(10000, 1);
    }

    public void generate() throws InterruptedException {
        int epoch = 0;
        int count = 0;
        int tupleCounter = 0;

        long emitStartTime = System.currentTimeMillis();

        int curRate = rate + base;

        System.out.println("++++++enter warm up");
        warmup();
        System.out.println("++++++end warm up");
        long start = System.currentTimeMillis();

        while (running) {

            emitStartTime = System.currentTimeMillis();

            if (emitStartTime >= start + (epoch + 1) * 1000) {
                // change input rate every 1 second.
                //epoch++;
                epoch = (int)((emitStartTime - start)/1000);
                curRate = base + changeRate(epoch);
                System.out.println("bid epoch: " + epoch%cycle + " current rate is: " + curRate);
                System.out.println("bid epoch: " + epoch + " actual current rate is: " + tupleCounter);
                tupleCounter = 0;
                count = 0;
            }

            for (int i = 0; i < Integer.valueOf(curRate/20); i++) {
                sendMessages();
//                tupleCounter++;
                tupleCounter += 13;
                i+=12;
                eventsCountSoFar++;
            }

            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000/20) {
                Thread.sleep(1000/20 - emitTime);
            }
            count++;
        }
        producer.close();
    }

    private void warmup() throws InterruptedException {
        long emitStartTime = 0;
        long warmupStart = System.currentTimeMillis();
        int curRate = rate + base;
        while (System.currentTimeMillis()-warmupStart < 120000) {
            emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < Integer.valueOf(curRate/20); i++) {
                sendMessages();
                eventsCountSoFar++;
            }

            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000/20) {
                Thread.sleep(1000/20 - emitTime);
            }
        }
    }

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
    }

    private int changeRate(int epoch) {
        double sineValue = Math.sin(Math.toRadians(epoch*360/cycle)) + 1;
//        System.out.println(sineValue);

        Double curRate = (sineValue * rate);
        return curRate.intValue();
    }

    private void sendMessages(){
        long nextId = nextId();
        Random rnd = new Random(nextId);

        // When, in event time, we should generate the event. Monotonic.
        long eventTimestamp =
                config.timestampAndInterEventDelayUsForEvent(
                        config.nextEventNumber(eventsCountSoFar)).getKey();

//                ProducerRecord<Long, String> newRecord = new ProducerRecord<Long, String>(TOPIC, null, System.currentTimeMillis(), nextId,
//                        BidGenerator.nextBid(nextId, rnd, eventTimestamp, config).toString());
//        ProducerRecord<Long, String> newRecord = new ProducerRecord<Long, String>(TOPIC, nextId,
//                BidGenerator.nextBid(nextId, rnd, eventTimestamp, config).toString());
        long key = zipfGenerator.next();
        ProducerRecord<Long, String> newRecord = new ProducerRecord<Long, String>(TOPIC, key,
                BidGenerator.nextBid(nextId, rnd, eventTimestamp, config).toString());

        producer.send(newRecord);

//        for q1, q2, q3, i < 9
//        for q5, i < 14
//        for (int i = 0; i < 9; i ++){
        for (int i = 0; i < 12; i ++){
            producer.send(newRecord);
            eventsCountSoFar++;
        }

//        for (int j = 0; j < 10; j ++) {
//            ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, j*2,  j*2l,
//                    BidGenerator.nextBid(j*2l, rnd, eventTimestamp, config).toString());
//            producer.send(newRecord1);
//        }
//        Random rnd1 = new Random(nextId);
//        if (rnd1.nextInt(4) == 0){
//            ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, 2l,
//                    BidGenerator.nextBid(2l, rnd, eventTimestamp, config).toString());
//            producer.send(newRecord1);
//        }

//                Random rnd1 = new Random(nextId);
		if(System.currentTimeMillis() - start > 500000 && System.currentTimeMillis() - start < 1100000){
			if (rnd1.nextInt(8) == 0){
		                ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, 2l,
        	                BidGenerator.nextBid(2l, rnd, eventTimestamp, config).toString());
                		producer.send(newRecord1);
			}

			if (rnd1.nextInt(8) == 0){
		                ProducerRecord<Long, String> newRecord2 = new ProducerRecord<Long, String>(TOPIC, 7l,
        	                BidGenerator.nextBid(7l, rnd, eventTimestamp, config).toString());
                		producer.send(newRecord2);
			}
		}
		if(System.currentTimeMillis() - start > 800000 && System.currentTimeMillis() - start < 1100000){
			if (rnd1.nextInt(8) == 0){
	                	ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, 2l,
                        	BidGenerator.nextBid(2l, rnd, eventTimestamp, config).toString());
                		producer.send(newRecord1);
			}
			if (rnd1.nextInt(4) == 0){
	                	ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, 7l,
                        	BidGenerator.nextBid(7l, rnd, eventTimestamp, config).toString());
                		producer.send(newRecord1);
			}
//			if (rnd1.nextInt(8) < 3){
//	                	ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, 1l,
//                        	BidGenerator.nextBid(1l, rnd, eventTimestamp, config).toString());
//                		producer.send(newRecord1);
//			}
		}
    }


    public static void main(String[] args) throws InterruptedException {
        final ParameterTool params = ParameterTool.fromArgs(args);

        String BROKERS = params.get("host", "localhost:9092");
        String TOPIC = params.get("topic", "bids");
        int rate = params.getInt("rate", 1000);
        int cycle = params.getInt("cycle", 360);
        int base = params.getInt("base", 0);

        new KafkaBidGenerator(TOPIC, BROKERS, rate, cycle, base).generate();
    }
}

