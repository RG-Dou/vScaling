package kafka.Nexmark;

import kafka.Nexmark.refactored.ZipfGenerator;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * SSE generaor
 */
public class KafkaPersonGenerator {

    private String TOPIC;

    private static KafkaProducer<Long, String> producer;

    private volatile boolean running = true;
    private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
    private long eventsCountSoFar = 0;
    private int rate;
    private int cycle;
    private int base;
    private long fixId = 10000;//DrG
    private ZipfGenerator zipfGenerator;

    public KafkaPersonGenerator(String input, String BROKERS, int rate, int cycle, int base) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERS);
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("partitioner.class", "generator.SSEPartitioner");
        producer = new KafkaProducer<Long, String>(props);
        TOPIC = input;
        this.rate = rate;
        this.cycle = cycle;
        this.base = base;
        zipfGenerator = new ZipfGenerator(10000, 1);
    }

    public void generate() throws InterruptedException {
        int epoch = 0;
        int count = 0;

        long emitStartTime = 0;
        int curRate = rate+base;

        while (running && eventsCountSoFar < 20_000_000) {

            emitStartTime = System.currentTimeMillis();

            if (count == 20) {
                // change input rate every 1 second.
                epoch++;
                System.out.println();
                curRate = changeRate(epoch) + base;
                System.out.println("epoch: " + epoch%cycle + " current rate is: " + curRate);
                count = 0;
            }

            for (int i = 0; i < Integer.valueOf(curRate/20); i++) {
                long nextId = nextId();
                Random rnd = new Random(nextId);

                // When, in event time, we should generate the event. Monotonic.
                long eventTimestamp =
                        config.timestampAndInterEventDelayUsForEvent(
                                config.nextEventNumber(eventsCountSoFar)).getKey();

//                ProducerRecord<Long, String> newRecord = new ProducerRecord<Long, String>(TOPIC, null , System.currentTimeMillis(), nextId,
//                        PersonGenerator.nextPerson(nextId, rnd, eventTimestamp, config).toString());
//                ProducerRecord<Long, String> newRecord = new ProducerRecord<Long, String>(TOPIC, nextId,
//                        PersonGenerator.nextPerson(nextId, rnd, eventTimestamp, config).toString());
                ProducerRecord<Long, String> newRecord = new ProducerRecord<Long, String>(TOPIC, zipfGenerator.next(),
                        PersonGenerator.nextPerson(nextId, rnd, eventTimestamp, config).toString());
//                producer.send(newRecord);
                //DrG

                for (int j = 0; j < 2; j ++) {
                    producer.send(newRecord);
                    eventsCountSoFar++;
                    i++;
                }
                i--;
//                for(int j  = 0; j < 1; j ++) {
//                    ProducerRecord<Long, String> newRecord1 = new ProducerRecord<Long, String>(TOPIC, fixId,
//                            PersonGenerator.nextPerson(fixId, rnd, eventTimestamp, config).toString());
//                    producer.send(newRecord1);
//                }

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

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
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
        String TOPIC = params.get("topic", "persons");
        int rate = params.getInt("rate", 1000);
        int cycle = params.getInt("cycle", 360);
        int base = params.getInt("base", 0);

        new KafkaPersonGenerator(TOPIC, BROKERS, rate, cycle, base).generate();
    }
}

