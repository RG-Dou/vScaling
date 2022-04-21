package samzaapps.Nexmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.*;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import samzaapps.Nexmark.serde.Auction;
import samzaapps.Nexmark.serde.Bid;
import samzaapps.Nexmark.serde.Person;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;


public class Query8 implements StreamApplication, Serializable {

    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final String BID_STREAM = "bids";
    private static final String AUCTION_STREAM = "auctions";
    private static final String PERSON_STREAM = "persons";
    private static final String OUTPUT_STREAM_ID = "results";
    private RandomDataGenerator randomGen = new RandomDataGenerator();

    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {

        Serde serde = KVSerde.of(new StringSerde(), new StringSerde());

        StringSerde stringSerde = new StringSerde();
        JsonSerdeV2<Person> personSerde = new JsonSerdeV2<>(Person.class);
        JsonSerdeV2<Bid> bidSerde = new JsonSerdeV2<>(Bid.class);
        JsonSerdeV2<Auction> auctionSerde = new JsonSerdeV2<>(Auction.class);
//        JsonSerdeV2<JoinResult> joinResultSerde = new JsonSerdeV2<>(JoinResult.class);

        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        KafkaInputDescriptor<Person> personDescriptor =
                kafkaSystemDescriptor.getInputDescriptor(PERSON_STREAM,
                        personSerde);

        KafkaInputDescriptor<Auction> auctionDescriptor =
                kafkaSystemDescriptor.getInputDescriptor(AUCTION_STREAM,
                        auctionSerde);

        KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
                        serde);

//        KafkaOutputDescriptor<JoinResult> joinResultOutputDescriptor =
//                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, joinResultSerde);
        KafkaOutputDescriptor<String> joinResultOutputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, new StringSerde());

        MessageStream<Person> persons = appDescriptor.getInputStream(personDescriptor);
        MessageStream<Auction> auctions = appDescriptor.getInputStream(auctionDescriptor);
        OutputStream<String> joinResults = appDescriptor.getOutputStream(joinResultOutputDescriptor);


        MessageStream<Person> repartitionedPersons =
                persons
//                        .partitionBy(ps -> String.valueOf(ps.getId()), ps -> ps, KVSerde.of(stringSerde, personSerde), "person")
                        .map(KV -> {
                            delay(4);
                            return KV;
                        });

        MessageStream<Auction> repartitionedAuctions =
                auctions
//                        .partitionBy(ac -> String.valueOf(ac.getSeller()), ac -> ac, KVSerde.of(stringSerde, auctionSerde), "auction")
                        .map(KV -> {
                            delay(4);
                            return KV;
                        });


        JoinFunction<String, Auction, Person, String> joinFunction =
                new JoinFunction<String, Auction, Person, String>() {
                    @Override
                    public String apply(Auction auction, Person person) {
                        return new JoinResult(String.valueOf(person.getId()), person.getName(), String.valueOf(auction.getReserve())).toString();
                    }

                    @Override
                    public String getFirstKey(Auction auction) {
                        return String.valueOf(auction.getSeller());
                    }

                    @Override
                    public String getSecondKey(Person person) {
                        return String.valueOf(person.getId());
                    }
                };

        repartitionedAuctions
                .join(repartitionedPersons, joinFunction,
                        stringSerde, auctionSerde, personSerde, Duration.ofSeconds(10), "join")
                .sendTo(joinResults);
    }

    static class JoinResult {
        public String personId;
        public String personName;
        public String auctionReserve;

        public JoinResult(String personId, String personName, String auctionReserve) {
            this.personId = personId;
            this.personName = personName;
            this.auctionReserve = auctionReserve;
        }

        @Override
        public String toString() {
            return "joinResult: { personId:" + personId + ", personName: " + personName + ", auctionReserve: " + auctionReserve + "}";

        }
    }

    private void delay(int interval) {
        Double ranN = randomGen.nextGaussian(interval, 1);
//        ranN = ranN*1000;
//        ranN = ranN*1000;
//        long delay = ranN.intValue();
//        if (delay < 0) delay = 6000;
        long delay = interval*100000;
        Long start = System.nanoTime();
        while (System.nanoTime() - start < delay) {}
    }
}
