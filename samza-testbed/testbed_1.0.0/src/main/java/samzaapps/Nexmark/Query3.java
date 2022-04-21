package samzaapps.Nexmark;

import org.apache.commons.math3.random.RandomDataGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.codehaus.jackson.annotate.JsonProperty;
import samzaapps.Nexmark.serde.Auction;
import samzaapps.Nexmark.serde.Bid;
import samzaapps.Nexmark.serde.Person;
import org.apache.samza.util.CommandLine;
import joptsimple.OptionSet;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;


public class Query3 implements StreamApplication, Serializable {

    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final String BID_STREAM = "bids";
    private static final String PERSON_STREAM = "persons";
    private static final String AUCTION_STREAM = "auctions";
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

        KafkaOutputDescriptor<String> joinResultOutputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, new StringSerde());


        MessageStream<Person> persons = appDescriptor.getInputStream(personDescriptor);
        MessageStream<Auction> auctions = appDescriptor.getInputStream(auctionDescriptor);
        OutputStream<String> joinResults = appDescriptor.getOutputStream(joinResultOutputDescriptor);

        MessageStream<Person> repartitionedPersons =
                persons
                        .map(kv -> {
                            delay(3);
                            return kv;
                        })
                        .filter(person -> {
                            if (person.getState().equals("OR") || person.getState().equals("ID") || person.getState().equals("CA")) {
                                return true;
                            } else {
                                return false;
                            }
                        })
                        .partitionBy(ps -> String.valueOf(ps.getId()), ps -> ps, KVSerde.of(stringSerde, personSerde), "person")
                        .map(KV -> {
//                            System.out.println(KV);
                            return KV.getValue();
                        });


        MessageStream<Auction> repartitionedAuctions =
                auctions
                        .map(kv -> {
                            delay(3);
                            return kv;
                        })
                        .partitionBy(ac -> String.valueOf(ac.getSeller()), ac -> ac, KVSerde.of(stringSerde, auctionSerde), "auction")
                        .map(KV::getValue);

        JoinFunction<String, Auction, Person, String> joinFunction =
                new JoinFunction<String, Auction, Person, String>() {
                    @Override
                    public String apply(Auction auction, Person person) {
                        return new JoinResult(String.valueOf(person.getName()), String.valueOf(person.getCity()), String.valueOf(person.getState()), Long.valueOf(auction.getId())).toString();
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
                        stringSerde, auctionSerde, personSerde, Duration.ofSeconds(3), "join")
                .sendTo(joinResults);
    }


    public static void main(String[] args) {
        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config config = cmdLine.loadConfig(options);
        LocalApplicationRunner runner = new LocalApplicationRunner(new Query3(), config);
        runner.run();
        runner.waitForFinish();
    }

    static class JoinResult {
        public String name;
        public String city;
        public String state;
        public long auctionId;

        public JoinResult(String name, String city, String state, long auctionId) {
            this.name = name;
            this.city = city;
            this.state = state;
            this.auctionId = auctionId;
        }

        @Override
        public String toString() {
            return "joinResult: { name:" + name + ", city: " + city + ", state: " + state + ", auctionId: " + auctionId + "}";

        }
    }
    private void delay(int interval) {
        Double ranN = randomGen.nextGaussian(interval, 1);
//        ranN = ranN*1000;
//        ranN = ranN*1000;
//        long delay = ranN.intValue();
//        if (delay < 0) delay = 6000;
        long delay = interval * 100000;
        Long start = System.nanoTime();
        while (System.nanoTime() - start < delay) {
        }
    }
}
