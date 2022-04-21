package samzaapps.Nexmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import samzaapps.Nexmark.serde.Auction;
import samzaapps.Nexmark.serde.Bid;
import samzaapps.Nexmark.serde.Person;

import java.util.List;
import java.util.Map;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class Query22 implements StreamApplication {

    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final String BID_STREAM = "bids";
    private static final String OUTPUT_STREAM_ID = "results";
    private RandomDataGenerator randomGen = new RandomDataGenerator();

    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
        Serde serde = KVSerde.of(new StringSerde(), new StringSerde());

        StringSerde stringSerde = new StringSerde();
        JsonSerdeV2<Person> personSerde = new JsonSerdeV2<>(Person.class);
        JsonSerdeV2<Bid> bidSerde = new JsonSerdeV2<>(Bid.class);
        JsonSerdeV2<Auction> auctionSerde = new JsonSerdeV2<>(Auction.class);

        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        KafkaInputDescriptor<Bid> inputDescriptor =
                kafkaSystemDescriptor.getInputDescriptor(BID_STREAM,
                        bidSerde);


        KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
                        serde);


        MessageStream<Bid> bids = appDescriptor.getInputStream(inputDescriptor);
        OutputStream<KV<String, String>> results = appDescriptor.getOutputStream(outputDescriptor);

        bids
                .map(bid -> {
                    delay(200000, 1);
                    return bid;
                })
                .filter(bid -> {
                    if (bid.getAuction() % 1007 == 0 || bid.getAuction() % 1020 == 0
                            || bid.getAuction() % 2001 == 0 || bid.getAuction() % 2019 == 0 || bid.getAuction() % 2087 == 0) {
                        return true;
                    } else {
                        return false;
                    }
                })
                .map(bid -> KV.of(String.valueOf(bid.getAuction()), String.valueOf(bid.getPrice())))
                .sendTo(results);
    }


    private void delay(int interval, double limit) {
        int actInt = (int) (interval/limit);
        long start = System.nanoTime();
        while (System.nanoTime() - start < actInt) {}
    }

    private String getHost() {
        String hostname = null;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return hostname;
    }

}
