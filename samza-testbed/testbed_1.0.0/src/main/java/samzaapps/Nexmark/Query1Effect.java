package samzaapps.Nexmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

import joptsimple.OptionSet;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.*;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.Util;
import samzaapps.Nexmark.serde.Auction;
import samzaapps.Nexmark.serde.Bid;
import samzaapps.Nexmark.serde.Person;


public class Query1Effect implements StreamApplication, Serializable {

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
                .map(kv -> {
                    delay(1);
                    return kv;
                })
                .map(bid -> KV.of(String.valueOf(bid.getAuction()),
                String.valueOf(bid.getAuction()) + dollarToEuro(bid.getPrice(), 0.82F) + bid.getBidder() + bid.getDateTime()))
                .sendTo(results);
    }

    private static long dollarToEuro(long dollarPrice, float rate) {
        return (long) (rate*dollarPrice);
    }

    private void delay(long interval) {
//        Double ranN = randomGen.nextGaussian(interval, 1);
//        ranN = ranN*1000;
//        ranN = ranN*1000;
//        long delay = ranN.intValue();
//        if (delay < 0) delay = 6000;
        long delay = interval*1000000;
        Long start = System.nanoTime();
        while (System.nanoTime() - start < delay) {}
    }
}
