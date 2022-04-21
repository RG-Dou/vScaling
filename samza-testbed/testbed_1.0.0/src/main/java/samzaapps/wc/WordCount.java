package samzaapps.wc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.*;

import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.Util;

public class WordCount implements StreamApplication {
    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final String INPUT_STREAM_ID = "WordCount";
    private static final String OUTPUT_STREAM_ID = "word-count-output";

    @Override
    public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {
        Serde serde = KVSerde.of(new StringSerde(), new StringSerde());

        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
            .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
            .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
            .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        KafkaInputDescriptor<KV<String, String>> inputDescriptor =
                kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID,
                        serde);


        KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
                        serde);


        MessageStream<KV<String, String>> lines = streamApplicationDescriptor.getInputStream(inputDescriptor);
        OutputStream<KV<String, String>> counts = streamApplicationDescriptor.getOutputStream(outputDescriptor);


        lines
                .map(kv->kv.getValue())
                .flatMap(s -> Arrays.asList(s.split(" ")))
                .map(v -> KV.of(String.valueOf(System.currentTimeMillis()), v))
                .window(Windows.keyedSessionWindow(
                        w -> w.getValue(), Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
                        new StringSerde(), new IntegerSerde()), "count")
                .map(windowPane ->
                        KV.of(windowPane.getKey().getKey(),
                                windowPane.getKey().getKey() + ": " + windowPane.getMessage().toString()))
                .sendTo(counts);
    }

//    public static void main(String[] args) {
//        CommandLine cmdLine = new CommandLine();
//        OptionSet options = cmdLine.parser().parse(args);
//        Config config = cmdLine.loadConfig(options);
//        LocalApplicationRunner runner = new LocalApplicationRunner(new WordCount(), config);
//        runner.run();
//        runner.waitForFinish();
//    }
}
