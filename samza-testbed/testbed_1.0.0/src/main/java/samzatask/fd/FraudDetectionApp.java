package samzatask.fd;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import joptsimple.OptionSet;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.util.CommandLine;
import samzatask.stock.StockAverageTask;
import samzatask.wc.WordCountApp;

import java.util.List;
import java.util.Map;

public class FraudDetectionApp implements TaskApplication {
    private static final String KAFKA_SYSTEM_NAME = "kafka";

    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final String INPUT_STREAM_ID = "fd_input";
    private static final String OUTPUT_STREAM_ID = "fd_output";

    @Override
    public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {
        Serde serde = KVSerde.of(new StringSerde(), new StringSerde());

        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        KafkaInputDescriptor<KV<String, String>> inputDescriptor =
                kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID,
                        serde);


        KafkaOutputDescriptor<String> outputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
                        new StringSerde());

        taskApplicationDescriptor.withDefaultSystem(kafkaSystemDescriptor);

        // Set the inputs
        taskApplicationDescriptor.withInputStream(inputDescriptor);

        // Set the output
        taskApplicationDescriptor.withOutputStream(outputDescriptor);

        // Set the task factory
        taskApplicationDescriptor.withTaskFactory((StreamTaskFactory) () -> new FraudDetectionTask());
    }

    public static void main(String[] args) {
        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config config = cmdLine.loadConfig(options);
        LocalApplicationRunner runner = new LocalApplicationRunner(new FraudDetectionApp(), config);
        runner.run();
        runner.waitForFinish();
    }
}
