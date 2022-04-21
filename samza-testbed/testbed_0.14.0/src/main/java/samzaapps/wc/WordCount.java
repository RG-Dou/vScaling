package samzaapps.wc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.*;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.Util;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordCount implements StreamApplication {
    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final String INPUT_STREAM_ID = "WordCount";
    private static final String OUTPUT_STREAM_ID = "word-count-output";

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));

        MessageStream<KV<String, String>> lines = graph.getInputStream(INPUT_STREAM_ID);
        OutputStream<KV<String, samzaapps.data.WordCount>> counts = graph.getOutputStream(OUTPUT_STREAM_ID, KVSerde.of(new StringSerde(), new JsonSerdeV2<>(samzaapps.data.WordCount.class)));



        lines
                .map(kv->kv.getValue())
                .flatMap(s -> Arrays.asList(s.split("\\|")))
                .map(v -> KV.of(String.valueOf(System.currentTimeMillis()), v))
                .window(Windows.keyedSessionWindow(
                        w -> w.getValue(), Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
                        new StringSerde(), new IntegerSerde()), "count")
//                .map(windowPane ->
//                        KV.of(windowPane.getKey().getKey(),
//                                windowPane.getKey().getKey() + ": " + windowPane.getMessage().toString()))
                .map(windowPane -> {
                    String word = windowPane.getKey().getKey();
                    int count = windowPane.getMessage();
                    return KV.of(word, new samzaapps.data.WordCount(word, count));
                })
                .sendTo(counts);
    }

    public static void main(String[] args) {
        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config config = cmdLine.loadConfig(options);
        LocalApplicationRunner runner = new LocalApplicationRunner(config);
        runner.run(new WordCount());
        runner.waitForFinish();
    }
}
