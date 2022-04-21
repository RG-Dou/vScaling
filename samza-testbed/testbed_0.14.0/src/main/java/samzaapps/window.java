package samzaapps;

import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.KVSerde;

import samzaapps.data.WordCount;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.Util;
import org.apache.samza.config.MapConfig;
import org.apache.samza.runtime.LocalApplicationRunner;

import java.time.Duration;
import java.util.*;

// test
public class window implements StreamApplication {

    private static final String INPUT_TOPIC = "WordCount";
    private static final String OUTPUT_TOPIC = "windowStage";

    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));

        MessageStream<KV<String, String>> lines = graph.getInputStream(INPUT_TOPIC);
        OutputStream<KV<String, WordCount>> counts = graph.getOutputStream(OUTPUT_TOPIC, KVSerde.of(new StringSerde(), new JsonSerdeV2<>(WordCount.class)));


        MessageStream<KV<String, String>> firstMap = lines
            .map(kv -> {
                return KV.of(kv.getKey(), kv.getValue().split("\\|")[0]);
            });


        for (int i=0; i<10; i++) {
            firstMap = firstMap.map(kv -> {
                System.out.println(kv.value);
                return kv;
            });
        }

        MessageStream<String> secondMap = firstMap
                .map(kv -> {
                    System.out.println("stage2");
                    return kv.value;
                });

        MessageStream<WindowPane<String, Integer>> countWindow = secondMap
                .flatMap(s -> Arrays.asList(s.split("\\W+")))
                .window(Windows.keyedTumblingWindow(
                        w -> w, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
                        new StringSerde(), new IntegerSerde()), "count");

        countWindow
            .map(windowPane -> {
                String word = windowPane.getKey().getKey();
                int count = windowPane.getMessage();
                return KV.of(word, new WordCount(word, count));
            })
            .sendTo(counts);
    }

    public static void main(String[] args) {
        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config config = cmdLine.loadConfig(options);
        Config newConfig = Util.rewriteConfig(new MapConfig(config));
        LocalApplicationRunner runner = new LocalApplicationRunner(newConfig);
        runner.run(new window());
        runner.waitForFinish();
    }
}

