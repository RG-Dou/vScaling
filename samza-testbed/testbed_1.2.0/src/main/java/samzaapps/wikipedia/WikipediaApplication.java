/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package samzaapps.wikipedia;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.context.Context;
import org.apache.samza.context.TaskContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.util.*;


/**
 *
 * The only functional difference is the lack of "wikipedia-raw" and "wikipedia-edits"
 * streams to connect the operators, as they are not needed with the fluent API.
 *
 * The application processes Wikipedia events in the following steps:
 * <ul>
 *   <li>Merge wikipedia, wiktionary, and wikinews events into one stream</li>
 *   <li>Parse each event to a more structured format</li>
 *   <li>Aggregate some stats over a 10s window</li>
 *   <li>Format each window output for public consumption</li>
 *   <li>Send the window output to Kafka</li>
 * </ul>
 *
 * All of this application logic is defined in the {@link #describe(StreamApplicationDescriptor)} method, which
 * is invoked by the framework to load the application.
 */
public class WikipediaApplication implements StreamApplication, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(WikipediaApplication.class);

  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String WIKIPEDIA_CHANNEL = "wikipedia";;
  public static final String WIKINEWS_CHANNEL = "wikinews";
  public static final String WIKTIONARY_CHANNEL = "wiktionary";
  private static final String OUTPUT_CHANNEL = "wikipedia-stats";

  private static final String INPUT_STREAM_IM_1 = "im_stream_1";
  private static final String OUTPUT_STREAM_IM_1 = "im_stream_1";
  private static final String INPUT_STREAM_IM_2 = "im_stream_2";
  private static final String OUTPUT_STREAM_IM_2 = "im_stream_2";
  private static final String INPUT_STREAM_IM_3 = "im_stream_3";
  private static final String OUTPUT_STREAM_IM_3 = "im_stream_3";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {

    StringSerde stringSerde = new StringSerde();
    JsonSerdeV2<WikipediaEvent> wikipediaSerde = new JsonSerdeV2<>(WikipediaEvent.class);

    Serde serde = KVSerde.of(stringSerde, wikipediaSerde);

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
            .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
            .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
            .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    Duration windowDuration =
        appDescriptor.getConfig().containsKey("deploy.test") ? Duration.ofMillis(10) : Duration.ofSeconds(10);

    KafkaInputDescriptor<WikipediaEvent> wikipediaInputDescriptor =
            kafkaSystemDescriptor.getInputDescriptor(WIKIPEDIA_CHANNEL, wikipediaSerde);

    KafkaInputDescriptor<WikipediaEvent> wikinewsInputDescriptor =
            kafkaSystemDescriptor.getInputDescriptor(WIKINEWS_CHANNEL, wikipediaSerde);

    KafkaInputDescriptor<WikipediaEvent> wiktionaryInputDescriptor =
            kafkaSystemDescriptor.getInputDescriptor(WIKTIONARY_CHANNEL, wikipediaSerde);

    MessageStream<WikipediaEvent> wikipediaEvents = appDescriptor.getInputStream(wikipediaInputDescriptor);
    MessageStream<WikipediaEvent> wikinewsEvents = appDescriptor.getInputStream(wikinewsInputDescriptor);
    MessageStream<WikipediaEvent> wiktionaryEvents = appDescriptor.getInputStream(wiktionaryInputDescriptor);

    KafkaOutputDescriptor<WikipediaStatsOutput> statsOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_CHANNEL, new JsonSerdeV2<>(WikipediaStatsOutput.class));
    OutputStream<WikipediaStatsOutput> wikipediaStats = appDescriptor.getOutputStream(statsOutputDescriptor);

    KafkaInputDescriptor<KV<String, WikipediaEvent>> inputDescriptor1 =
            kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_IM_1, serde);

    KafkaOutputDescriptor<KV<String, WikipediaEvent>> outputDescriptor1 =
            kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_IM_1, serde);

    KafkaInputDescriptor<KV<String, WikipediaEvent>> inputDescriptor2 =
            kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_IM_2, serde);

    KafkaOutputDescriptor<KV<String, WikipediaEvent>> outputDescriptor2 =
            kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_IM_2, serde);

    KafkaInputDescriptor<KV<String, WikipediaEvent>> inputDescriptor3 =
            kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_IM_3, serde);

    KafkaOutputDescriptor<KV<String, WikipediaEvent>> outputDescriptor3 =
            kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_IM_3, serde);


    MessageStream<KV<String, WikipediaEvent>> imStreamIn1 = appDescriptor.getInputStream(inputDescriptor1);
    OutputStream<KV<String, WikipediaEvent>> imStreamOut1 = appDescriptor.getOutputStream(outputDescriptor1);
    MessageStream<KV<String, WikipediaEvent>> imStreamIn2 = appDescriptor.getInputStream(inputDescriptor2);
    OutputStream<KV<String, WikipediaEvent>> imStreamOut2 = appDescriptor.getOutputStream(outputDescriptor2);
    MessageStream<KV<String, WikipediaEvent>> imStreamIn3 = appDescriptor.getInputStream(inputDescriptor3);
    OutputStream<KV<String, WikipediaEvent>> imStreamOut3 = appDescriptor.getOutputStream(outputDescriptor3);

    wikipediaEvents.map(this::preprocess).sendTo(imStreamOut1);
    wikinewsEvents.map(this::preprocess).sendTo(imStreamOut2);
    wiktionaryEvents.map(this::preprocess).sendTo(imStreamOut3);

    // Merge inputs
    MessageStream<WikipediaEvent> allWikipediaEvents =
            MessageStream.mergeAll(ImmutableList.of(
                    imStreamIn1.map(KV::getValue),
                    imStreamIn2.map(KV::getValue),
                    imStreamIn3.map(KV::getValue)));

//      MessageStream<WikipediaEvent> allWikipediaEvents =
//             MessageStream.mergeAll(ImmutableList.of(
//                      wikipediaEvents.map(this::preprocess).map(KV::getValue),
//                      wikinewsEvents.map(this::preprocess).map(KV::getValue),
//                      wiktionaryEvents.map(this::preprocess).map(KV::getValue)));

    // Parse, update stats, prepare output, and send
    allWikipediaEvents
        .map(kv -> {
          return WikipediaParser.parseEvent(kv);
        })
        .window(Windows.tumblingWindow(windowDuration,
            WikipediaStats::new, new WikipediaStatsAggregator(), WikipediaStats.serde()), "statsWindow")
        .map(this::formatOutput)
        .sendTo(wikipediaStats);
  }

  /**
   * Updates the windowed and total stats based on each "edit" event.
   *
   * Uses a KeyValueStore to persist a total edit count across restarts.
   */
  private static class WikipediaStatsAggregator implements FoldLeftFunction<Map<String, Object>, WikipediaStats> {
    private static final String EDIT_COUNT_KEY = "count-edits-all-time";

    private transient KeyValueStore<String, Integer> store;

    // Example metric. Running counter of the number of repeat edits of the same title within a single window.
    private transient Counter repeatEdits;

    /**
     * {@inheritDoc}
     * Override {@link org.apache.samza.operators.functions.InitableFunction#init(Context)} to
     * get a KeyValueStore for persistence and the MetricsRegistry for metrics.
     */
    @Override
    public void init(Context context) {
      TaskContext taskContext = context.getTaskContext();
      store = (KeyValueStore<String, Integer>) taskContext.getStore("wikipedia-stats");
      repeatEdits = taskContext.getTaskMetricsRegistry().newCounter("edit-counters", "repeat-edits");
    }

    @Override
    public WikipediaStats apply(Map<String, Object> edit, WikipediaStats stats) {

      RandomDataGenerator messageGenerator = new RandomDataGenerator();
      Double number = messageGenerator.nextGaussian(3, 1);
      int delay = number.intValue();
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < delay + 1){}

//    long start = System. nanoTime();
//    while (System.nanoTime() - start < (delay*100000 + 1000000)){}

      // Update persisted total
      Integer editsAllTime = store.get(EDIT_COUNT_KEY);
      if (editsAllTime == null) {
        editsAllTime = 0;
      }
      editsAllTime++;
      store.put(EDIT_COUNT_KEY, editsAllTime);

      // Update window stats
      stats.edits++;
      stats.totalEdits = editsAllTime;
      stats.byteDiff += (Integer) edit.get("diff-bytes");
      boolean newTitle = stats.titles.add((String) edit.get("title"));

      Map<String, Boolean> flags = (Map<String, Boolean>) edit.get("flags");
      for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
        if (Boolean.TRUE.equals(flag.getValue())) {
          stats.counts.compute(flag.getKey(), (k, v) -> v == null ? 0 : v + 1);
        }
      }

      if (!newTitle) {
        repeatEdits.inc();
        LOG.info("Frequent edits for title: {}", edit.get("title"));
      }
      return stats;
    }
  }

  /**
   * Format the stats for output to Kafka.
   */
  private WikipediaStatsOutput formatOutput(WindowPane<Void, WikipediaStats> statsWindowPane) {
    WikipediaStats stats = statsWindowPane.getMessage();
    return new WikipediaStatsOutput(stats.edits, stats.totalEdits, stats.byteDiff, stats.titles.size(), stats.counts);
  }

  private KV<String, WikipediaEvent> preprocess(WikipediaEvent wikipediaEvent){
    RandomDataGenerator messageGenerator = new RandomDataGenerator();
    Double number = messageGenerator.nextGaussian(5, 1);
    int delay = number.intValue();
    long start = System. nanoTime();
    while (System.nanoTime() - start < (delay*100000 + 500000)){}
    return new KV(String.valueOf(start), wikipediaEvent);
  }

  /**
   * A few statistics about the incoming messages.
   */
  public static class WikipediaStats {
    // Windowed stats
    int edits = 0;
    int byteDiff = 0;
    Set<String> titles = new HashSet<>();
    Map<String, Integer> counts = new HashMap<>();

    // Total stats
    int totalEdits = 0;

    @Override
    public String toString() {
      return String.format("Stats {edits:%d, byteDiff:%d, titles:%s, counts:%s}", edits, byteDiff, titles, counts);
    }

    static Serde<WikipediaStats> serde() {
      return new WikipediaStatsSerde();
    }

    public static class WikipediaStatsSerde implements Serde<WikipediaStats> {
      @Override
      public WikipediaStats fromBytes(byte[] bytes) {
        try {
          ByteArrayInputStream bias = new ByteArrayInputStream(bytes);
          ObjectInputStream ois = new ObjectInputStream(bias);
          WikipediaStats stats = new WikipediaStats();
          stats.edits = ois.readInt();
          stats.byteDiff = ois.readInt();
          stats.titles = (Set<String>) ois.readObject();
          stats.counts = (Map<String, Integer>) ois.readObject();
          return stats;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public byte[] toBytes(WikipediaStats wikipediaStats) {
        try {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream dos = new ObjectOutputStream(baos);
          dos.writeInt(wikipediaStats.edits);
          dos.writeInt(wikipediaStats.byteDiff);
          dos.writeObject(wikipediaStats.titles);
          dos.writeObject(wikipediaStats.counts);
          return baos.toByteArray();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  static class WikipediaStatsOutput {
    public int edits;
    public int editsAllTime;
    public int bytesAdded;
    public int uniqueTitles;
    public Map<String, Integer> counts;

    public WikipediaStatsOutput(int edits, int editsAllTime, int bytesAdded, int uniqueTitles,
        Map<String, Integer> counts) {
      this.edits = edits;
      this.editsAllTime = editsAllTime;
      this.bytesAdded = bytesAdded;
      this.uniqueTitles = uniqueTitles;
      this.counts = counts;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WikipediaStatsOutput that = (WikipediaStatsOutput) o;
      return edits == that.edits && editsAllTime == that.editsAllTime && bytesAdded == that.bytesAdded
          && uniqueTitles == that.uniqueTitles && Objects.equals(counts, that.counts);
    }

    @Override
    public String toString() {
      return "WikipediaStatsOutput{" + "edits=" + edits + ", editsAllTime=" + editsAllTime + ", bytesAdded="
          + bytesAdded + ", uniqueTitles=" + uniqueTitles + ", counts=" + counts + '}';
    }

  }
}

