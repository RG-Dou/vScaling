package samzaapps.stock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;

public class StockXact implements StreamApplication {
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    Map<String, Float> stockAvgPriceMap = new HashMap<String, Float>();

    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final String INPUT_STREAM_ID = "stock_order";
    private static final String INPUT_STREAM_ID_2 = "stock_buyer";
    private static final String OUTPUT_STREAM_ID_2 = "stock_buyer";
    private static final String INPUT_STREAM_ID_3 = "stock_seller";
    private static final String OUTPUT_STREAM_ID_3 = "stock_seller";
    private static final String OUTPUT_STREAM_ID = "stock_analysis_result";

    @Override
    public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {
        Serde serde = KVSerde.of(new StringSerde(), new StringSerde());
        StringSerde stringSerde = new StringSerde();

        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        KafkaInputDescriptor<KV<String, String>> inputDescriptor =
                kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID,
                        serde);

        KafkaInputDescriptor<KV<String, String>> inputDescriptor2 =
                kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID_2,
                        serde);

        KafkaInputDescriptor<KV<String, String>> inputDescriptor3 =
                kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID_3,
                        serde);

        KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
                        serde);

        KafkaOutputDescriptor<KV<String, String>> outputDescriptor2 =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID_2,
                        serde);

        KafkaOutputDescriptor<KV<String, String>> outputDescriptor3 =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID_3,
                        serde);

        MessageStream<KV<String, String>> inputStream = streamApplicationDescriptor.getInputStream(inputDescriptor);
        MessageStream<KV<String, String>> imStreamIn2 = streamApplicationDescriptor.getInputStream(inputDescriptor2);
        OutputStream<KV<String, String>> imStreamOut2 = streamApplicationDescriptor.getOutputStream(outputDescriptor2);
        MessageStream<KV<String, String>> imStreamIn3 = streamApplicationDescriptor.getInputStream(inputDescriptor3);
        OutputStream<KV<String, String>> imStreamOut3 = streamApplicationDescriptor.getOutputStream(outputDescriptor3);
        OutputStream<KV<String, String>> outputStream = streamApplicationDescriptor.getOutputStream(outputDescriptor);

        inputStream
            .map(order -> {
                String[] orderArr = order.getValue().split("\\|");
                return new KV(order.getKey(), orderArr);
            })
            .map((KV m) -> getBuyer(m))
            .sendTo(imStreamOut2);

        inputStream
                .map(order -> {
                    String[] orderArr = order.getValue().split("\\|");
                    return new KV(order.getKey(), orderArr);
                })
                .map((KV m) -> getBuyer(m))
                .sendTo(imStreamOut3);

        MessageStream<String> buyer = imStreamIn2.map(KV::getValue);
        MessageStream<String> seller = imStreamIn3.map(KV::getValue);

        buyer
            .join(seller, new JoinFunction<String, String, String, String>(){
                @Override
                public String apply(String buyerMsg, String sellerMsg) {
                    return "test";
                }

                @Override
                public String getFirstKey(String buyerMsg) {
                    return "test";
                }
                @Override
                public String getSecondKey(String sellerMsg) {
                    return "test";
                }
            }, serde, serde, serde, Duration.ofMinutes(3), "join")
            .sendTo(outputStream);
    }

    private KV<String, String> getBuyer(KV m) {
        String[] orderArr = (String[]) m.getValue();
        if (!stockAvgPriceMap.containsKey(orderArr[Sec_Code])) {
            stockAvgPriceMap.put(orderArr[Sec_Code], (float) 0);
        }
        float sum = stockAvgPriceMap.get(orderArr[Sec_Code]) + Float.parseFloat(orderArr[Order_Price]);
        stockAvgPriceMap.put(orderArr[Sec_Code], sum);
        return new KV(m.getKey(), String.valueOf(sum));
    }
}
