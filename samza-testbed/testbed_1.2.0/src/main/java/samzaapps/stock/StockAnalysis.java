package samzaapps.stock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.*;
import java.util.*;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;

import java.util.Random;

public class StockAnalysis implements StreamApplication, Serializable {
    private static final int Trade_No = 0;
    private static final int Trade_Date = 1;
    private static final int Trade_Time = 2;
    private static final int Trade_Time_Dec =3;
    private static final int Order_Time = 4;
    private static final int Order_Time_Dec = 5;
    private static final int Order_No = 6;
    private static final int Trade_Price = 7;
    private static final int Trade_Amt = 8;
    private static final int Trade_Vol = 9;
    private static final int Sec_Code = 10;
    private static final int PBU_ID = 11;
    private static final int Acct_ID = 12;
    private static final int Trade_Dir= 13;
    private static final int Order_PrtFil_Code= 14;
    private static final int Tran_Type= 15;
    private static final int Trade_Type = 16;
    private static final int Proc_Type = 17;
    private static final int Order_Type = 18;
    private static final int Stat_PBU_ID = 19;
    private static final int Credit_Type = 20;


    Map<String, Float> stockAvgPriceMap = new HashMap<String, Float>();
    Map<String, Long> stockVolumeMap = new HashMap<String, Long>();

    private static final int kmeansKernels  = 20;

    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final int MINIMAL_PRICE = 10;

    private static final String INPUT_STREAM_ID = "stock_order";
    private static final String INPUT_STREAM_ID_2 = "im_stream";
    private static final String OUTPUT_STREAM_ID2 = "im_stream";
    private static final String OUTPUT_STREAM_ID = "stock_analysis";

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

        KafkaInputDescriptor<KV<String, String>> inputDescriptor2 =
                kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID_2,
                        serde);

        KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
                        serde);

        KafkaOutputDescriptor<KV<String, String>> outputDescriptor2 =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID2,
                        serde);

        MessageStream<KV<String, String>> inputStream = streamApplicationDescriptor.getInputStream(inputDescriptor);
        MessageStream<KV<String, String>> imStreamIn = streamApplicationDescriptor.getInputStream(inputDescriptor2);
        OutputStream<KV<String, String>> imStreamOut = streamApplicationDescriptor.getOutputStream(outputDescriptor2);
        OutputStream<KV<String, String>> outputStream = streamApplicationDescriptor.getOutputStream(outputDescriptor);

        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        // "transactor"
        inputStream
            .map(order -> {
                Double number = messageGenerator.nextGaussian(5, 1);
                int delay = number.intValue();
                long start = System. nanoTime();
                while (System.nanoTime() - start < (delay*30000)){}
                return order;
            })
//            .sendTo(imStreamOut);

//        //  movingAverage
//        imStreamIn
//            .map(order -> {
//                String[] orderArr = order.getValue().split("\\|");
//                return new KV(order.getKey(), orderArr);
//            })
//                .map(this::movingAverage)
//            .sendTo(outputStream);
//
//        // composite index
//        imStreamIn
//            .map(order -> {
//                String[] orderArr = order.getValue().split("\\|");
//                return new KV(order.getKey(), orderArr);
//            })
//            .map(this::compositeIndex)
//            .sendTo(outputStream);
//
//        // price alarm
//        imStreamIn
//            .map(order -> {
//                String[] orderArr = order.getValue().split("\\|");
//                return new KV(order.getKey(), orderArr);
//            })
//            .map(this::priceAlarm)
//            .sendTo(outputStream);

        // fraud detection
//        imStreamIn
            .map(order -> {
                Double number = messageGenerator.nextGaussian(10, 2);
                int delay = number.intValue();
                long start = System. nanoTime();
                while (System.nanoTime() - start < (delay*50000)){}
                String[] orderArr = order.getValue().split("\\|");
                return new KV(order.getKey(), orderArr);
            })
            .map(this::fraudDetection)
            .sendTo(outputStream);
    }

    private KV<String, String> movingAverage(KV m) {
        String[] orderArr = (String[]) m.getValue();
        if (!stockAvgPriceMap.containsKey(orderArr[Sec_Code])) {
            stockAvgPriceMap.put(orderArr[Sec_Code], (float) 0);
        }
        float sum = stockAvgPriceMap.get(orderArr[Sec_Code]) + Float.parseFloat(orderArr[Trade_Price]);
        stockAvgPriceMap.put(orderArr[Sec_Code], sum);
        return new KV(String.valueOf(m.getKey()), String.valueOf(sum));
    }

    private KV<String, String> compositeIndex(KV m) {
        String[] orderArr = (String[]) m.getValue();
        // user transaction statistics
        Long sum = stockVolumeMap.get(orderArr[Sec_Code]) + Long.parseLong(orderArr[Trade_Vol]);
        return new KV(m.getKey(), String.valueOf(sum));
    }

    private KV<String, String> priceAlarm(KV m) {
        String[] orderArr = (String[]) m.getValue();
        // user transaction statistics
        if (Float.parseFloat(orderArr[Trade_Price]) < MINIMAL_PRICE) {
            return new KV(m.getKey(), "low stock exchange price alarm!");
        } else {
            return new KV(m.getKey(), "normal transaction");
        }
    }

    private KV<String, String> fraudDetection(KV m) {
        String[] orderArr = (String[]) m.getValue();
        // design a kmeans algorithm here
        List<TradedOrder> centroids = new ArrayList<>();
        TradedOrder tradedOrder = new TradedOrder(Float.valueOf(orderArr[Trade_Price])*1000, Float.valueOf(orderArr[Trade_Vol]));

        Random ra =new Random();
        for (int i=0; i<kmeansKernels; i++) {
            centroids.add(new TradedOrder(ra.nextInt(2000), ra.nextInt(1000)));
        }
        findCloest(tradedOrder, centroids);
        computeMedian(centroids);
        return new KV(m.getKey(), "fraud detection!");
    }

    private void findCloest(TradedOrder tradedOrder, List<TradedOrder> centroids) {
        int bestIndex = 0;
        Double cloest = Double.POSITIVE_INFINITY;
        for (int i=0; i<centroids.size(); i++) {
            double tempDist = euclideanDistance(tradedOrder, centroids.get(i));
            if (tempDist < cloest) {
                cloest = tempDist;
                bestIndex = i;
            }
        }
//        return bestIndex;
    }

    private void computeMedian(List<TradedOrder> centroids) {
        long priceSum = 0;
        long volSum = 0;
        int length = centroids.size();
        for (int i=0; i<length; i++) {
            priceSum += centroids.get(i).tradePrice;
            volSum += centroids.get(i).tradeVol;
        }
        long avgPrice = priceSum / length;
        long avgVol = volSum / length;
    }

    private double euclideanDistance(TradedOrder var1, TradedOrder var2) {
        return (var1.tradePrice - var2.tradePrice) * (var1.tradePrice - var2.tradePrice) + (var1.tradeVol - var2.tradeVol);
    }

    static class TradedOrder implements Serializable {
        public float tradePrice;
        public float tradeVol;

        public TradedOrder(float tradePrice, float tradeVol) {
            this.tradePrice = tradePrice;
            this.tradeVol = tradeVol;
        }
    }
}
