package samzaapps.stock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.*;
import java.net.URI;
import java.util.*;

import joptsimple.OptionSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.samza.util.CommandLine;

public class StockExchange implements StreamApplication {
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

    private static final String FILTER_KEY1 = "D";
    private static final String FILTER_KEY2 = "X";
    private static final String FILTER_KEY3 = "";
    Map<String, Map<Float, List<Order>>> pool = new HashMap<>();
    Map<String, List<Float>> poolPrice = new HashMap<>();

    private static final String INPUT_STREAM_ID = "stock_sb";
    private static final String OUTPUT_STREAM_ID = "stock_output";

    @Override
    public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {

        loadPool();

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

        MessageStream<KV<String, String>> inputStream = streamApplicationDescriptor.getInputStream(inputDescriptor);
        OutputStream<KV<String, String>> outputStream = streamApplicationDescriptor.getOutputStream(outputDescriptor);

        inputStream
                .map((tuple)->{
                    // String[] orderList = tuple.split("\\|");
                    Order order = new Order(tuple.value);
                    return order;
                })
//                .map(order -> {
//                    String[] orderArr = order.getValue().split("\\|");
//                    return new KV(order.getKey(), orderArr);
//                })
                .filter((order) -> !FILTER_KEY2.equals(order.getTranMaintCode()))
                .filter((order) -> !FILTER_KEY3.equals(order.getTranMaintCode()))
                .map((order)->{
                    return this.stockExchange(pool, poolPrice, order);
                })
                .filter((tradeResult) -> !tradeResult.isEmpty())
                .map(list -> KV.of("traded", list.toString()))
                .sendTo(outputStream);
//                .map((KV m) -> movingAverage(m))
//                .sendTo(outputStream);
    }

    /**
     * load file into buffer
     * @param
     * @return List<Order>
     */
    public void loadPool() {

        System.out.println("loading the pool...");

        Configuration conf = new Configuration();
        String hdfsuri = "hdfs://camel:9000/";
        conf.set("fs.defaultFS", hdfsuri);

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");

        // pool initialization
        BufferedReader br = null;
        Map<Float, List<Order>> poolI = new HashMap<Float, List<Order>>();
        List<Order> orderList;
        List<Float> pricePoolI = new ArrayList<>();
        String sCurrentLine;

        // load all opening price from HDFS
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
            FileStatus[] stockRoot = fs.listStatus(new Path(hdfsuri+"opening"));
            for(FileStatus status : stockRoot){
                FileStatus[] stockDir = fs.listStatus(status.getPath());
                for (FileStatus realFile : stockDir) {
                    FSDataInputStream inputStream = fs.open(realFile.getPath());
                    br = new BufferedReader(new InputStreamReader(inputStream));
                    while ((sCurrentLine = br.readLine()) != null) {
                        Order order = new Order(sCurrentLine);
                        orderList = poolI.get(order.getOrderPrice());
                        if (orderList == null) {
                            pricePoolI.add(order.getOrderPrice());
                            orderList = new ArrayList<>();
                        }
                        orderList.add(order);
                        poolI.put(order.getOrderPrice(), orderList);
                    }
                    String key = status.getPath().getName() + realFile.getPath().getName().charAt(0);
                    pool.put(key, poolI);
                    poolPrice.put(key, pricePoolI);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("loading complete");
    }

    private KV<String, String> movingAverage(KV m) {
        String[] orderArr = (String[]) m.getValue();
        if (!stockAvgPriceMap.containsKey(orderArr[Sec_Code])) {
            stockAvgPriceMap.put(orderArr[Sec_Code], (float) 0);
        }
        float sum = stockAvgPriceMap.get(orderArr[Sec_Code]) + Float.parseFloat(orderArr[Order_Price]);
        stockAvgPriceMap.put(orderArr[Sec_Code], sum);
        return new KV(m.getKey(), String.valueOf(sum));
    }

    /**
     * deal continous transaction
     * @param poolB,poolS,pool,order
     * @return output string
     */
    public List<String> transaction(Map<Float, List<Order>> poolB, Map<Float, List<Order>> poolS,
                                    List<Float> poolPriceB, List<Float> poolPriceS, Map<String, List<Float>> poolPrice,
                                    Map<String, Map<Float, List<Order>>> pool, Order order) {
        // hava a transaction
        int top = 0;
        int i = 0;
        int j = 0;
        int otherOrderVol;
        int totalVol = 0;
        float tradePrice = 0;
        List<String> tradeResult = new ArrayList<>();
        while (poolPriceS.get(top) <= poolPriceB.get(top)) {
            tradePrice = poolPriceS.get(top);
            if (poolB.get(poolPriceB.get(top)).get(top).getOrderVol() > poolS.get(poolPriceS.get(top)).get(top).getOrderVol()) {
                // B remains B_top-S_top
                otherOrderVol = poolS.get(poolPriceS.get(top)).get(top).getOrderVol();
                // totalVol sum
                totalVol += otherOrderVol;
                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
                // S complete
                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
                // remove top of poolS
                poolS.get(poolPriceS.get(top)).remove(top);
                // no order in poolS, transaction over
                if (poolS.get(poolPriceS.get(top)).isEmpty()) {
                    // find next price
                    poolS.remove(poolPriceS.get(top));
                    poolPriceS.remove(top);
                    if (poolPriceS.isEmpty()) {
                        break;
                    }
                }
                // TODO: output poolB poolS price etc
            } else {
                otherOrderVol = poolB.get(poolPriceB.get(top)).get(top).getOrderVol();
                // totalVol sum
                totalVol += otherOrderVol;
                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
                poolB.get(poolPriceB.get(top)).remove(top);
                // no order in poolB, transaction over
                if (poolB.get(poolPriceB.get(top)).isEmpty()) {
                    poolB.remove(poolPriceB.get(top));
                    poolPriceB.remove(top);
                    if (poolPriceB.isEmpty()) {
                        break;
                    }
                }
                // TODO: output poolB poolS price etc
            }
        }
        pool.put(order.getSecCode()+"S", poolS);
        pool.put(order.getSecCode()+"B", poolB);
        poolPrice.put(order.getSecCode()+"B", poolPriceB);
        poolPrice.put(order.getSecCode()+"S", poolPriceS);
        tradeResult.add(order.getSecCode());
        tradeResult.add(String.valueOf(totalVol));
        tradeResult.add(String.valueOf(tradePrice));
        // return tradeResult;
        return tradeResult;
    }

    /**
     * mapFunction
     * @param pool, order
     * @return String
     */
    public List<String> stockExchange(Map<String, Map<Float, List<Order>>> pool, Map<String, List<Float>> poolPrice, Order order) {
        // String complete = new String();
        List<String> tradeResult = new ArrayList<>();
        // load poolS poolB
        Map<Float, List<Order>> poolS = pool.get(order.getSecCode()+"S");
        Map<Float, List<Order>> poolB = pool.get(order.getSecCode()+"B");
        List<Float> poolPriceB = poolPrice.get(order.getSecCode()+"B");
        List<Float> poolPriceS = poolPrice.get(order.getSecCode()+"S");

        if (poolB == null) {
            poolB = new HashMap<>();
        }
        if (poolS == null) {
            poolS = new HashMap<>();
        }
        if (poolPriceB == null) {
            poolPriceB = new ArrayList<>();
        }
        if (poolPriceS == null) {
            poolPriceS = new ArrayList<>();
        }

        if (order.getTradeDir().equals("B")) {
            float orderPrice = order.getOrderPrice();
            List<Order> BorderList = poolB.get(orderPrice);
            // if order tran_maint_code is "D", delete from pool
            if (FILTER_KEY1.equals(order.getTranMaintCode())) {
                // if exist in order, remove from pool
                String orderNo = order.getOrderNo();
                if (BorderList == null) {
                    // return "{\"process_no\":\"11\", \"result\":\"no such B order to delete:" + orderNo+"\"}";
                    return tradeResult;
                }
                for (int i=0; i < BorderList.size(); i++) {
                    if (orderNo.equals(BorderList.get(i).getOrderNo())) {
                        BorderList.remove(i);
                        // if no other price delete poolPrice
                        if (BorderList.isEmpty()) {
                            for (int j=0; j < poolPriceB.size(); j++) {
                                if (poolPriceB.get(j) == orderPrice) {
                                    poolPriceB.remove(j);
                                    break;
                                }
                            }
                            poolB.remove(orderPrice);
                        } else {
                            poolB.put(orderPrice, BorderList);
                        }
                        poolPrice.put(order.getSecCode()+"B", poolPriceB);
                        pool.put(order.getSecCode()+"B", poolB);
                        return tradeResult;
                    }
                }
                // else output no delete order exist
                return tradeResult;
            }

            // put into buy poolB
            if (BorderList == null) {
                BorderList = new ArrayList<>();
                // price add a value
                if (poolB.isEmpty()) {
                    poolPriceB.add(orderPrice);
                } else {
                    for (int i = 0; i < poolPriceB.size(); i++) {
                        if (poolPriceB.get(i) < orderPrice) {
                            poolPriceB.add(i, orderPrice);
                            break;
                        }
                        if (i == poolPriceB.size()-1) {
                            poolPriceB.add(orderPrice);
                            break;
                        }
                    }
                }
            }
            BorderList.add(order);
            poolB.put(orderPrice, BorderList);

            // if no elements in poolS, no transaction, add poolB
            if (poolPriceS.isEmpty()) {
                pool.put(order.getSecCode()+"B", poolB);
                poolPrice.put(order.getSecCode()+"B", poolPriceB);
                // return complete;
                return tradeResult;
            }

            // no satisfied price
            if (poolPriceS.get(0) > poolPriceB.get(0)) {
                // this.savepool();
                pool.put(order.getSecCode()+"S", poolS);
                pool.put(order.getSecCode()+"B", poolB);
                poolPrice.put(order.getSecCode()+"S", poolPriceS);
                poolPrice.put(order.getSecCode()+"B", poolPriceB);
                return tradeResult;
            } else {
                tradeResult = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
            }
        } else if (order.getTradeDir().equals("S")) {
            float orderPrice = order.getOrderPrice();
            List<Order> SorderList = poolS.get(orderPrice);
            // if order tran_maint_code is "D", delete from pool
            if (FILTER_KEY1.equals(order.getTranMaintCode())) {
                // if exist in order, remove from pool
                String orderNo = order.getOrderNo();
                if (SorderList == null) {
                    return tradeResult;
                }
                for (int i=0; i < SorderList.size(); i++) {
                    if (orderNo.equals(SorderList.get(i).getOrderNo())) {
                        SorderList.remove(i);
                        // if no other price delete poolPrice
                        if (SorderList.isEmpty()) {
                            for (int j=0; j < poolPriceS.size(); j++) {
                                if (poolPriceS.get(j) == orderPrice) {
                                    poolPriceS.remove(j);
                                    break;
                                }
                            }
                            poolS.remove(orderPrice);
                        } else {
                            poolS.put(orderPrice, SorderList);
                        }
                        poolPrice.put(order.getSecCode()+"S", poolPriceS);
                        pool.put(order.getSecCode()+"S", poolS);
                        return tradeResult;
                    }
                }
                // else output no delete order exist
                return tradeResult;
            }

            // put into buy poolS
            if (SorderList == null) {
                SorderList = new ArrayList<>();
                // price add a value
                if (poolS.isEmpty()) {
                    poolPriceS.add(orderPrice);
                } else {
                    for (int i = 0; i < poolPriceS.size(); i++) {
                        if (poolPriceS.get(i) > orderPrice) {
                            poolPriceS.add(i, orderPrice);
                            break;
                        }
                        if (i == poolPriceS.size()-1) {
                            poolPriceS.add(orderPrice);
                            break;
                        }
                    }
                }
            }
            SorderList.add(order);
            poolS.put(orderPrice, SorderList);
            // if no elements in poolB, no transaction, add poolS
            if (poolPriceB.isEmpty()) {
                pool.put(order.getSecCode()+"S", poolS);
                poolPrice.put(order.getSecCode()+"S", poolPriceS);
                return tradeResult;
            }

            // no satisfied price
            if (poolPriceS.get(0) > poolPriceB.get(0)) {
                // this.savepool();
                pool.put(order.getSecCode()+"S", poolS);
                pool.put(order.getSecCode()+"B", poolB);
                poolPrice.put(order.getSecCode()+"S", poolPriceS);
                poolPrice.put(order.getSecCode()+"B", poolPriceB);
                return tradeResult;
            } else {
                tradeResult = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
            }
        }
        return tradeResult;
    }
}
