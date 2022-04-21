package samzatask.stock;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest.TOPIC;

public class StockExchangeSimu {
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    private static final String FILTER_KEY1 = "D";
    private static final String FILTER_KEY2 = "X";
    private static final String FILTER_KEY3 = "";

    private Map<String, String> stockExchangeMapSell;
    private Map<String, String> stockExchangeMapBuy;

    public StockExchangeSimu() {
        this.stockExchangeMapSell = new HashMap<>();
        this.stockExchangeMapBuy = new HashMap<>();
    }

//    private Map<String, Map<Float, List<Order>>> pool = new HashMap<>();
//    private Map<String, List<Float>> poolPrice = new HashMap<>();
//
//    /**
//     * load file into buffer
//     * @param
//     * @return List<Order>
//     */
//    public void loadPool() {
//
//        System.out.println("loading the pool...");
//
//        Configuration conf = new Configuration();
//        String hdfsuri = "hdfs://camel:9000/";
//        conf.set("fs.defaultFS", hdfsuri);
//
//        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//
//        System.setProperty("HADOOP_USER_NAME", "hdfs");
//        System.setProperty("hadoop.home.dir", "/");
//
//        // pool initialization
//        BufferedReader br = null;
//        Map<Float, List<Order>> poolI = new HashMap<Float, List<Order>>();
//        List<Order> orderList;
//        List<Float> pricePoolI = new ArrayList<>();
//        String sCurrentLine;
//
//        // load all opening price from HDFS
//        try {
//            FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
//            FileStatus[] stockRoot = fs.listStatus(new Path(hdfsuri+"opening"));
//            for(FileStatus status : stockRoot){
//                FileStatus[] stockDir = fs.listStatus(status.getPath());
//                for (FileStatus realFile : stockDir) {
//                    FSDataInputStream inputStream = fs.open(realFile.getPath());
//                    br = new BufferedReader(new InputStreamReader(inputStream));
//                    while ((sCurrentLine = br.readLine()) != null) {
//                        Order order = new Order(sCurrentLine.split("\\|"));
//                        orderList = poolI.get(order.getOrderPrice());
//                        if (orderList == null) {
//                            pricePoolI.add(order.getOrderPrice());
//                            orderList = new ArrayList<>();
//                        }
//                        orderList.add(order);
//                        poolI.put(order.getOrderPrice(), orderList);
//                    }
//                    String key = status.getPath().getName() + realFile.getPath().getName().charAt(0);
//                    pool.put(key, poolI);
//                    poolPrice.put(key, pricePoolI);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("loading complete");
//    }
//
//    /**
//     * deal continous transaction
//     * @param poolB,poolS,pool,order
//     * @return output string
//     */
//    private List<String> transaction(Map<Float, List<Order>> poolB, Map<Float, List<Order>> poolS,
//                                     List<Float> poolPriceB, List<Float> poolPriceS, Map<String, List<Float>> poolPrice,
//                                     Map<String, Map<Float, List<Order>>> pool, Order order) {
//        // hava a transaction
//        int top = 0;
//        int i = 0;
//        int j = 0;
//        int otherOrderVol;
//        int totalVol = 0;
//        float tradePrice = 0;
//        List<String> tradeResult = new ArrayList<>();
//        while (poolPriceS.get(top) <= poolPriceB.get(top)) {
//            tradePrice = poolPriceS.get(top);
//            if (poolB.get(poolPriceB.get(top)).get(top).getOrderVol() > poolS.get(poolPriceS.get(top)).get(top).getOrderVol()) {
//                // B remains B_top-S_top
//                otherOrderVol = poolS.get(poolPriceS.get(top)).get(top).getOrderVol();
//                // totalVol sum
//                totalVol += otherOrderVol;
//                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
//                // S complete
//                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
//                // remove top of poolS
//                poolS.get(poolPriceS.get(top)).remove(top);
//                // no order in poolS, transaction over
//                if (poolS.get(poolPriceS.get(top)).isEmpty()) {
//                    // find next price
//                    poolS.remove(poolPriceS.get(top));
//                    poolPriceS.remove(top);
//                    if (poolPriceS.isEmpty()) {
//                        break;
//                    }
//                }
//                // TODO: output poolB poolS price etc
//            } else {
//                otherOrderVol = poolB.get(poolPriceB.get(top)).get(top).getOrderVol();
//                // totalVol sum
//                totalVol += otherOrderVol;
//                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
//                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
//                poolB.get(poolPriceB.get(top)).remove(top);
//                // no order in poolB, transaction over
//                if (poolB.get(poolPriceB.get(top)).isEmpty()) {
//                    poolB.remove(poolPriceB.get(top));
//                    poolPriceB.remove(top);
//                    if (poolPriceB.isEmpty()) {
//                        break;
//                    }
//                }
//                // TODO: output poolB poolS price etc
//            }
//        }
//        pool.put(order.getSecCode()+"S", poolS);
//        pool.put(order.getSecCode()+"B", poolB);
//        poolPrice.put(order.getSecCode()+"B", poolPriceB);
//        poolPrice.put(order.getSecCode()+"S", poolPriceS);
//        tradeResult.add(order.getSecCode());
//        tradeResult.add(String.valueOf(totalVol));
//        tradeResult.add(String.valueOf(tradePrice));
//        // return tradeResult;
//        return tradeResult;
//    }
//
//    /**
//     * mapFunction
//     * @param pool, order
//     * @return String
//     */
//    private List<String> stockExchange(Map<String, Map<Float, List<Order>>> pool, Map<String, List<Float>> poolPrice, Order order) {
//        // String complete = new String();
//        List<String> tradeResult = new ArrayList<>();
//        // load poolS poolB
//        Map<Float, List<Order>> poolS = pool.get(order.getSecCode()+"S");
//        Map<Float, List<Order>> poolB = pool.get(order.getSecCode()+"B");
//        List<Float> poolPriceB = poolPrice.get(order.getSecCode()+"B");
//        List<Float> poolPriceS = poolPrice.get(order.getSecCode()+"S");
//
//        if (poolB == null) {
//            poolB = new HashMap<>();
//        }
//        if (poolS == null) {
//            poolS = new HashMap<>();
//        }
//        if (poolPriceB == null) {
//            poolPriceB = new ArrayList<>();
//        }
//        if (poolPriceS == null) {
//            poolPriceS = new ArrayList<>();
//        }
//
//        if (order.getTradeDir().equals("B")) {
//            float orderPrice = order.getOrderPrice();
//            List<Order> BorderList = poolB.get(orderPrice);
//            // if order tran_maint_code is "D", delete from pool
//            if (FILTER_KEY1.equals(order.getTranMaintCode())) {
//                // if exist in order, remove from pool
//                String orderNo = order.getOrderNo();
//                if (BorderList == null) {
//                    // return "{\"process_no\":\"11\", \"result\":\"no such B order to delete:" + orderNo+"\"}";
//                    return tradeResult;
//                }
//                for (int i=0; i < BorderList.size(); i++) {
//                    if (orderNo.equals(BorderList.get(i).getOrderNo())) {
//                        BorderList.remove(i);
//                        // if no other price delete poolPrice
//                        if (BorderList.isEmpty()) {
//                            for (int j=0; j < poolPriceB.size(); j++) {
//                                if (poolPriceB.get(j) == orderPrice) {
//                                    poolPriceB.remove(j);
//                                    break;
//                                }
//                            }
//                            poolB.remove(orderPrice);
//                        } else {
//                            poolB.put(orderPrice, BorderList);
//                        }
//                        poolPrice.put(order.getSecCode()+"B", poolPriceB);
//                        pool.put(order.getSecCode()+"B", poolB);
//                        return tradeResult;
//                    }
//                }
//                // else output no delete order exist
//                return tradeResult;
//            }
//
//            // put into buy poolB
//            if (BorderList == null) {
//                BorderList = new ArrayList<>();
//                // price add a value
//                if (poolB.isEmpty()) {
//                    poolPriceB.add(orderPrice);
//                } else {
//                    for (int i = 0; i < poolPriceB.size(); i++) {
//                        if (poolPriceB.get(i) < orderPrice) {
//                            poolPriceB.add(i, orderPrice);
//                            break;
//                        }
//                        if (i == poolPriceB.size()-1) {
//                            poolPriceB.add(orderPrice);
//                            break;
//                        }
//                    }
//                }
//            }
//            BorderList.add(order);
//            poolB.put(orderPrice, BorderList);
//
//            // if no elements in poolS, no transaction, add poolB
//            if (poolPriceS.isEmpty()) {
//                pool.put(order.getSecCode()+"B", poolB);
//                poolPrice.put(order.getSecCode()+"B", poolPriceB);
//                // return complete;
//                return tradeResult;
//            }
//
//            // no satisfied price
//            if (poolPriceS.get(0) > poolPriceB.get(0)) {
//                // this.savepool();
//                pool.put(order.getSecCode()+"S", poolS);
//                pool.put(order.getSecCode()+"B", poolB);
//                poolPrice.put(order.getSecCode()+"S", poolPriceS);
//                poolPrice.put(order.getSecCode()+"B", poolPriceB);
//                return tradeResult;
//            } else {
//                tradeResult = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
//            }
//        } else if (order.getTradeDir().equals("S")) {
//            float orderPrice = order.getOrderPrice();
//            List<Order> SorderList = poolS.get(orderPrice);
//            // if order tran_maint_code is "D", delete from pool
//            if (FILTER_KEY1.equals(order.getTranMaintCode())) {
//                // if exist in order, remove from pool
//                String orderNo = order.getOrderNo();
//                if (SorderList == null) {
//                    return tradeResult;
//                }
//                for (int i=0; i < SorderList.size(); i++) {
//                    if (orderNo.equals(SorderList.get(i).getOrderNo())) {
//                        SorderList.remove(i);
//                        // if no other price delete poolPrice
//                        if (SorderList.isEmpty()) {
//                            for (int j=0; j < poolPriceS.size(); j++) {
//                                if (poolPriceS.get(j) == orderPrice) {
//                                    poolPriceS.remove(j);
//                                    break;
//                                }
//                            }
//                            poolS.remove(orderPrice);
//                        } else {
//                            poolS.put(orderPrice, SorderList);
//                        }
//                        poolPrice.put(order.getSecCode()+"S", poolPriceS);
//                        pool.put(order.getSecCode()+"S", poolS);
//                        return tradeResult;
//                    }
//                }
//                // else output no delete order exist
//                return tradeResult;
//            }
//
//            // put into buy poolS
//            if (SorderList == null) {
//                SorderList = new ArrayList<>();
//                // price add a value
//                if (poolS.isEmpty()) {
//                    poolPriceS.add(orderPrice);
//                } else {
//                    for (int i = 0; i < poolPriceS.size(); i++) {
//                        if (poolPriceS.get(i) > orderPrice) {
//                            poolPriceS.add(i, orderPrice);
//                            break;
//                        }
//                        if (i == poolPriceS.size()-1) {
//                            poolPriceS.add(orderPrice);
//                            break;
//                        }
//                    }
//                }
//            }
//            SorderList.add(order);
//            poolS.put(orderPrice, SorderList);
//            // if no elements in poolB, no transaction, add poolS
//            if (poolPriceB.isEmpty()) {
//                pool.put(order.getSecCode()+"S", poolS);
//                poolPrice.put(order.getSecCode()+"S", poolPriceS);
//                return tradeResult;
//            }
//
//            // no satisfied price
//            if (poolPriceS.get(0) > poolPriceB.get(0)) {
//                // this.savepool();
//                pool.put(order.getSecCode()+"S", poolS);
//                pool.put(order.getSecCode()+"B", poolB);
//                poolPrice.put(order.getSecCode()+"S", poolPriceS);
//                poolPrice.put(order.getSecCode()+"B", poolPriceB);
//                return tradeResult;
//            } else {
//                tradeResult = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
//            }
//        }
//        return tradeResult;
//    }

    public Map<String, String> doStockExchange(String[] orderArr, String direction) {
        Map<String, String> matchedResult = new HashMap<>();
        if (direction.equals("")) {
            System.out.println("bad tuple received!");
            return matchedResult;
        }
        if (direction.equals("S")) {
            stockExchangeMapSell.put(orderArr[Sec_Code], String.join("|", orderArr));
            matchedResult = tradeSell(orderArr, stockExchangeMapBuy);
        } else {
            stockExchangeMapBuy.put(orderArr[Sec_Code], String.join("|", orderArr));
            matchedResult = tradeBuy(orderArr, stockExchangeMapSell);
        }
        return matchedResult;
    }

    private Map<String, String> tradeSell(String[] sellerOrder, Map<String, String> stockExchangeMap) {
        Map<String, String> matchedBuy = new HashMap<>();
        Map<String, String> matchedSell = new HashMap<>();
        Map<String, String> pendingBuy = new HashMap<>();
        Map<String, String> pendingSell = new HashMap<>();
        Iterator iter = stockExchangeMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
            String orderNo = entry.getKey();
            String[] curBuyerOrder = entry.getValue().split("\\|");

            if (curBuyerOrder[Sec_Code].equals(sellerOrder[Sec_Code])) {
                String left = match(curBuyerOrder, sellerOrder);
                if (!left.equals("unmatched")) {
                    if (left.equals("S")) {
                        pendingSell.put(sellerOrder[Sec_Code], String.join("\\|", sellerOrder));
                        matchedBuy.put(curBuyerOrder[Sec_Code], String.join("\\|", curBuyerOrder));
                    } else if (left.equals("B")) {
                        pendingBuy.put(curBuyerOrder[Sec_Code], String.join("\\|", curBuyerOrder));
                        matchedSell.put(sellerOrder[Sec_Code], String.join("\\|", sellerOrder));
                    } else {
                        matchedSell.put(sellerOrder[Sec_Code], String.join("\\|", sellerOrder));
                        matchedBuy.put(curBuyerOrder[Sec_Code], String.join("\\|", curBuyerOrder));
                    }
                }
            }
        }

        updateStore(pendingBuy, pendingSell, matchedBuy, matchedSell);

        return matchedSell;
    }

    private Map<String, String> tradeBuy(String[] buyerOrder, Map<String, String> stockExchangeMap) {
        Map<String, String> matchedBuy = new HashMap<>();
        Map<String, String> matchedSell = new HashMap<>();
        Map<String, String> pendingBuy = new HashMap<>();
        Map<String, String> pendingSell = new HashMap<>();
        Iterator iter = stockExchangeMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
            String orderNo = entry.getKey();
            String[] curSellerOrder = entry.getValue().split("\\|");

            if (curSellerOrder[Sec_Code].equals(buyerOrder[Sec_Code])) {
                String left = match(buyerOrder, curSellerOrder);
                if (!left.equals("unmatched")) {
                    if (left.equals("S")) {
                        pendingSell.put(curSellerOrder[Sec_Code], String.join("\\|", curSellerOrder));
                        matchedBuy.put(buyerOrder[Sec_Code], String.join("\\|", buyerOrder));
                    } else if (left.equals("B")) {
                        pendingBuy.put(buyerOrder[Sec_Code], String.join("\\|", buyerOrder));
                        matchedSell.put(curSellerOrder[Sec_Code], String.join("\\|", curSellerOrder));
                    } else {
                        matchedSell.put(curSellerOrder[Sec_Code], String.join("\\|", curSellerOrder));
                        matchedBuy.put(buyerOrder[Sec_Code], String.join("\\|", buyerOrder));
                    }
                }
            }
        }

        updateStore(pendingBuy, pendingSell, matchedBuy, matchedSell);

        return matchedSell;
    }

    private String match(String[] buyerOrder, String[] sellerOrder) {
        float buyPrice = Float.valueOf(buyerOrder[Order_Price]);
        float sellPrice = Float.valueOf(sellerOrder[Order_Price]);
        if (buyPrice < sellPrice) {
            return "unmatched";
        }
        float buyVol = Float.valueOf(buyerOrder[Order_Vol]);
        float sellVol = Float.valueOf(sellerOrder[Order_Vol]);
        if (buyVol > sellVol) {
            buyerOrder[Order_Vol] = String.valueOf(buyVol - sellVol);
            return "B";
        } else if (buyVol < sellVol) {
            sellerOrder[Order_Vol] = String.valueOf(sellVol - buyVol);
            return "S";
        } else {
            return "";
        }
    }

    private void updateStore(
            Map<String, String> pendingBuy,
            Map<String, String> pendingSell,
            Map<String, String> matchedBuy,
            Map<String, String> matchedSell) {
        for (Map.Entry<String, String> order : pendingBuy.entrySet()) {
            stockExchangeMapBuy.put(order.getKey(), order.getValue());
        }
        for (Map.Entry<String, String> order : pendingSell.entrySet()) {
            stockExchangeMapSell.put(order.getKey(), order.getValue());
        }
        for (Map.Entry<String, String> order : matchedBuy.entrySet()) {
            stockExchangeMapBuy.remove(order.getKey());
        }
        for (Map.Entry<String, String> order : matchedSell.entrySet()) {
            stockExchangeMapSell.remove(order.getKey());
        }
    }

    public static void main(String[] args) throws IOException {
        StockExchangeSimu ses = new StockExchangeSimu();
//        ses.loadPool();

        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        int noRecSleepCnt = 0;
        stream = new FileReader("/root/SSE-kafka-producer/partition1.txt");
        br = new BufferedReader(stream);

        int interval = 1000000000/1000;

        RandomDataGenerator randomGen = new RandomDataGenerator();


        while ((sCurrentLine = br.readLine()) != null) {

            if (sCurrentLine.equals("end")) {
                continue;
            }

            if (sCurrentLine.split("\\|").length < 10) {
                continue;
            }

            String[] orderArr = sCurrentLine.split("\\|");

            if (orderArr[Tran_Maint_Code].equals(FILTER_KEY1) || orderArr[Tran_Maint_Code].equals(FILTER_KEY2) || orderArr[Tran_Maint_Code].equals(FILTER_KEY3)) {
                continue;
            }

            Map<String, String> matchedResult = ses.doStockExchange(orderArr, orderArr[Trade_Dir]);

//            for (Map.Entry<String, String> order : matchedResult.entrySet()) {
//                System.out.println(order.getValue());
//            }
        }
    }
}
