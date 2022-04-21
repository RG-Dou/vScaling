package samzatask.stock;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This is a simple task that writes each message to a state store and prints them all out on reload.
 *
 * It is useful for command line testing with the kafka console producer and consumer and text messages.
 */
public class StateLoaderTask implements StreamTask, InitableTask {
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
    private Map<String, Map<Float, List<Order>>> pool = new HashMap<>();
    private Map<String, List<Float>> poolPrice = new HashMap<>();


    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "stock_cj");
    private KeyValueStore<String, String> stockExchangeMapSell;
    private KeyValueStore<String, String> stockExchangeMapBuy;
    private RandomDataGenerator randomGen = new RandomDataGenerator();

    @SuppressWarnings("unchecked")
    public void init(Context context) {
        this.stockExchangeMapSell = (KeyValueStore<String, String>) context.getTaskContext().getStore("stock-exchange-sell");
        this.stockExchangeMapBuy = (KeyValueStore<String, String>) context.getTaskContext().getStore("stock-exchange-buy");
        System.out.println("+++++Store loaded successfully!");
    }

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String stockOrder = (String) envelope.getMessage();
        String[] orderArr = stockOrder.split("\\|");

        //filter
        if (orderArr[Tran_Maint_Code].equals(FILTER_KEY1) || orderArr[Tran_Maint_Code].equals(FILTER_KEY2) || orderArr[Tran_Maint_Code].equals(FILTER_KEY3)) {
            return;
        }
        // do transaction
        Map<String, String> matchedResult = doStockExchange(orderArr, orderArr[Trade_Dir]);

        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, orderArr[Sec_Code], stockOrder));
    }

    private Map<String, String> doStockExchange(String[] orderArr, String direction) {
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

    private Map<String, String> tradeSell(String[] sellerOrder, KeyValueStore<String, String> stockExchangeMap) {
        Map<String, String> matchedBuy = new HashMap<>();
        Map<String, String> matchedSell = new HashMap<>();
        Map<String, String> pendingBuy = new HashMap<>();
        Map<String, String> pendingSell = new HashMap<>();
        KeyValueIterator<String, String> iter = stockExchangeMap.all();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
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
        iter.close();

        updateStore(pendingBuy, pendingSell, matchedBuy, matchedSell);

        return matchedSell;
    }

    private Map<String, String> tradeBuy(String[] buyerOrder, KeyValueStore<String, String> stockExchangeMap) {
        Map<String, String> matchedBuy = new HashMap<>();
        Map<String, String> matchedSell = new HashMap<>();
        Map<String, String> pendingBuy = new HashMap<>();
        Map<String, String> pendingSell = new HashMap<>();
        KeyValueIterator<String, String> iter = stockExchangeMap.all();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
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
        iter.close();

        updateStore(pendingBuy, pendingSell, matchedBuy, matchedSell);

        return matchedBuy;
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
            stockExchangeMapBuy.delete(order.getKey());
        }
        for (Map.Entry<String, String> order : matchedSell.entrySet()) {
            stockExchangeMapSell.delete(order.getKey());
        }
    }
}