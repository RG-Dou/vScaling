package samzatask.stock;

import java.util.HashMap;
import java.util.Map;

/**
 * Author by Mao
 * kmeans data structure, and some operator
 */

class OrderV2 {
    /**
     * The user that viewed the page
     */
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Last_Upd_Time = 2;
    private static final int Order_Price = 3;
    private static final int Order_Exec_Vol = 4;
    private static final int Order_Vol = 5;
    private static final int Sec_Code = 6;
    private static final int Trade_Dir = 7;

    private String[] orderArr;

    OrderV2(String[] orderArr) {
        this.orderArr = orderArr;
    }

    String getOrderNo() {
        return orderArr[Order_No];
    }
    String getTranMaintCode() {
        return orderArr[Tran_Maint_Code];
    }
    int getOrderPrice() {
        Float price = Float.parseFloat(orderArr[Order_Price]) * 100000;
        return price.intValue();
    }
    private int getOrderExecVol() {
        Float interOrderExecVol = Float.parseFloat(orderArr[Order_Exec_Vol]);
        return interOrderExecVol.intValue();
    }
    int getOrderVol() {
        Float interOrderVol = Float.parseFloat(orderArr[Order_Vol]);
        return interOrderVol.intValue();
    }
    String getSecCode() {
        return orderArr[Sec_Code];
    }
    String getTradeDir() {
        return orderArr[Trade_Dir];
    }

    public String getKey(int key) {
        return orderArr[key];
    }

    @Override
    public String toString() {
        return String.join("|", orderArr);
    }

    void updateOrder(int otherOrderVol) {
        orderArr[Order_Vol] = (this.getOrderVol() - otherOrderVol) + "";
        orderArr[Order_Exec_Vol] = (this.getOrderExecVol() + otherOrderVol) + "";
    }
}

