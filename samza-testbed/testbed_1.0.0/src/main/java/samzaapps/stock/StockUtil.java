package samzaapps.stock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StockUtil {

    /**
     * load file into buffer
     * @param
     * @return List<Order>
     */
    public static void loadPool() {

        Map<String, Map<Float, List<Order>>> pool = new HashMap<>();
        Map<String, List<Float>> poolPrice = new HashMap<>();

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
        List<Order> orderList = new ArrayList<>();
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
                    System.out.println(key);
                    pool.put(key, poolI);
                    poolPrice.put(key, pricePoolI);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        StockUtil.loadPool();
    }
}
