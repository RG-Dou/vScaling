package samzatask.SD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.*;

import org.apache.samza.config.Config;

/**
 * This is a simple task that writes each message to a state store and prints them all out on reload.
 *
 * It is useful for command line testing with the kafka console producer and consumer and text messages.
 */
public class SpikeDetectionTask implements StreamTask, InitableTask, Serializable {
    private static final double threshold = 0.03d;
    private static final long movingStep = 1000;

    private static final int Date_Seq = 0;
    private static final int Time_Seq = 1;
    private static final int Epoch_Seq = 2;
    private static final int Mote_Id = 3;
    private static final int Temperature_Seq = 4;
    private static final int Humidity_Seq = 5;
    private static final int Light_Seq = 6;
    private static final int Voltage_Seq = 7;

    private final Config config;

    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "spike-alert");
    private KeyValueStore<String, LinkedList<Long>> deviceIDtoStreamMap;
    private RandomDataGenerator randomGen = new RandomDataGenerator();

    public SpikeDetectionTask(Config config) {
        this.config = config;
    }

    @SuppressWarnings("unchecked")
    public void init(Context context) {
        this.deviceIDtoStreamMap = (KeyValueStore<String,  LinkedList<Long>>)
                context.getTaskContext().getStore("device-ID-to-Stream-Map");
    }

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String logsStr = (String) envelope.getMessage();
        String[] logs = logsStr.split(" ");
        String moteId = logs[Mote_Id];
        double humidity = Double.parseDouble(logs[Humidity_Seq]);
        long tempe = Long.parseLong(logs[Humidity_Seq]);
        long avg = movingAverage(moteId, tempe);
        if (avg >= threshold){
            System.out.println("Spike Detected!");
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, moteId, "Spike Detected!"));
        }
    }

    private long movingAverage(String deviceId, long temperature){
        KeyValueIterator<String, LinkedList<Long>> mapIter = deviceIDtoStreamMap.all();
        LinkedList<Long> valueList = deviceIDtoStreamMap.get(deviceId);
        if (valueList != null) {
            if (valueList.size() > movingStep - 1) {
                double valueToRemove = valueList.removeFirst();
            }
            valueList.addLast(temperature);
            long sum = 0;
            for(int i =0; i < valueList.size(); i ++){
                sum += valueList.get(i);
            }
            deviceIDtoStreamMap.put(deviceId, valueList);
            return sum / valueList.size();
        } else {
            valueList = new LinkedList<>();
            valueList.add(temperature);
            deviceIDtoStreamMap.put(deviceId, valueList);
            return temperature;
        }
    }
}