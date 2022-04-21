package samzatask.AD;

import java.io.Serializable;

import org.apache.commons.math3.random.RandomDataGenerator;
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

import java.util.*;

import org.apache.samza.config.Config;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

/**
 * This is a simple task that writes each message to a state store and prints them all out on reload.
 *
 * It is useful for command line testing with the kafka console producer and consumer and text messages.
 */
public class AccidentDetectionTask implements StreamTask, InitableTask, Serializable {
    private static final double threshold = 0.03d;
    private static final long movingStep = 1000;

    private static final int Time_Seq = 1;
    private static final int Vehicle_ID = 2;
    private static final int Position_Seq = 8;
    private static final int Speed_Seq = 3;

    private final Config config;

    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "stop-detection");
    private KeyValueStore<String, LinkedList<String>> stoppedCars;
    private KeyValueStore<String, LinkedList<String>> allReports;
    private RandomDataGenerator randomGen = new RandomDataGenerator();

    public AccidentDetectionTask(Config config) {
        this.config = config;
    }

    @SuppressWarnings("unchecked")
    public void init(Context context) {
        this.stoppedCars = (KeyValueStore<String,  LinkedList<String>>)
                context.getTaskContext().getStore("stopped-cars");
        this.allReports = (KeyValueStore<String,  LinkedList<String>>)
                context.getTaskContext().getStore("all-reports");
    }

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String logsStr = (String) envelope.getMessage();
        String[] logs = logsStr.split(",");
        String vehicleId = logs[Vehicle_ID];
//        updateReports(vehicleId, logsStr);
        String position = logs[Position_Seq];
        long speed = Long.parseLong(logs[Speed_Seq]);
        boolean stopped = updateStoppedCar(vehicleId, position);
//        delay(100);
        if (stopped){
//            System.out.println(vehicleId + " stop at " + position);
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, vehicleId, "stop at " + position));
        } else{
//            System.out.println(vehicleId + " running!");
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, vehicleId, "running!"));
        }
    }

    private void delay(int interval) {
//        ranN = ranN*1000;
//        ranN = ranN*1000;
//        long delay = ranN.intValue();
//        if (delay < 0) delay = 6000;
        long delay = interval*100000;
        Long start = System.nanoTime();
        while (System.nanoTime() - start < delay) {}
    }

    private boolean updateStoppedCar(String vehicleId, String position){
        LinkedList<String> list = this.stoppedCars.get(vehicleId);
        if (list == null){
            list = new LinkedList<>();
        }
        if(list.size() >= 4) {
            list.remove(0);
        }
        list.add(position);
        this.stoppedCars.put(vehicleId, list);
        if(list.size() >= 4) {
            for (int i = 0; i < 4; i++) {
                if (!list.get(i).equals(position))
                    return false;
            }
        }
        return true;
    }

    private void updateReports(String vid, String reports){
        LinkedList<String> list = this.allReports.get(vid);
        if (list == null){
            list = new LinkedList<>();
        }
        for(int i = 0; i < 1; i ++) {
            list.add(reports);
        }
        this.allReports.put(vid, list);
    }
}