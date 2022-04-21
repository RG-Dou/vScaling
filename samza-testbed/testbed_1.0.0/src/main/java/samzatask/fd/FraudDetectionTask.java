package samzatask.fd;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import samzatask.config.Configuration;
import samzatask.fd.model.predictor.MarkovModelPredictor;
import samzatask.fd.model.predictor.ModelBasedPredictor;
import samzatask.fd.model.predictor.Prediction;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FraudDetectionTask implements StreamTask, InitableTask {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "fd_output");
    private KeyValueStore<String, String> store;
    private ModelBasedPredictor predictor;
    private Configuration config;
    private static final String CFG_PATH = "/fd/%s.properties";

    @Override
    public void init(Context context) throws Exception {
//        this.store = (KeyValueStore<String, String>) context.getTaskContext().getStore("fraud-detection");
//        System.out.println("Contents of store: ");
//        KeyValueIterator<String, String> iter = store.all();
//        while (iter.hasNext()) {
//            Entry<String, String> entry = iter.next();
//            System.out.println(entry.getKey() + " => " + entry.getValue());
//        }
//        iter.close();

        String cfg = String.format(CFG_PATH, "fraud-detection");
        Properties p = loadProperties(cfg, true);

        System.out.println("Loaded default configuration file " + cfg);

        config = Configuration.fromProperties(p);
        // initialize predictor
        predictor = new MarkovModelPredictor(config);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String input = (String) envelope.getMessage();
        String[] items = input.split(",", 2);
        String entityID = items[0];
        String record   = items[1];
        Prediction p    = predictor.execute(entityID, record);
        String fdResults = p.toString();
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, fdResults));
    }

    public static Properties loadProperties(String filename, boolean classpath) throws IOException {
        Properties properties = new Properties();
        InputStream is;

        if (classpath) {
            is = FraudDetectionTask.class.getResourceAsStream(filename);
        } else {
            is = new FileInputStream(filename);
        }

        properties.load(is);
        is.close();

        return properties;
    }
}
