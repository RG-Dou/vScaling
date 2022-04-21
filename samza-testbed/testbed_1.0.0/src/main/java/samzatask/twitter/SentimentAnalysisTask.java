package samzatask.twitter;

import org.apache.samza.context.Context;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.codehaus.jettison.json.JSONObject;
import samzatask.config.SentimentAnalysisConstants.*;
import samzatask.fd.FraudDetectionTask;
import samzatask.config.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import samzatask.twitter.model.classifier.SentimentClassifier;
import samzatask.twitter.model.classifier.SentimentClassifierFactory;
import samzatask.twitter.model.classifier.SentimentResult;

public class SentimentAnalysisTask implements StreamTask, InitableTask  {

    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "sa_output");

    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy")
            .withLocale(Locale.ENGLISH);
    private SentimentClassifier classifier;
    private Configuration config;
    private static final String CFG_PATH = "/sa/%s.properties";

    @Override
    public void init(Context context) throws Exception {

        String cfg = String.format(CFG_PATH, "sentiment-analysis");
        Properties p = loadProperties(cfg, true);
        System.out.println("Loaded default configuration file " + cfg);

        config = Configuration.fromProperties(p);
        // initialize predictor

        String classifierType = config.getString(Conf.CLASSIFIER_TYPE, SentimentClassifierFactory.BASIC);
        classifier = SentimentClassifierFactory.create(classifierType, config);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String input = (String) envelope.getMessage();
        JSONObject tweet = new JSONObject(input);
        if (!tweet.has("id_str") || !tweet.has("text") || !tweet.has("created_at"))
            return;
        String tweetId     = (String) tweet.get("id_str");
        String text        = (String) tweet.get("text");
        DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get("created_at"));

        SentimentResult result = classifier.classify(text);

        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, result.toString()));
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
