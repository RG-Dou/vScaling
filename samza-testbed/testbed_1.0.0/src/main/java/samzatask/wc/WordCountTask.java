package samzatask.wc;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.Map;

public class WordCountTask implements StreamTask {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "word-count-output");

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String outgoingMap = (String) envelope.getMessage();
        System.out.println(outgoingMap);
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outgoingMap));
    }
}
