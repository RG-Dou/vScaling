package samzatask.simpletask;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.TaskCoordinator.RequestScope;

/**
 * This is a simple task that writes each message to a state store and prints them all out on reload.
 *
 * It is useful for command line testing with the kafka console producer and consumer and text messages.
 */
public class SimpleStatefulTask implements StreamTask, InitableTask {

    private KeyValueStore<String, String> store;

    @SuppressWarnings("unchecked")
    public void init(Context context) {
        this.store = (KeyValueStore<String, String>) context.getTaskContext().getStore("mystore");
        System.out.println("Contents of store: ");
        KeyValueIterator<String, String> iter = store.all();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            System.out.println(entry.getKey() + " => " + entry.getValue());
        }
        iter.close();
    }

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        System.out.println("Adding " + envelope.getMessage() + " => " + envelope.getMessage() + " to the store.");
        store.put((String) envelope.getMessage(), (String) envelope.getMessage());
        coordinator.commit(RequestScope.ALL_TASKS_IN_CONTAINER);
    }

}
