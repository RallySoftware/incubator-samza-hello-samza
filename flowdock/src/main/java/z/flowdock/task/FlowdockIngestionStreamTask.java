package z.flowdock.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class FlowdockIngestionStreamTask implements StreamTask {
    @Override
    public void process(final IncomingMessageEnvelope envelope, final MessageCollector collector, final TaskCoordinator coordinator) throws Exception {

    }
}
