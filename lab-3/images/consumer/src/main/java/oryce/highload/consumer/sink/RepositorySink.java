package oryce.highload.consumer.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import oryce.highload.consumer.sink.entity.RepositoryFactory;
import oryce.highload.consumer.source.SaleEvent;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class RepositorySink implements Sink<SaleEvent> {

    private final RepositoryFactory factory;

    public RepositorySink(RepositoryFactory factory) {
        this.factory = requireNonNull(factory, "repository factory");
    }

    // Implemented for compatibility reasons. `@Override` is not recommended here per `Sink`'s
    // documentation.
    @SuppressWarnings("deprecation")
    public SinkWriter<SaleEvent> createWriter(InitContext context) throws IOException {
        return createWriter();
    }

    @Override
    public SinkWriter<SaleEvent> createWriter(WriterInitContext context) throws IOException {
        return createWriter();
    }

    private RepositorySinkWriter createWriter() throws IOException {
        return new RepositorySinkWriter(factory.repository());
    }
}
