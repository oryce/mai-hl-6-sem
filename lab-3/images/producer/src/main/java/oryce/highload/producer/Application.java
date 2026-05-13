package oryce.highload.producer;

import tools.jackson.databind.JsonNode;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class Application implements AutoCloseable {

    private final RecordReader reader;
    private final RecordProducer producer;

    public Application(RecordReader reader, RecordProducer producer) {
        this.reader = requireNonNull(reader, "reader");
        this.producer = requireNonNull(producer, "producer");
    }

    public void run() throws Exception {
        try (Stream<JsonNode> stream = reader.stream()) {
            producer.produce(stream);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
