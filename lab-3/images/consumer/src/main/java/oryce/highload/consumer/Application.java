package oryce.highload.consumer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import oryce.highload.consumer.sink.RepositorySink;
import oryce.highload.consumer.source.SaleEvent;

import static java.util.Objects.requireNonNull;

public class Application implements AutoCloseable {

    private final StreamExecutionEnvironment environment;
    private final KafkaSource<SaleEvent> source;
    private final RepositorySink sink;

    public Application(
        StreamExecutionEnvironment environment,
        KafkaSource<SaleEvent> source,
        RepositorySink sink
    ) {
        this.environment = requireNonNull(environment, "environment");
        this.source = requireNonNull(source, "source");
        this.sink = requireNonNull(sink, "sink");
    }

    public void run() throws Exception {
        environment.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source")
            .sinkTo(sink);
        environment.execute("Kafka to PostgreSQL normalization job");
    }

    @Override
    public void close() {
        // TODO (12.05.26, ~oryce):
        //   Future-proofing.
    }
}
