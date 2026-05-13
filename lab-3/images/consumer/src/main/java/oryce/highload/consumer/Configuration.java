package oryce.highload.consumer;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import oryce.highload.consumer.sink.RepositorySink;
import oryce.highload.consumer.sink.entity.JdbcRepositoryFactory;
import oryce.highload.consumer.sink.entity.RepositoryFactory;
import oryce.highload.consumer.source.SaleEvent;
import oryce.highload.consumer.source.SaleEventDeserializationSchema;

import static java.util.Objects.requireNonNull;

public class Configuration {

    private final String groupId;
    private final KafkaConfig kafkaConfig;
    private final DatabaseConfig databaseConfig;

    public Configuration(
        String groupId,
        KafkaConfig kafkaConfig,
        DatabaseConfig databaseConfig
    ) {
        this.groupId = requireNonNull(groupId, "group ID");
        this.kafkaConfig = requireNonNull(kafkaConfig, "Kafka config");
        this.databaseConfig = requireNonNull(databaseConfig, "database config");
    }

    public Application application() {
        return new Application(environment(), source(), sink());
    }

    public StreamExecutionEnvironment environment() {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(10_000);
        return environment;
    }

    public KafkaSource<SaleEvent> source() {
        return KafkaSource.<SaleEvent>builder()
            .setBootstrapServers(kafkaConfig.bootstrapServers())
            .setTopics(kafkaConfig.topic())
            .setGroupId(groupId)
            .setValueOnlyDeserializer(deserializationSchema())
            .build();
    }

    public SaleEventDeserializationSchema deserializationSchema() {
        return new SaleEventDeserializationSchema();
    }

    public RepositorySink sink() {
        return new RepositorySink(repositoryFactory());
    }

    public RepositoryFactory repositoryFactory() {
        return new JdbcRepositoryFactory(
            databaseConfig.url(),
            databaseConfig.username(),
            databaseConfig.password()
        );
    }

    public record KafkaConfig(
        String bootstrapServers,
        String topic
    ) {
    }

    public record DatabaseConfig(
        String url,
        String username,
        String password
    ) {
    }
}
