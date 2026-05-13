package oryce.highload.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import tools.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class Configuration {

    private final String bootstrapServers;
    private final String topic;
    private final String sourceDir;

    public Configuration(String bootstrapServers, String topic, String sourceDir) {
        this.bootstrapServers = requireNonNull(bootstrapServers, "bootstrap servers");
        this.topic = requireNonNull(topic, "topic");
        this.sourceDir = requireNonNull(sourceDir, "source dir");
    }

    public Application application() throws Exception {
        return new Application(recordReader(), recordProducer());
    }

    public RecordReader recordReader() throws Exception {
        try (Stream<Path> stream = Files.list(Paths.get(sourceDir))) {
            List<Path> paths = stream.filter(Files::isRegularFile).toList();
            return new RecordReader(paths);
        }
    }

    public RecordProducer recordProducer() {
        return new RecordProducer(kafkaProducer(), topic, objectMapper());
    }

    public KafkaProducer<String, String> kafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
