package oryce.highload.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class RecordProducer implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(RecordProducer.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final ObjectMapper mapper;

    public RecordProducer(
        KafkaProducer<String, String> producer,
        String topic,
        ObjectMapper mapper
    ) {
        this.producer = requireNonNull(producer, "producer");
        this.topic = requireNonNull(topic, "topic");
        this.mapper = requireNonNull(mapper, "mapper");
    }

    public void produce(Stream<JsonNode> records) {
        records.forEach((record) -> {
            var serializedRecord = mapper.writeValueAsString(record);
            var kafkaRecord = new ProducerRecord<String, String>(topic, serializedRecord);
            producer.send(kafkaRecord, (metadata, exception) -> {
                if (exception == null) LOGGER.trace("Produced record {}", metadata);
                else LOGGER.error("Exception while producing record", exception);
            });
        });
    }

    @Override
    public void close() {
        producer.close();
    }
}
