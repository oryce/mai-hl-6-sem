package oryce.highload.consumer;

import org.apache.flink.api.java.utils.ParameterTool;

public class Main {

    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        Configuration configuration = new Configuration(
            params.get("group.id"),
            new Configuration.KafkaConfig(
                params.get("kafka.bootstrap.servers"),
                params.get("kafka.topic")
            ),
            new Configuration.DatabaseConfig(
                params.get("database.url"),
                params.get("database.username"),
                params.get("database.password")
            )
        );

        try (Application application = configuration.application()) {
            application.run();
        } catch (Exception e) {
            throw new RuntimeException("Exception configuring application", e);
        }
    }
}
