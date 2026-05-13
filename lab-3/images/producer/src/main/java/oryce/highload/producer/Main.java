package oryce.highload.producer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Main {

    static void main(String[] args) throws ParseException {
        Options options = new Options();

        Option bootstrapServersOption = Option.builder()
            .longOpt("bootstrap-servers")
            .desc("Kafka bootstrap servers")
            .hasArg()
            .required()
            .get();
        options.addOption(bootstrapServersOption);

        Option topicOption = Option.builder()
            .longOpt("topic")
            .desc("Kafka topic")
            .hasArg()
            .required()
            .get();
        options.addOption(topicOption);

        Option sourceDirOption = Option.builder()
            .longOpt("source-dir")
            .desc("Path to directory containing source files")
            .hasArg()
            .required()
            .get();
        options.addOption(sourceDirOption);

        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine commandLine = commandLineParser.parse(options, args);

        Configuration configuration = new Configuration(
            commandLine.getOptionValue(bootstrapServersOption),
            commandLine.getOptionValue(topicOption),
            commandLine.getOptionValue(sourceDirOption)
        );

        try (Application application = configuration.application()) {
            application.run();
        } catch (Exception e) {
            throw new RuntimeException("Exception configuring application", e);
        }
    }
}
