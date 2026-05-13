package oryce.highload.producer;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.MappingIterator;
import tools.jackson.dataformat.csv.CsvMapper;
import tools.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

public class RecordReader {

    private final List<Path> paths;

    public RecordReader(List<Path> paths) {
        this.paths = requireNonNull(paths, "paths");
    }

    private static class InputStreams implements Iterable<InputStream>, AutoCloseable {

        private final List<InputStream> streams = new ArrayList<>();

        public void open(Path path) throws IOException {
            InputStream stream = Files.newInputStream(path);
            streams.add(stream);
        }

        public void openAll(Collection<Path> paths) throws IOException {
            for (Path path : paths) {
                open(path);
            }
        }

        @Override
        public Iterator<InputStream> iterator() {
            return streams.iterator();
        }

        @Override
        public void close() throws IOException {
            Iterator<InputStream> iterator = streams.iterator();

            while (iterator.hasNext()) {
                InputStream stream = iterator.next();
                stream.close();
                iterator.remove();
            }
        }
    }

    // TODO (12.05.26, ~oryce):
    //   Open streams lazily.

    public Stream<JsonNode> stream() throws IOException {
        if (paths.isEmpty()) return Stream.empty();

        InputStreams inputStreams = new InputStreams();

        try {
            inputStreams.openAll(paths);

            Stream<JsonNode> result = null;
            CsvMapper mapper = new CsvMapper();
            CsvSchema schema = CsvSchema.emptySchema().withHeader();

            for (InputStream inputStream : inputStreams) {
                MappingIterator<JsonNode> iterator = mapper.readerFor(JsonNode.class)
                    .with(schema)
                    .readValues(inputStream);
                Stream<JsonNode> stream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(
                        iterator,
                        Spliterator.ORDERED | Spliterator.NONNULL
                    ),
                    false
                );
                result = result != null ? Stream.concat(result, stream) : stream;
            }

            assert result != null : "must have at least one stream";
            return result.onClose(() -> {
                try {
                    inputStreams.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (Exception e) {
            try {
                inputStreams.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }
}
