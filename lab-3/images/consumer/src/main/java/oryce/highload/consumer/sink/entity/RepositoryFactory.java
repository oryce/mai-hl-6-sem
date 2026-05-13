package oryce.highload.consumer.sink.entity;

import java.io.IOException;
import java.io.Serializable;

@FunctionalInterface
public interface RepositoryFactory extends Serializable {

    Repository repository() throws IOException;
}
