package oryce.highload.consumer.sink.entity;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class JdbcRepositoryFactory implements RepositoryFactory {

    private final String url;
    private final String username;
    private final String password;

    public JdbcRepositoryFactory(String url, String username, String password) {
        this.url = requireNonNull(url, "database URL");
        this.username = requireNonNull(username, "database username");
        this.password = requireNonNull(password, "database password");
    }

    @Override
    public Repository repository() throws IOException {
        try {
            var connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false);
            return new JdbcRepository(connection);
        } catch (SQLException e) {
            throw new IOException("Can't create JDBC connection", e);
        }
    }
}
