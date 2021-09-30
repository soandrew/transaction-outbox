package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Dialect;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestSqlServer2017 extends AbstractAcceptanceTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2017-latest")
              .acceptLicense()
              .withStartupTimeout(Duration.ofHours(1));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(Dialect.SQL_SERVER_2012)
        .driverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }
}
