package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Simple database migration manager. Inspired by Flyway, Liquibase, Morf etc, just trimmed down for
 * minimum dependencies.
 */
@Slf4j
class DefaultMigrationManager {

  /** Migrations can be dialect specific * */
  private static final List<Migration> MIGRATIONS =
      List.of(
          new Migration(
              1,
              "Create outbox table",
              "CREATE TABLE TXNO_OUTBOX (\n"
                  + "    id VARCHAR(36) PRIMARY KEY,\n"
                  + "    invocation TEXT,\n"
                  + "    nextAttemptTime TIMESTAMP(6),\n"
                  + "    attempts INT,\n"
                  + "    blacklisted BOOLEAN,\n"
                  + "    version INT\n"
                  + ")",
              Map.of(
                  Dialect.SQL_SERVER_2012,
                  "CREATE TABLE TXNO_OUTBOX (\n"
                      + "    id VARCHAR(36) PRIMARY KEY,\n"
                      + "    invocation NVARCHAR(MAX),\n"
                      + "    nextAttemptTime DATETIME2(6),\n"
                      + "    attempts INT,\n"
                      + "    blacklisted BIT,\n"
                      + "    version INT\n"
                      + ")")),
          new Migration(
              2,
              "Add unique request id",
              "ALTER TABLE TXNO_OUTBOX ADD COLUMN uniqueRequestId VARCHAR(100) NULL UNIQUE",
              Map.of(
                  Dialect.SQL_SERVER_2012,
                  // Unique constraint added in v9. If added here it would not allow multiple nulls.
                  "ALTER TABLE TXNO_OUTBOX ADD uniqueRequestId VARCHAR(100)")),
          new Migration(
              3,
              "Add processed flag",
              "ALTER TABLE TXNO_OUTBOX ADD COLUMN processed BOOLEAN",
              Map.of(Dialect.SQL_SERVER_2012, "ALTER TABLE TXNO_OUTBOX ADD processed BIT")),
          new Migration(
              4,
              "Add flush index",
              "CREATE INDEX IX_TXNO_OUTBOX_1 ON TXNO_OUTBOX (processed, blacklisted, nextAttemptTime)"),
          new Migration(
              5,
              "Increase size of uniqueRequestId",
              "ALTER TABLE TXNO_OUTBOX MODIFY COLUMN uniqueRequestId VARCHAR(250)",
              Map.of(
                  Dialect.POSTGRESQL_9,
                  "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId TYPE VARCHAR(250)",
                  Dialect.H2,
                  "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId VARCHAR(250)",
                  Dialect.SQL_SERVER_2012,
                  "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId VARCHAR(250)")),
          new Migration(
              6,
              "Rename column blacklisted to blocked",
              "ALTER TABLE TXNO_OUTBOX CHANGE COLUMN blacklisted blocked VARCHAR(250)",
              Map.of(
                  Dialect.POSTGRESQL_9,
                  "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked",
                  Dialect.H2,
                  "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked",
                  Dialect.SQL_SERVER_2012,
                  "EXEC sp_rename 'TXNO_OUTBOX.blacklisted', 'blocked', 'COLUMN'")),
          new Migration(
              7,
              "Add lastAttemptTime column to outbox",
              "ALTER TABLE TXNO_OUTBOX ADD COLUMN lastAttemptTime TIMESTAMP(6) NULL AFTER invocation",
              Map.of(
                  Dialect.POSTGRESQL_9,
                  "ALTER TABLE TXNO_OUTBOX ADD COLUMN lastAttemptTime TIMESTAMP(6)",
                  Dialect.SQL_SERVER_2012,
                  "ALTER TABLE TXNO_OUTBOX ADD lastAttemptTime DATETIME2(6)")),
          new Migration(
              8,
              "Update length of invocation column on outbox for MySQL dialects only.",
              "ALTER TABLE TXNO_OUTBOX MODIFY COLUMN invocation MEDIUMTEXT",
              Map.of(Dialect.POSTGRESQL_9, "", Dialect.H2, "", Dialect.SQL_SERVER_2012, "")),
          new Migration(
              9,
              "Add unique constraint that allows multiple nulls for uniqueRequestId column on outbox for SQLServer dialects only.",
              "",
              Map.of(
                  Dialect.SQL_SERVER_2012,
                  "CREATE UNIQUE INDEX UX_TXNO_OUTBOX_uniqueRequestId ON TXNO_OUTBOX (uniqueRequestId) WHERE uniqueRequestId IS NOT NULL")));

  static void migrate(TransactionManager transactionManager, @NotNull Dialect dialect) {
    transactionManager.inTransaction(
        transaction -> {
          try {
            int currentVersion = currentVersion(transaction.connection(), dialect);
            MIGRATIONS.stream()
                .filter(migration -> migration.version > currentVersion)
                .forEach(migration -> runSql(transaction.connection(), migration, dialect));
          } catch (Exception e) {
            throw new RuntimeException("Migrations failed", e);
          }
        });
  }

  @SneakyThrows
  private static void runSql(Connection connection, Migration migration, @NotNull Dialect dialect) {
    log.info("Running migration: {}", migration.name);
    try (Statement s = connection.createStatement()) {
      String sql = migration.sqlFor(dialect);
      if (sql != null && !sql.isBlank()) {
        s.execute(sql);
      }
      if (s.executeUpdate("UPDATE TXNO_VERSION SET version = " + migration.version) != 1) {
        s.execute("INSERT INTO TXNO_VERSION VALUES (" + migration.version + ")");
      }
    }
  }

  private static int currentVersion(Connection connection, @NotNull Dialect dialect)
      throws SQLException {
    createVersionTableIfNotExists(connection, dialect);
    try (Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery(dialect.getSelectCurrentVersionAndLockTable())) {
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    }
  }

  private static void createVersionTableIfNotExists(Connection connection, @NotNull Dialect dialect)
      throws SQLException {
    try (Statement s = connection.createStatement()) {
      s.execute(dialect.getCreateVersionTableIfNotExists());
      if (dialect == Dialect.SQL_SERVER_2012) {
        // SQL Server requires DLL to be committed before newly created objects can be referenced
        // in DML
        connection.commit();
      }
    }
  }

  @AllArgsConstructor
  private static final class Migration {
    private final int version;
    private final String name;
    private final String sql;
    private final Map<Dialect, String> dialectSpecific;

    Migration(int version, String name, String sql) {
      this(version, name, sql, Collections.emptyMap());
    }

    String sqlFor(Dialect dialect) {
      return dialectSpecific.getOrDefault(dialect, sql);
    }
  }
}
