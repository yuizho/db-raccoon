package com.github.yuizho.dbraccoon;

import com.github.yuizho.dbraccoon.annotation.*;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@DataSet(testData = {
        @Table(name = "parent", rows = {
                @Row(columns = {
                        @Col(name = "id", value = "2", isId = true),
                        @Col(name = "name", value = "class-parent")
                })
        }),
        @Table(name = "child", rows = {
                @Row(columns = {
                        @Col(name = "id", value = "2", isId = true),
                        @Col(name = "name", value = "class-child"),
                        @Col(name = "parent_id", value = "2"),
                })
        })
})
public class DbRaccoonExtensionJavaIT {
    private final JdbcDataSource dataSource;

    @RegisterExtension
    final DbRaccoonExtension dbRaccoonExtension;

    {
        dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:file:./target/db-raccoon");
        dataSource.setUser("sa");
        dbRaccoonExtension = new DbRaccoonExtension.Builder(dataSource)
                .cleanupPhase(CleanupPhase.BEFORE_AND_AFTER_TEST)
                .cleanupStrategy(CleanupStrategy.USED_ROWS)
                .setUpQueries(Arrays.asList(
                        // https://www.h2database.com/html/commands.html#set_referential_integrity
                        "SET REFERENTIAL_INTEGRITY FALSE",
                        // https://www.h2database.com/html/commands.html#set_query_timeout
                        "SET QUERY_TIMEOUT 10000")
                )
                .tearDownQueries(Arrays.asList(
                        // https://www.h2database.com/html/commands.html#set_referential_integrity
                        "SET REFERENTIAL_INTEGRITY TRUE",
                        // https://www.h2database.com/html/commands.html#set_query_timeout
                        "SET QUERY_TIMEOUT 0")
                )
                .build();
    }

    @Test
    @CsvDataSet(testData = {
            @CsvTable(name = "child", rows = {
                    "id, name, parent_id",
                    "1, method-child, 1"
            }, id = "id")
    })
    public void cleanInsertWorksWhenDataSetIsAppliedToAMethod() throws Exception {
        // The child table has a foreign key constraint. But the foreign key check is disabled by setUpQueries.
        // That's why this test works fine.
        try (Statement stmt = dataSource.getConnection().createStatement();) {
            try (ResultSet rs = stmt.executeQuery("SELECT id, name, parent_id FROM child");) {
                rs.next();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("method-child");
                assertThat(rs.getInt("parent_id")).isEqualTo(1);
            }
        }
    }

    @Test
    public void cleanInsertWorksWhenDataSetIsAppliedToClass() throws Exception {
        try (Statement stmt = dataSource.getConnection().createStatement();) {
            try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM parent");) {
                rs.next();
                assertThat(rs.getInt("id")).isEqualTo(2);
                assertThat(rs.getString("name")).isEqualTo("class-parent");
            }
            try (ResultSet rs = stmt.executeQuery("SELECT id, name, parent_id FROM child");) {
                rs.next();
                assertThat(rs.getInt("id")).isEqualTo(2);
                assertThat(rs.getString("name")).isEqualTo("class-child");
                assertThat(rs.getInt("parent_id")).isEqualTo(2);
            }
        }
    }
}
