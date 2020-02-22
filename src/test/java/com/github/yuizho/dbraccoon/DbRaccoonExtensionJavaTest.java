package com.github.yuizho.dbraccoon;

import com.github.yuizho.dbraccoon.annotation.Col;
import com.github.yuizho.dbraccoon.annotation.DataSet;
import com.github.yuizho.dbraccoon.annotation.Row;
import com.github.yuizho.dbraccoon.annotation.Table;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@DataSet(testData = {
        @Table(name = "parent", rows = {
                @Row(values = {
                        @Col(name = "id", value = "2", isId = true),
                        @Col(name = "name", value = "class-parent")
                })
        }),
        @Table(name = "child", rows = {
                @Row(values = {
                        @Col(name = "id", value = "2", isId = true),
                        @Col(name = "name", value = "class-child"),
                        @Col(name = "parent_id", value = "2"),
                })
        })
})
public class DbRaccoonExtensionJavaTest {
    private final JdbcDataSource dataSource;
    @RegisterExtension
    DbRaccoonExtension dbRaccoonExtension;

    {
        dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:file:./target/db-raccoon");
        dataSource.setUser("sa");
        dbRaccoonExtension = new DbRaccoonExtension(dataSource);
    }

    @Test
    @DataSet(testData = {
            @Table(name = "parent", rows = {
                    @Row(values = {
                            @Col(name = "id", value = "1", isId = true),
                            @Col(name = "name", value = "method-parent")
                    })
            }),
            @Table(name = "child", rows = {
                    @Row(values = {
                            @Col(name = "id", value = "1", isId = true),
                            @Col(name = "name", value = "method-child"),
                            @Col(name = "parent_id", value = "1"),
                    })
            })
    })
    public void cleanInsertWorksWhenDataSetIsAppliedToAMethod() throws Exception {
        try (Statement stmt = dataSource.getConnection().createStatement();) {
            try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM parent");) {
                rs.next();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("method-parent");
            }
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
