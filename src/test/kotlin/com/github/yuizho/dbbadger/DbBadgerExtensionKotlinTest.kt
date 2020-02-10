package com.github.yuizho.dbbadger

import com.github.yuizho.dbbadger.annotation.Col
import com.github.yuizho.dbbadger.annotation.DataSet
import com.github.yuizho.dbbadger.annotation.Row
import com.github.yuizho.dbbadger.annotation.Table
import org.assertj.core.api.Assertions.assertThat
import org.h2.jdbcx.JdbcDataSource
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

@DataSet([
    Table("parent", [
        Row([
            Col("id", "2", true),
            Col("name", "class-parent")
        ])
    ]),
    Table("child", [
        Row([
            Col("id", "2", true),
            Col("name", "class-child"),
            Col("parent_id", "2")
        ])
    ])
])
class DbBadgerExtensionKotlinTest {
    companion object {
        val dataSource = JdbcDataSource().also {
            it.setUrl("jdbc:h2:file:./target/db-badger")
            it.user = "sa"
        }

        @JvmField
        @RegisterExtension
        val sampleExtension = DbBadgerExtension(
                dataSource = dataSource
        )
    }

    @Test
    @DataSet([
        Table("parent", [
            Row([
                Col("id", "1", true),
                Col("name", "method-parent")
            ])
        ]),
        Table("child", [
            Row([
                Col("id", "1", true),
                Col("name", "method-child"),
                Col("parent_id", "1")
            ])
        ])
    ])
    fun `clean-insert works when @DataSet is applied to a method`() {
        dataSource.connection.createStatement().use { stmt ->
            stmt.executeQuery("SELECT id, name FROM parent").use { rs ->
                rs.next()
                assertThat(rs.getInt("id")).isEqualTo(1)
                assertThat(rs.getString("name")).isEqualTo("method-parent")
            }
            stmt.executeQuery("SELECT id, name, parent_id FROM child").use { rs ->
                rs.next()
                assertThat(rs.getInt("id")).isEqualTo(1)
                assertThat(rs.getString("name")).isEqualTo("method-child")
                assertThat(rs.getInt("parent_id")).isEqualTo(1)
            }
        }
    }

    @Test
    fun `clean-insert works when @DataSet is applied to a class`() {
        dataSource.connection.createStatement().use { stmt ->
            stmt.executeQuery("SELECT id, name FROM parent").use { rs ->
                rs.next()
                assertThat(rs.getInt("id")).isEqualTo(2)
                assertThat(rs.getString("name")).isEqualTo("class-parent")
            }
            stmt.executeQuery("SELECT id, name, parent_id FROM child").use { rs ->
                rs.next()
                assertThat(rs.getInt("id")).isEqualTo(2)
                assertThat(rs.getString("name")).isEqualTo("class-child")
                assertThat(rs.getInt("parent_id")).isEqualTo(2)
            }
        }
    }
}