package com.github.yuizho.dbraccoon

import com.github.yuizho.dbraccoon.annotation.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.postgresql.ds.PGSimpleDataSource
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.*

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
    ], [TypeHint("id", ColType.INTEGER)])
])
class DbRaccoonExtensionPostgresIT {
    companion object {
        val dataSource = PGSimpleDataSource().also {
            it.setUrl("jdbc:postgresql://127.0.0.1:15432/testdb")
            it.user = "test"
            it.password = "password"
        }

        @JvmField
        @RegisterExtension
        val sampleExtension = DbRaccoonExtension(
                dataSource = dataSource,
                cleanupPhase = CleanupPhase.BEFORE_AND_AFTER_TEST
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
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
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
    }

    @Test
    fun `clean-insert works when @DataSet is applied to a class`() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
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

    @Test
    @DataSet([
        Table("type", [
            Row([
                Col("int_c", "2147483647", true),
                Col("boolean_c", "true"),
                Col("smallint_c", "32767"),
                Col("bigint_c", "9223372036854775807"),
                Col("decimal_c", "1234567890.12345"),
                Col("double_c", "1.111"),
                Col("float_c", "2.222"),
                Col("real_c", "3.5"),
                Col("time_c", "12:33:49.123456"),
                Col("date_c", "2014-01-10"),
                Col("timestamp_c", "2014-01-10 12:33:49.123456"),
                Col("binary_c", "YWJjZGVmZzE="),
                Col("varchar_c", "abcdefgh2"),
                Col("char_c", "abcdefgh333333"),
                Col("clob_c", "abcdefgh4")
            ])
        ], [
            // When TypeHint is not appied to DECIMAL Column,
            // the Column is scanned as DEFAULT
            TypeHint("decimal_c", ColType.DECIMAL)
        ]
        )
    ])
    fun `clean-insert works when @TypeHint is not applied (except decimal column)`() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT * FROM type").use { rs ->
                    rs.next()
                    assertThat(rs.getInt(1)).isEqualTo(2147483647)
                    assertThat(rs.getBoolean(2)).isTrue()
                    assertThat(rs.getShort(3)).isEqualTo(32767)
                    assertThat(rs.getLong(4)).isEqualTo(9223372036854775807L)
                    assertThat(rs.getBigDecimal(5)).isEqualTo(BigDecimal("1234567890.12345"))
                    assertThat(rs.getDouble(6)).isEqualTo(1.111)
                    assertThat(rs.getDouble(7)).isEqualTo(2.222)
                    assertThat(rs.getDouble(8)).isEqualTo(3.5)
                    val expectedTime = LocalTime.parse(
                            "12:33:49.123456",
                            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")
                    )
                    assertThat(rs.getTime(9)).isEqualTo(java.sql.Time.valueOf(expectedTime))
                    assertThat(rs.getDate(10)).isEqualTo(java.sql.Date.valueOf("2014-01-10"))
                    val expectedDateTime = LocalDateTime.parse(
                            "2014-01-10 12:33:49.123456",
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    )
                    assertThat(rs.getTimestamp(11)).isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
                    assertThat(rs.getBytes(12)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzE="))
                    assertThat(rs.getString(13)).isEqualTo("abcdefgh2")
                    assertThat(rs.getString(14)).isEqualTo("abcdefgh333333")
                    assertThat(rs.getString(15)).isEqualTo("abcdefgh4")
                }
            }
        }
    }

    @Test
    @DataSet([
        Table("type", [
            Row([
                Col("int_c", "2147483647", true),
                Col("boolean_c", "true"),
                Col("smallint_c", "32767"),
                Col("bigint_c", "9223372036854775807"),
                Col("decimal_c", "1234567890.12345"),
                Col("double_c", "1.111"),
                Col("float_c", "2.222"),
                Col("real_c", "3.5"),
                Col("time_c", "12:33:49.123456"),
                Col("date_c", "2014-01-10"),
                Col("timestamp_c", "2014-01-10 12:33:49.123456"),
                Col("binary_c", "YWJjZGVmZzE="),
                Col("varchar_c", "abcdefgh2"),
                Col("char_c", "abcdefgh333333"),
                Col("clob_c", "abcdefgh4")
            ])
        ], [
            TypeHint("int_c", ColType.INTEGER),
            TypeHint("boolean_c", ColType.BOOLEAN),
            TypeHint("smallint_c", ColType.SMALLINT),
            TypeHint("bigint_c", ColType.BIGINT),
            TypeHint("decimal_c", ColType.DECIMAL),
            TypeHint("double_c", ColType.DOUBLE),
            TypeHint("float_c", ColType.FLOAT),
            TypeHint("real_c", ColType.REAL),
            TypeHint("time_c", ColType.TIME),
            TypeHint("date_c", ColType.DATE),
            TypeHint("timestamp_c", ColType.TIMESTAMP),
            TypeHint("binary_c", ColType.BINARY),
            TypeHint("varchar_c", ColType.VARCHAR),
            TypeHint("char_c", ColType.CHAR),
            TypeHint("clob_c", ColType.CLOB)
        ])
    ])
    fun `clean-insert works when @TypeHint is applied to columns`() {
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT * FROM type").use { rs ->
                    rs.next()
                    assertThat(rs.getInt(1)).isEqualTo(2147483647)
                    assertThat(rs.getBoolean(2)).isTrue()
                    assertThat(rs.getShort(3)).isEqualTo(32767)
                    assertThat(rs.getLong(4)).isEqualTo(9223372036854775807L)
                    assertThat(rs.getBigDecimal(5)).isEqualTo(BigDecimal("1234567890.12345"))
                    assertThat(rs.getDouble(6)).isEqualTo(1.111)
                    assertThat(rs.getDouble(7)).isEqualTo(2.222)
                    assertThat(rs.getDouble(8)).isEqualTo(3.5)
                    val expectedTime = LocalTime.parse(
                            "12:33:49.123456",
                            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")
                    )
                    assertThat(rs.getTime(9)).isEqualTo(java.sql.Time.valueOf(expectedTime))
                    assertThat(rs.getDate(10)).isEqualTo(java.sql.Date.valueOf("2014-01-10"))
                    val expectedDateTime = LocalDateTime.parse(
                            "2014-01-10 12:33:49.123456",
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    )
                    assertThat(rs.getTimestamp(11)).isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
                    assertThat(rs.getBytes(12)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzE="))
                    assertThat(rs.getString(13)).isEqualTo("abcdefgh2")
                    assertThat(rs.getString(14)).isEqualTo("abcdefgh333333")
                    assertThat(rs.getClob(15).getSubString(1, 9)).isEqualTo("abcdefgh4")
                }
            }
        }
    }
}