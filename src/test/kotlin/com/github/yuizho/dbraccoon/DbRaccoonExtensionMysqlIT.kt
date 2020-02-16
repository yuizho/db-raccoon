package com.github.yuizho.dbraccoon

import com.github.yuizho.dbraccoon.annotation.*
import com.mysql.cj.jdbc.MysqlDataSource
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
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
class DbRaccoonExtensionMysqlIT {
    companion object {
        val dataSource = MysqlDataSource().also {
            it.setUrl("jdbc:mysql://127.0.0.1:13306/testdb")
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
                Col("tinyint_c", "127"),
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
                Col("varbinary_c", "abcdefgh1"),
                Col("varchar_c", "abcdefgh2"),
                Col("char_c", "abcdefgh3"),
                Col("blob_c", "YWJjZGVmZzI="),
                Col("clob_c", "abcdefgh4"),
                Col("bit_c", "true"),
                Col("datetime_c", "2014-01-10 12:33:49.123456")
            ])
        ], [
            // When TypeHint is not appied to BLOB Column,
            // the Column is scanned as LONGVARBINARY
            TypeHint("blob_c", ColType.BLOB)]
        )
    ])
    fun `clean-insert works when @TypeHint is not applied (except blob column)`() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT * FROM type").use { rs ->
                    rs.next()
                    assertThat(rs.getInt(1)).isEqualTo(2147483647)
                    assertThat(rs.getBoolean(2)).isTrue()
                    assertThat(rs.getShort(3)).isEqualTo(127)
                    assertThat(rs.getInt(4)).isEqualTo(32767)
                    assertThat(rs.getLong(5)).isEqualTo(9223372036854775807L)
                    assertThat(rs.getBigDecimal(6)).isEqualTo(BigDecimal("1234567890.12345"))
                    assertThat(rs.getDouble(7)).isEqualTo(1.111)
                    assertThat(rs.getDouble(8)).isEqualTo(2.222)
                    assertThat(rs.getDouble(9)).isEqualTo(3.5)
                    val expectedTime = LocalTime.parse(
                            "12:33:49.123456",
                            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")
                    )
                    assertThat(rs.getTime(10)).isEqualTo(java.sql.Time.valueOf(expectedTime))
                    assertThat(rs.getDate(11)).isEqualTo(java.sql.Date.valueOf("2014-01-10"))
                    val expectedDateTime = LocalDateTime.parse(
                            "2014-01-10 12:33:49.123456",
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    )
                    assertThat(rs.getTimestamp(12)).isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
                    assertThat(rs.getBytes(13)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzE="))
                    assertThat(rs.getString(14)).isEqualTo("abcdefgh1")
                    assertThat(rs.getString(15)).isEqualTo("abcdefgh2")
                    assertThat(rs.getString(16)).isEqualTo("abcdefgh3")
                    assertThat(rs.getBytes(17)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzI="))
                    assertThat(rs.getString(18)).isEqualTo("abcdefgh4")
                    assertThat(rs.getBoolean(19)).isTrue()
                    assertThat(rs.getTimestamp(20)).isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
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
                Col("tinyint_c", "127"),
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
                Col("varbinary_c", "abcdefgh1"),
                Col("varchar_c", "abcdefgh2"),
                Col("char_c", "abcdefgh3"),
                Col("blob_c", "YWJjZGVmZzI="),
                Col("clob_c", "abcdefgh4"),
                Col("bit_c", "true"),
                Col("datetime_c", "2014-01-10 12:33:49.123456")
            ])
        ], [
            TypeHint("int_c", ColType.INTEGER),
            TypeHint("boolean_c", ColType.BOOLEAN),
            TypeHint("tinyint_c", ColType.TINYINT),
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
            TypeHint("varbinary_c", ColType.VARBINARY),
            TypeHint("varchar_c", ColType.VARCHAR),
            TypeHint("char_c", ColType.CHAR),
            TypeHint("blob_c", ColType.BLOB),
            TypeHint("clob_c", ColType.CLOB),
            TypeHint("bit_c", ColType.BIT),
            TypeHint("datetime_c", ColType.TIMESTAMP)
        ])
    ])
    fun `clean-insert works when @TypeHint is applied to columns`() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT * FROM type").use { rs ->
                    rs.next()
                    assertThat(rs.getInt(1)).isEqualTo(2147483647)
                    assertThat(rs.getBoolean(2)).isTrue()
                    assertThat(rs.getShort(3)).isEqualTo(127)
                    assertThat(rs.getInt(4)).isEqualTo(32767)
                    assertThat(rs.getLong(5)).isEqualTo(9223372036854775807L)
                    assertThat(rs.getBigDecimal(6)).isEqualTo(BigDecimal("1234567890.12345"))
                    assertThat(rs.getDouble(7)).isEqualTo(1.111)
                    assertThat(rs.getDouble(8)).isEqualTo(2.222)
                    assertThat(rs.getDouble(9)).isEqualTo(3.5)
                    val expectedTime = LocalTime.parse(
                            "12:33:49.123456",
                            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")
                    )
                    assertThat(rs.getTime(10)).isEqualTo(java.sql.Time.valueOf(expectedTime))
                    assertThat(rs.getDate(11)).isEqualTo(java.sql.Date.valueOf("2014-01-10"))
                    val expectedDateTime = LocalDateTime.parse(
                            "2014-01-10 12:33:49.123456",
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    )
                    assertThat(rs.getTimestamp(12)).isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
                    assertThat(rs.getBytes(13)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzE="))
                    assertThat(rs.getString(14)).isEqualTo("abcdefgh1")
                    assertThat(rs.getString(15)).isEqualTo("abcdefgh2")
                    assertThat(rs.getString(16)).isEqualTo("abcdefgh3")
                    assertThat(rs.getBytes(17)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzI="))
                    assertThat(rs.getString(18)).isEqualTo("abcdefgh4")
                    assertThat(rs.getBoolean(19)).isTrue()
                    assertThat(rs.getTimestamp(20)).isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
                }
            }
        }
    }
}