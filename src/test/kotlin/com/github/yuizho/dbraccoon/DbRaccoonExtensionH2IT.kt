package com.github.yuizho.dbraccoon

import com.github.yuizho.dbraccoon.annotation.*
import org.assertj.core.api.Assertions.assertThat
import org.h2.jdbcx.JdbcDataSource
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*

class DbRaccoonExtensionH2IT {
    companion object {
        val dataSource = JdbcDataSource().also {
            it.setUrl("jdbc:h2:file:./target/db-raccoon")
            it.user = "sa"
        }

        @JvmField
        @RegisterExtension
        val sampleExtension = DbRaccoonExtension(
                dataSource = dataSource
        )
    }

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
    @Nested
    @DisplayName("The test cases that @DataSet is applied")
    inner class DataSetCases {
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
                    Col("decimal_c", "9223372036854775807"),
                    Col("double_c", "1.111"),
                    Col("float_c", "2.222"),
                    Col("real_c", "3.5"),
                    Col("time_c", "12:33:49.123"),
                    Col("date_c", "2014-01-10"),
                    Col("timestamp_c", "2014-01-10 12:33:49.123"),
                    Col("timestamp_with_time_zone_c", "2014-01-10 12:33:49+09"),
                    Col("binary_c", "YWJjZGVmZzE="),
                    Col("varbinary_c", "abcdefghあいうえお1"),
                    Col("longvarbinary_c", "abcdefghあいうえお2"),
                    Col("varchar_c", "abcdefghあいうえお3"),
                    Col("longvarchar_c", "abcdefghあいうえお4"),
                    Col("char_c", "abcdefghあいうえお5"),
                    Col("blob_c", "YWJjZGVmZzI="),
                    Col("clob_c", "abcdefghあいうえお6"),
                    Col("bit_c", "true")
                ])
            ], [
                // When TypeHint is not appied to BINARY Column,
                // the Column is scanned as BINARY
                // TODO: BINARY TYPE might be almost same as VARBINARY...(not BLOB)
                TypeHint("binary_c", ColType.BINARY)]
            )
        ])
        fun `clean-insert works when @TypeHint is not applied (except binary columns)`() {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT * FROM type").use { rs ->
                        rs.next()
                        assertThat(rs.getInt(1)).isEqualTo(2147483647)
                        assertThat(rs.getBoolean(2)).isTrue()
                        assertThat(rs.getShort(3)).isEqualTo(127)
                        assertThat(rs.getInt(4)).isEqualTo(32767)
                        assertThat(rs.getLong(5)).isEqualTo(9223372036854775807L)
                        assertThat(rs.getBigDecimal(6)).isEqualTo(BigDecimal("9223372036854775807"))
                        assertThat(rs.getDouble(7)).isEqualTo(1.111)
                        assertThat(rs.getDouble(8)).isEqualTo(2.222)
                        assertThat(rs.getDouble(9)).isEqualTo(3.5)
                        val expectedTime = LocalTime.parse(
                                "12:33:49.123456789",
                                DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")
                        )
                        assertThat(rs.getTime(10)).isEqualTo(java.sql.Time.valueOf(expectedTime))
                        val expectedDate = LocalDate.parse(
                                "2014-01-10",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd")
                        )
                        assertThat(rs.getDate(11)).isEqualTo(java.sql.Date.valueOf(expectedDate))
                        val expectedDateTime = LocalDateTime.parse(
                                "2014-01-10 12:33:49.123",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                        )
                        assertThat(rs.getTimestamp(12)).isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
                        val expecteDOffsetDateTime = OffsetDateTime.parse(
                                "2014-01-10 12:33:49+0900",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX")
                        ).toInstant()
                        assertThat(rs.getTimestamp(13).toInstant()).isEqualTo(expecteDOffsetDateTime)
                        assertThat(rs.getBytes(14)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzE="))
                        assertThat(rs.getBytes(15)).isEqualTo("abcdefghあいうえお1".toByteArray())
                        assertThat(rs.getBytes(16)).isEqualTo("abcdefghあいうえお2".toByteArray())
                        assertThat(rs.getString(17)).isEqualTo("abcdefghあいうえお3")
                        assertThat(rs.getString(18)).isEqualTo("abcdefghあいうえお4")
                        assertThat(rs.getString(19)).isEqualTo("abcdefghあいうえお5")
                        assertThat(rs.getBytes(20)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzI="))
                        assertThat(rs.getString(21)).isEqualTo("abcdefghあいうえお6")
                        assertThat(rs.getBoolean(22)).isTrue()
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
                    Col("decimal_c", "9223372036854775807"),
                    Col("double_c", "1.111"),
                    Col("float_c", "2.222"),
                    Col("real_c", "3.5"),
                    Col("time_c", "12:33:49.123"),
                    Col("date_c", "2014-01-10"),
                    Col("timestamp_c", "2014-01-10 12:33:49.123"),
                    Col("timestamp_with_time_zone_c", "2014-01-10 12:33:49+09"),
                    Col("binary_c", "YWJjZGVmZzE="),
                    Col("varbinary_c", "abcdefghあいうえお1"),
                    Col("longvarbinary_c", "abcdefghあいうえお2"),
                    Col("varchar_c", "abcdefghあいうえお3"),
                    Col("longvarchar_c", "abcdefghあいうえお4"),
                    Col("char_c", "abcdefghあいうえお5"),
                    Col("blob_c", "YWJjZGVmZzI="),
                    Col("clob_c", "abcdefghあいうえお6"),
                    Col("bit_c", "true")
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
                TypeHint("timestamp_with_time_zone_c", ColType.TIMESTAMP_WITH_TIMEZONE),
                TypeHint("binary_c", ColType.BINARY),
                TypeHint("varbinary_c", ColType.VARBINARY),
                TypeHint("longvarbinary_c", ColType.LONGVARBINARY),
                TypeHint("varchar_c", ColType.VARCHAR),
                TypeHint("longvarchar_c", ColType.LONGVARCHAR),
                TypeHint("char_c", ColType.CHAR),
                TypeHint("blob_c", ColType.BLOB),
                TypeHint("clob_c", ColType.CLOB),
                TypeHint("bit_c", ColType.BIT)
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
                        assertThat(rs.getBigDecimal(6)).isEqualTo(BigDecimal("9223372036854775807"))
                        assertThat(rs.getDouble(7)).isEqualTo(1.111)
                        assertThat(rs.getDouble(8)).isEqualTo(2.222)
                        assertThat(rs.getDouble(9)).isEqualTo(3.5)
                        val expectedTime = LocalTime.parse(
                                "12:33:49.123456789",
                                DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")
                        )
                        assertThat(rs.getTime(10)).isEqualTo(java.sql.Time.valueOf(expectedTime))
                        val expectedDate = LocalDate.parse(
                                "2014-01-10",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd")
                        )
                        assertThat(rs.getDate(11)).isEqualTo(java.sql.Date.valueOf(expectedDate))
                        val expectedDateTime = LocalDateTime.parse(
                                "2014-01-10 12:33:49.123",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                        )
                        assertThat(rs.getTimestamp(12)).isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
                        val expecteDOffsetDateTime = OffsetDateTime.parse(
                                "2014-01-10 12:33:49+0900",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX")
                        ).toInstant()
                        assertThat(rs.getTimestamp(13).toInstant()).isEqualTo(expecteDOffsetDateTime)
                        assertThat(rs.getBytes(14)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzE="))
                        assertThat(rs.getBytes(15)).isEqualTo("abcdefghあいうえお1".toByteArray())
                        assertThat(rs.getBytes(16)).isEqualTo("abcdefghあいうえお2".toByteArray())
                        assertThat(rs.getString(17)).isEqualTo("abcdefghあいうえお3")
                        assertThat(rs.getString(18)).isEqualTo("abcdefghあいうえお4")
                        assertThat(rs.getString(19)).isEqualTo("abcdefghあいうえお5")
                        assertThat(rs.getBytes(20)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzI="))
                        assertThat(rs.getString(21)).isEqualTo("abcdefghあいうえお6")
                        assertThat(rs.getBoolean(22)).isTrue()
                    }
                }
            }
        }
    }

    @CsvDataSet([
        CsvTable("parent", [
            "id, name",
            "2, class-parent"
        ], ["id"]),
        CsvTable("child", [
            "id, name, parent_id",
            "2, class-child, 2"
        ], ["id"], [TypeHint("id", ColType.INTEGER)])
    ])
    @Nested
    @DisplayName("The test cases that @CsvDataSet is applied")
    inner class CsvDataSetCases {
        @Test
        @CsvDataSet([
            CsvTable("parent", [
                "id, name",
                "1, method-parent"
            ], ["id"]),
            CsvTable("child", [
                "id, name, parent_id",
                "1, method-child, 1"
            ], ["id"])
        ])
        fun `clean-insert works when @CsvDataSet is applied to a method`() {
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
        fun `clean-insert works when @CsvDataSet is applied to a class`() {
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
        @CsvDataSet([
            CsvTable("NULL_ABLE", [
                "id, name",
                "1, [null]"
            ], ["id"])
        ])
        fun `null value insert works when @CsvDataSet is applied to a method`() {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT id, name FROM null_able").use { rs ->
                        rs.next()
                        assertThat(rs.getInt("id")).isEqualTo(1)
                        assertThat(rs.getString("name")).isNull()
                    }
                }
            }
        }

        @Test
        @CsvDataSet([
            CsvTable("type", [
                "INT_C,BOOLEAN_C,TINYINT_C,SMALLINT_C,BIGINT_C,DECIMAL_C,DOUBLE_C,FLOAT_C,REAL_C,TIME_C,DATE_C,TIMESTAMP_C,TIMESTAMP_WITH_TIME_ZONE_C,BINARY_C,VARBINARY_C,LONGVARBINARY_C,VARCHAR_C,LONGVARCHAR_C,CHAR_C,BLOB_C,CLOB_C,BIT_C",
                "2147483647,true,127,32767,9223372036854775807,9223372036854775807,1.111,2.222,3.5,12:33:49,2014-01-10,2014-01-10 12:33:49.123,2014-01-10 12:33:49+09,YWJjZGVmZzE=,abcdefghあいうえお1,abcdefghあいうえお2,abcdefghあいうえお3,abcdefghあいうえお4,abcdefghあいうえお5,YWJjZGVmZzI=,abcdefghあいうえお6,true"
            ], ["INT_C"], [
                // When TypeHint is not applied to BINARY Column,
                // the Column is scanned as BINARY
                TypeHint("BINARY_C", ColType.BINARY)
            ])
        ])
        fun `clean-insert works when @TypeHint is not applied (except binary columns)`() {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT * FROM type").use { rs ->
                        rs.next()
                        assertThat(rs.getInt(1)).isEqualTo(2147483647)
                        assertThat(rs.getBoolean(2)).isTrue()
                        assertThat(rs.getShort(3)).isEqualTo(127)
                        assertThat(rs.getInt(4)).isEqualTo(32767)
                        assertThat(rs.getLong(5)).isEqualTo(9223372036854775807L)
                        assertThat(rs.getBigDecimal(6)).isEqualTo(BigDecimal("9223372036854775807"))
                        assertThat(rs.getDouble(7)).isEqualTo(1.111)
                        assertThat(rs.getDouble(8)).isEqualTo(2.222)
                        assertThat(rs.getDouble(9)).isEqualTo(3.5)
                        val expectedTime = LocalTime.parse(
                                "12:33:49.123456789",
                                DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")
                        )
                        assertThat(rs.getTime(10)).isEqualTo(java.sql.Time.valueOf(expectedTime))
                        val expectedDate = LocalDate.parse(
                                "2014-01-10",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd")
                        )
                        assertThat(rs.getDate(11)).isEqualTo(java.sql.Date.valueOf(expectedDate))
                        val expectedDateTime = LocalDateTime.parse(
                                "2014-01-10 12:33:49.123",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                        )
                        assertThat(rs.getTimestamp(12)).isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
                        val expecteDOffsetDateTime = OffsetDateTime.parse(
                                "2014-01-10 12:33:49+0900",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX")
                        ).toInstant()
                        assertThat(rs.getTimestamp(13).toInstant()).isEqualTo(expecteDOffsetDateTime)
                        assertThat(rs.getBytes(14)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzE="))
                        assertThat(rs.getBytes(15)).isEqualTo("abcdefghあいうえお1".toByteArray())
                        assertThat(rs.getBytes(16)).isEqualTo("abcdefghあいうえお2".toByteArray())
                        assertThat(rs.getString(17)).isEqualTo("abcdefghあいうえお3")
                        assertThat(rs.getString(18)).isEqualTo("abcdefghあいうえお4")
                        assertThat(rs.getString(19)).isEqualTo("abcdefghあいうえお5")
                        assertThat(rs.getBytes(20)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzI="))
                        assertThat(rs.getString(21)).isEqualTo("abcdefghあいうえお6")
                        assertThat(rs.getBoolean(22)).isTrue()
                    }
                }
            }
        }

        @Test
        @CsvDataSet([
            CsvTable("type", [
                "INT_C,BOOLEAN_C,TINYINT_C,SMALLINT_C,BIGINT_C,DECIMAL_C,DOUBLE_C,FLOAT_C,REAL_C,TIME_C,DATE_C,TIMESTAMP_C,TIMESTAMP_WITH_TIME_ZONE_C,BINARY_C,VARBINARY_C,LONGVARBINARY_C,VARCHAR_C,LONGVARCHAR_C,CHAR_C,BLOB_C,CLOB_C,BIT_C",
                "2147483647,true,127,32767,9223372036854775807,9223372036854775807,1.111,2.222,3.5,12:33:49,2014-01-10,2014-01-10 12:33:49.123,2014-01-10 12:33:49+09,YWJjZGVmZzE=,abcdefghあいうえお1,abcdefghあいうえお2,abcdefghあいうえお3,abcdefghあいうえお4,abcdefghあいうえお5,YWJjZGVmZzI=,abcdefghあいうえお6,true"
            ], ["INT_C"], [
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
                TypeHint("timestamp_with_time_zone_c", ColType.TIMESTAMP_WITH_TIMEZONE),
                TypeHint("binary_c", ColType.BINARY),
                TypeHint("varbinary_c", ColType.VARBINARY),
                TypeHint("longvarbinary_c", ColType.LONGVARBINARY),
                TypeHint("varchar_c", ColType.VARCHAR),
                TypeHint("longvarchar_c", ColType.LONGVARCHAR),
                TypeHint("char_c", ColType.CHAR),
                TypeHint("blob_c", ColType.BLOB),
                TypeHint("clob_c", ColType.CLOB),
                TypeHint("bit_c", ColType.BIT)
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
                        assertThat(rs.getBigDecimal(6)).isEqualTo(BigDecimal("9223372036854775807"))
                        assertThat(rs.getDouble(7)).isEqualTo(1.111)
                        assertThat(rs.getDouble(8)).isEqualTo(2.222)
                        assertThat(rs.getDouble(9)).isEqualTo(3.5)
                        val expectedTime = LocalTime.parse(
                                "12:33:49.123456789",
                                DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")
                        )
                        assertThat(rs.getTime(10)).isEqualTo(java.sql.Time.valueOf(expectedTime))
                        val expectedDate = LocalDate.parse(
                                "2014-01-10",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd")
                        )
                        assertThat(rs.getDate(11)).isEqualTo(java.sql.Date.valueOf(expectedDate))
                        val expectedDateTime = LocalDateTime.parse(
                                "2014-01-10 12:33:49.123",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                        )
                        assertThat(rs.getTimestamp(12)).isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
                        val expecteDOffsetDateTime = OffsetDateTime.parse(
                                "2014-01-10 12:33:49+0900",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX")
                        ).toInstant()
                        assertThat(rs.getTimestamp(13).toInstant()).isEqualTo(expecteDOffsetDateTime)
                        assertThat(rs.getBytes(14)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzE="))
                        assertThat(rs.getBytes(15)).isEqualTo("abcdefghあいうえお1".toByteArray())
                        assertThat(rs.getBytes(16)).isEqualTo("abcdefghあいうえお2".toByteArray())
                        assertThat(rs.getString(17)).isEqualTo("abcdefghあいうえお3")
                        assertThat(rs.getString(18)).isEqualTo("abcdefghあいうえお4")
                        assertThat(rs.getString(19)).isEqualTo("abcdefghあいうえお5")
                        assertThat(rs.getBytes(20)).isEqualTo(Base64.getDecoder().decode("YWJjZGVmZzI="))
                        assertThat(rs.getString(21)).isEqualTo("abcdefghあいうえお6")
                        assertThat(rs.getBoolean(22)).isTrue()
                    }
                }
            }
        }
    }
}