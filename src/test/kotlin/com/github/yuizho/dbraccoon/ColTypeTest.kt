package com.github.yuizho.dbraccoon

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import javax.sql.rowset.serial.SerialBlob
import javax.sql.rowset.serial.SerialClob

class ColTypeTest {
    @ParameterizedTest
    @ValueSource(strings = ["abcdefg", "あいうえお"])
    fun `VARBINARY#convert returns ByteArray`(param: String) {
        val actual = ColType.VARBINARY.convert(param)
        assertThat(actual)
                .isInstanceOf(ByteArray::class.java)
                .isEqualTo(param.toByteArray())
    }

    @ParameterizedTest
    @ValueSource(strings = ["abcdefg", "あいうえお"])
    fun `LONGVARBINARY#convert returns ByteArray`(param: String) {
        val actual = ColType.LONGVARBINARY.convert(param)
        assertThat(actual)
                .isInstanceOf(ByteArray::class.java)
                .isEqualTo(param.toByteArray())
    }

    @ParameterizedTest
    @ValueSource(strings = [
        // base64 strings
        "YWJjZGVmZw==",        // source value: abcdefg
        "44GC44GE44GG44GI44GK" // source value: あいうえお
    ])
    fun `BINARY#convert converts Base64String to ByteArray`(param: String) {
        val actual = ColType.BINARY.convert(param)
        assertThat(actual)
                .isInstanceOf(ByteArray::class.java)
                .isEqualTo(Base64.getDecoder().decode(param))
    }

    @ParameterizedTest
    @ValueSource(strings = [
        // base64 strings
        "YWJjZGVmZw==",        // source value: abcdefg
        "44GC44GE44GG44GI44GK" // source value: あいうえお
    ])
    fun `BLOB#convert converts Base64String to java-sql-Blob`(param: String) {
        val actual = ColType.BLOB.convert(param)
        assertThat(actual)
                .isInstanceOf(java.sql.Blob::class.java)
                .isEqualTo(
                        SerialBlob(Base64.getDecoder().decode(param))
                )
    }

    @ParameterizedTest
    @ValueSource(strings = ["true", "false"])
    fun `BOOLEAN#convert returns boolean`(param: String) {
        val actual = ColType.BOOLEAN.convert(param)
        assertThat(actual)
                .isInstanceOf(java.lang.Boolean::class.java)
                .isEqualTo(param.toBoolean())
    }

    @ParameterizedTest
    @ValueSource(strings = ["true", "false"])
    fun `BIT#convert returns boolean`(param: String) {
        val actual = ColType.BIT.convert(param)
        assertThat(actual)
                .isInstanceOf(java.lang.Boolean::class.java)
                .isEqualTo(param.toBoolean())
    }

    @ParameterizedTest
    @ValueSource(strings = ["1970-01-01", "2019-01-11", "2020-12-31"])
    fun `DATE#convert returns java-sql-Date`(param: String) {
        val actual = ColType.DATE.convert(param)
        assertThat(actual)
                .isInstanceOf(java.sql.Date::class.java)
                .isEqualTo(java.sql.Date.valueOf(param))
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "00:00:00",
        "12:50:59.1",
        "12:50:59.12",
        "12:50:59.123",
        "12:50:59.1234",
        "12:50:59.12345",
        "01:02:03.123456",
        "01:02:03.1234567",
        "01:02:03.12345678",
        "01:02:03.123456789"
    ])
    fun `TIME#convert returns java-sql-Time`(param: String) {
        val actual = ColType.TIME.convert(param)
        val expectedTime = LocalTime.parse(
                param,
                DateTimeFormatter.ofPattern(
                        "[HH:mm:ss.SSSSSSSSS][HH:mm:ss.SSSSSSSS][HH:mm:ss.SSSSSSSS][HH:mm:ss.SSSSSSS][HH:mm:ss.SSSSSS][HH:mm:ss.SSSSS][HH:mm:ss.SSSS][HH:mm:ss.SSS][HH:mm:ss.SS][HH:mm:ss.S][HH:mm:ss]"
                )
        )
        assertThat(actual)
                .isInstanceOf(java.sql.Time::class.java)
                .isEqualTo(java.sql.Time.valueOf(expectedTime))
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "1970-01-01 00:00:00",
        "2019-12-31 12:50:59.1",
        "2019-12-31 12:50:59.12",
        "2019-10-09 12:50:59.123",
        "2019-12-31 12:50:59.1234",
        "2019-12-31 12:50:59.12345",
        "2019-02-28 01:02:03.123456",
        "2019-12-31 01:02:03.1234567",
        "2019-12-31 01:02:03.12345678",
        "2019-12-31 01:02:03.123456789"
    ])
    fun `TIMESTAMP#convert returns java-sql-Timestamp`(param: String) {
        val actual = ColType.TIMESTAMP.convert(param)
        val expectedDateTime = LocalDateTime.parse(
                param,
                DateTimeFormatter.ofPattern(
                        "[yyyy-MM-dd HH:mm:ss.SSSSSSSSS][yyyy-MM-dd HH:mm:ss.SSSSSSSS][yyyy-MM-dd HH:mm:ss.SSSSSSSS][yyyy-MM-dd HH:mm:ss.SSSSSSS][yyyy-MM-dd HH:mm:ss.SSSSSS][yyyy-MM-dd HH:mm:ss.SSSSS][yyyy-MM-dd HH:mm:ss.SSSS][yyyy-MM-dd HH:mm:ss.SSS][yyyy-MM-dd HH:mm:ss.SS][yyyy-MM-dd HH:mm:ss.S][yyyy-MM-dd HH:mm:ss]"
                )
        )
        assertThat(actual)
                .isInstanceOf(java.sql.Timestamp::class.java)
                .isEqualTo(java.sql.Timestamp.valueOf(expectedDateTime))
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "1970-01-01 00:00:00+00",
        "2019-10-09 12:50:59+0000",
        "2019-02-28 12:50:59+00:00",
        "1970-01-01 00:00:00-00",
        "2019-10-09 12:50:59-0000",
        "2019-02-28 12:50:59-00:00",
        "1970-01-01 00:00:00+09",
        "2019-10-09 03:53:01+0900",
        "2019-12-31 12:54:59+11:30",
        "1970-01-01 00:00:00-09",
        "2019-10-09 03:53:01-0900",
        "2019-12-31 12:54:59-11:30"
    ])
    fun `TIMESTAMP_WITH_TIMEZONE#convert returns OffsetDateTime`(param: String) {
        val actual = ColType.TIMESTAMP_WITH_TIMEZONE.convert(param)
        assertThat(actual)
                .isInstanceOf(OffsetDateTime::class.java)
                .isEqualTo(
                        OffsetDateTime.parse(
                                param,
                                DateTimeFormatter.ofPattern("[yyyy-MM-dd HH:mm:ssxxx][yyyy-MM-dd HH:mm:ssxx][yyyy-MM-dd HH:mm:ssx]")
                        )
                )
    }

    @ParameterizedTest
    @ValueSource(strings = ["-32768", "0", "32767"])
    fun `TINYINT#convert returns short`(param: String) {
        val actual = ColType.TINYINT.convert(param)
        assertThat(actual)
                .isInstanceOf(java.lang.Short::class.java)
                .isEqualTo(param.toShort())
    }

    @ParameterizedTest
    @ValueSource(strings = ["-2147483648", "0", "2147483647"])
    fun `INTEGER#convert returns int`(param: String) {
        val actual = ColType.INTEGER.convert(param)
        assertThat(actual)
                .isInstanceOf(java.lang.Integer::class.java)
                .isEqualTo(param.toInt())
    }

    @ParameterizedTest
    @ValueSource(strings = ["-2147483648", "0", "2147483647"])
    fun `SMALLINT#convert returns int`(param: String) {
        val actual = ColType.SMALLINT.convert(param)
        assertThat(actual)
                .isInstanceOf(java.lang.Integer::class.java)
                .isEqualTo(param.toInt())
    }

    @ParameterizedTest
    @ValueSource(strings = ["-9223372036854775808", "0", "9223372036854775807"])
    fun `BIGINT#convert returns long`(param: String) {
        val actual = ColType.BIGINT.convert(param)
        assertThat(actual)
                .isInstanceOf(java.lang.Long::class.java)
                .isEqualTo(param.toLong())
    }

    @ParameterizedTest
    @ValueSource(strings = ["1.4E-45", "0", "3.4028235E38"])
    fun `REAL#convert returns float`(param: String) {
        val actual = ColType.REAL.convert(param)
        assertThat(actual)
                .isInstanceOf(java.lang.Float::class.java)
                .isEqualTo(param.toFloat())
    }

    @ParameterizedTest
    @ValueSource(strings = ["4.9E-324", "0", "1.7976931348623157E308"])
    fun `FLOAT#convert returns double`(param: String) {
        val actual = ColType.FLOAT.convert(param)
        assertThat(actual)
                .isInstanceOf(java.lang.Double::class.java)
                .isEqualTo(param.toDouble())
    }

    @ParameterizedTest
    @ValueSource(strings = ["4.9E-324", "0", "1.7976931348623157E308"])
    fun `DOUBLE#convert returns double`(param: String) {
        val actual = ColType.DOUBLE.convert(param)
        assertThat(actual)
                .isInstanceOf(java.lang.Double::class.java)
                .isEqualTo(param.toDouble())
    }

    @ParameterizedTest
    @ValueSource(strings = ["-9223372036854775808.0123456789", "0", "9223372036854775807.0123456789"])
    fun `DECIMAL#convert returns BigDecimal`(param: String) {
        val actual = ColType.DECIMAL.convert(param)
        assertThat(actual)
                .isInstanceOf(BigDecimal::class.java)
                .isEqualTo(param.toBigDecimal())
    }

    @ParameterizedTest
    @ValueSource(strings = ["abcdefg", "あいうえお"])
    fun `CHAR#convert returns String`(param: String) {
        val actual = ColType.CHAR.convert(param)
        assertThat(actual)
                .isInstanceOf(String::class.java)
                .isEqualTo(param)
    }

    @ParameterizedTest
    @ValueSource(strings = ["abcdefg", "あいうえお"])
    fun `VARCHAR#convert returns String`(param: String) {
        val actual = ColType.VARCHAR.convert(param)
        assertThat(actual)
                .isInstanceOf(String::class.java)
                .isEqualTo(param)
    }

    @ParameterizedTest
    @ValueSource(strings = ["abcdefg", "あいうえお"])
    fun `LONGVARCHAR#convert returns String`(param: String) {
        val actual = ColType.LONGVARCHAR.convert(param)
        assertThat(actual)
                .isInstanceOf(String::class.java)
                .isEqualTo(param)
    }

    @ParameterizedTest
    @ValueSource(strings = ["abcdefg", "あいうえお"])
    fun `CLOB#convert returns java-sql-Clob`(param: String) {
        val actual = ColType.CLOB.convert(param)
        assertThat(actual)
                .isInstanceOf(java.sql.Clob::class.java)
                .isEqualTo(SerialClob(param.toCharArray()))
    }

    @ParameterizedTest
    @ValueSource(strings = ["abcdefg", "あいうえお"])
    fun `DEFAULT#convert returns String`(param: String) {
        val actual = ColType.DEFAULT.convert(param)
        assertThat(actual)
                .isInstanceOf(String::class.java)
                .isEqualTo(param)
    }
}