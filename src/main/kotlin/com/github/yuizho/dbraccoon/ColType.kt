package com.github.yuizho.dbraccoon

import java.sql.Types
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import javax.sql.rowset.serial.SerialBlob
import javax.sql.rowset.serial.SerialClob

enum class ColType(internal val sqlType: Int) {
    BINARY(Types.BINARY),
    VARBINARY(Types.VARBINARY),
    LONGVARBINARY(Types.LONGVARBINARY),
    BLOB(Types.BLOB),

    BOOLEAN(Types.BOOLEAN),
    BIT(Types.BIT),

    DATE(Types.DATE),
    TIME(Types.TIME),
    TIMESTAMP(Types.TIMESTAMP),
    TIMESTAMP_WITH_TIMEZONE(Types.TIMESTAMP_WITH_TIMEZONE),

    BIGINT(Types.BIGINT),
    DECIMAL(Types.DECIMAL),
    DOUBLE(Types.DOUBLE),
    FLOAT(Types.FLOAT),
    INTEGER(Types.INTEGER),
    REAL(Types.REAL),
    SMALLINT(Types.SMALLINT),
    TINYINT(Types.TINYINT),

    CHAR(Types.CHAR),
    CLOB(Types.CLOB),
    LONGVARCHAR(Types.LONGVARCHAR),
    VARCHAR(Types.VARCHAR),

    DEFAULT(Types.NULL);

    companion object {
        fun valueOf(sqlType: Int): ColType =
                values().findLast { it.sqlType == sqlType } ?: DEFAULT
    }
}

internal fun ColType.convert(value: String): Any {
    return when (this) {
        ColType.VARBINARY,
        ColType.LONGVARBINARY -> value.toByteArray()

        ColType.BINARY -> Base64.getDecoder().decode(value)
        ColType.BLOB -> SerialBlob(Base64.getDecoder().decode(value))

        ColType.BOOLEAN,
        ColType.BIT -> value.toBoolean()

        ColType.DATE -> {
            val date = LocalDate.parse(
                    value,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd")
            )
            return java.sql.Date.valueOf(date)
        }
        ColType.TIME -> {
            val time = LocalTime.parse(value)
            return java.sql.Time.valueOf(time)
        }
        ColType.TIMESTAMP -> {
            val dateTime = LocalDateTime.parse(
                    value,
                    DateTimeFormatter.ofPattern(
                            "[yyyy-MM-dd HH:mm:ss.SSSSSSSSS][yyyy-MM-dd HH:mm:ss.SSSSSSSS][yyyy-MM-dd HH:mm:ss.SSSSSSSS][yyyy-MM-dd HH:mm:ss.SSSSSSS][yyyy-MM-dd HH:mm:ss.SSSSSS][yyyy-MM-dd HH:mm:ss.SSSSS][yyyy-MM-dd HH:mm:ss.SSSS][yyyy-MM-dd HH:mm:ss.SSS][yyyy-MM-dd HH:mm:ss.SS][yyyy-MM-dd HH:mm:ss.S][yyyy-MM-dd HH:mm:ss]"
                    )
            )
            return java.sql.Timestamp.valueOf(dateTime)
        }
        ColType.TIMESTAMP_WITH_TIMEZONE -> {
            return OffsetDateTime.parse(
                    value,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX")
            )
        }

        ColType.TINYINT -> value.toShort()
        ColType.INTEGER,
        ColType.SMALLINT -> value.toInt()
        ColType.BIGINT -> value.toLong()
        ColType.REAL -> value.toFloat()
        ColType.FLOAT,
        ColType.DOUBLE -> value.toDouble()
        ColType.DECIMAL -> value.toBigDecimal()

        ColType.CHAR,
        ColType.VARCHAR,
        ColType.LONGVARCHAR -> value
        ColType.CLOB -> SerialClob(value.toCharArray())

        else -> value
    }
}