package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.ColType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

internal class ColumnMetadataScanner(val tableName: String) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(ColumnMetadataScanner::class.java)
    }

    fun execute(conn: Connection): TypeByColumn {
        conn.createStatement().use { stmt ->
            val resultSet = stmt.executeQuery("SELECT * FROM $tableName WHERE 1 = 2")
            val metadata = resultSet.metaData
            val columnCount = metadata.columnCount
            val typeByColumn = (1..columnCount).map { i ->
                metadata.getColumnName(i).toLowerCase() to
                        ColType.valueOf(metadata.getColumnType(i))
            }.toMap()
            logger.info("typeByColumn: $tableName={$typeByColumn}")
            return typeByColumn
        }
    }
}

internal typealias TypeByColumn = Map<String, ColType>