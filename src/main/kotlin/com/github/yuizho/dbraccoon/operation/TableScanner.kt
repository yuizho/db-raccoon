package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.ColType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

internal class TableScanner(private val tableName: String) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(TableScanner::class.java)
    }

    fun scanColumnTypes(conn: Connection): TypeByColumn {
        conn.createStatement().use { stmt ->
            val resultSet = stmt.executeQuery("select * from $tableName where 1 = 2")
            val metaData = resultSet.metaData
            val columnCount = metaData.columnCount
            val typeByColumn = (1..columnCount).map { i ->
                metaData.getColumnName(i).toLowerCase() to
                        ColType.valueOf(metaData.getColumnType(i))
            }.toMap()
            logger.info("typeByColumn: $tableName={$typeByColumn}")
            return typeByColumn
        }
    }
}

internal typealias TypeByColumn = Map<String, ColType>