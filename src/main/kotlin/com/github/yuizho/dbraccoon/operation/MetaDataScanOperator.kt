package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

internal class ColumnMetadataScanOperator(val scanners: List<ColumnMetadataScanner>) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(ColumnMetadataScanOperator::class.java)
    }

    fun execute(conn: Connection): ColumnMetadataByTable {
        conn.autoCommit = false
        return try {
            scanners.map { scanner ->
                scanner.tableName to scanner.execute(conn)
            }.toMap()
        } catch (ex: Exception) {
            conn.rollback()
            logger.info("rollback")
            throw DbRaccoonException(ex)
        }
    }
}

internal typealias ColumnMetadataByTable = Map<String, TypeByColumn>