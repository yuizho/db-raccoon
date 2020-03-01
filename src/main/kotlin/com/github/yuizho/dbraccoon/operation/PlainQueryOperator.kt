package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.exception.DbRaccoonDataSetException
import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

internal data class PlainQueryOperator(val queries: List<String>) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(PlainQueryOperator::class.java)
    }

    fun executeQueries(conn: Connection) {
        conn.autoCommit = false
        kotlin.runCatching {
            conn.createStatement().use { stmt ->
                queries.forEach { query -> stmt.addBatch(query) }
                logger.info("[queries] $queries")
                stmt.executeBatch()
            }
        }.onSuccess {
            conn.commit()
            logger.info("commit")
        }.onFailure { ex ->
            conn.rollback()
            logger.info("rollback")
            when (ex) {
                is DbRaccoonDataSetException -> throw ex
                else -> throw DbRaccoonException(ex)
            }
        }
    }
}