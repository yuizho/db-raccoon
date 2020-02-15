package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

internal data class QueryOperator(val querySources: List<Query>) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(QueryOperator::class.java)
    }

    fun executeQueries(conn: Connection) {
        conn.autoCommit = false
        kotlin.runCatching { querySources.forEach { it.execute(conn) } }
                .onSuccess {
                    conn.commit()
                    logger.info("commit")
                }
                .onFailure { ex ->
                    conn.rollback()
                    logger.info("rollback")
                    throw DbRaccoonException(ex)
                }
    }
}