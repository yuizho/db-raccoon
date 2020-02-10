package com.github.yuizho.dbbadger.operation

import com.github.yuizho.dbbadger.exception.DbBadgerException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

internal data class QueryOperator(val querySources: List<QuerySource>) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(QueryOperator::class.java)
    }

    fun executeQueries(conn: Connection) {
        conn.autoCommit = false
        kotlin.runCatching { querySources.forEach { it.executeQuery(conn) } }
                .onSuccess {
                    conn.commit()
                    logger.info("commit")
                }
                .onFailure { ex ->
                    conn.rollback()
                    logger.info("rollback")
                    throw DbBadgerException(ex)
                }
    }
}