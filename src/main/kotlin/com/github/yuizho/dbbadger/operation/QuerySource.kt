package com.github.yuizho.dbbadger.operation

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

internal data class QuerySource(val sql: String, val params: List<String>) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(QuerySource::class.java)
    }

    fun executeQuery(conn: Connection) {
        conn.prepareStatement(sql).use { pstmt ->
            params.forEachIndexed { i, elm ->
                pstmt.setString(i + 1, elm)
            }
            logger.info("[created query] $pstmt")
            pstmt.executeUpdate()
        }
    }
}