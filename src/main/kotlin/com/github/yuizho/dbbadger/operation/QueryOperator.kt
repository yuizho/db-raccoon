package com.github.yuizho.dbbadger.operation

import java.sql.Connection

internal data class QueryOperator(val querySources: List<QuerySource>) {
    fun executeQueries(conn: Connection) {
        querySources.forEach { it.executeQuery(conn) }
    }
}