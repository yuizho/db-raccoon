package com.github.yuizho.dbbadger.operation

import com.github.yuizho.dbbadger.ColType
import com.github.yuizho.dbbadger.convert
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.PreparedStatement

internal data class QuerySource(val sql: String,
                                val params: List<Parameter>) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(QuerySource::class.java)
    }

    fun executeQuery(conn: Connection) {
        conn.prepareStatement(sql).use { pstmt ->
            params.forEachIndexed { i, param ->
                pstmt.setObject(i, param.value, param.type)
            }
            logger.info("[created query] $pstmt")
            pstmt.executeUpdate()
        }
    }

    internal data class Parameter(val value: String, val type: ColType)
}

private fun PreparedStatement.setObject(i: Int, value: String, type: ColType) {
    val bindIndex = i + 1
    if (type == ColType.DEFAULT) {
        this.setString(bindIndex, value)
    } else {
        val convertedValue = type.convert(value)
        this.setObject(bindIndex, convertedValue, type.sqlType)
    }
}