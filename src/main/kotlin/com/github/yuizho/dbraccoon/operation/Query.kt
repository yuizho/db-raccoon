package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.ColType
import com.github.yuizho.dbraccoon.convert
import com.github.yuizho.dbraccoon.exception.DbRaccoonDataSetException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.PreparedStatement

internal data class Query(val sql: String,
                          val params: List<Parameter> = emptyList()) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(Query::class.java)
    }

    fun execute(conn: Connection) {
        conn.prepareStatement(sql).use { pstmt ->
            params.forEachIndexed { i, param ->
                pstmt.setObject(i, param.value, param.type)
            }
            logger.info("[created query] $pstmt")
            pstmt.executeUpdate()
        }
    }

    internal data class Parameter(val value: String?, val type: ColType)
}

private fun PreparedStatement.setObject(i: Int, value: String?, type: ColType) {
    val bindIndex = i + 1
    if (type == ColType.DEFAULT) {
        this.setString(bindIndex, value)
    } else {
        val convertedValue = try {
            if (value != null) type.convert(value) else null
        } catch (ex: Exception) {
            throw DbRaccoonDataSetException(ex)
        }
        this.setObject(bindIndex, convertedValue, type.sqlType)
    }
}