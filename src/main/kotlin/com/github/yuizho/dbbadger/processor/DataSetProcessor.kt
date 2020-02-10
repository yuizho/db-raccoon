package com.github.yuizho.dbbadger.processor

import com.github.yuizho.dbbadger.annotation.DataSet
import com.github.yuizho.dbbadger.annotation.Row
import com.github.yuizho.dbbadger.annotation.Table
import com.github.yuizho.dbbadger.exception.DbBadgerDataSetException
import com.github.yuizho.dbbadger.operation.QueryOperator
import com.github.yuizho.dbbadger.operation.QuerySource

internal fun DataSet.createInsertQueryOperator(): QueryOperator =
        QueryOperator(testData.flatMap { it.createInsertQuerySources() })

internal fun DataSet.createDeleteQueryOperator(): QueryOperator {
    return QueryOperator(testData.flatMap { it.createDeleteQuerySources() }
            .reversed())
}

internal fun Table.createInsertQuerySources(): List<QuerySource> {
    return rows
            .map { it.createValuesSyntax() }
            .map {
                QuerySource(
                        sql = "INSERT INTO $name ${it.first}",
                        params = it.second
                )
            }
}

private fun Table.createDeleteQuerySources(): List<QuerySource> {
    return rows
            .map { it.createWhereSyntax() }
            .map {
                QuerySource(
                        sql = "DELETE FROM $name WHERE ${it.first}",
                        params = it.second
                )
            }
}

private fun Row.createValuesSyntax(): Pair<String, List<String>> {
    val keys = vals.map { it.name }
    return "(${keys.joinToString(", ")}) VALUES (${keys.map { "?" }.joinToString(", ")})" to
            vals.map { it.value }
}

private fun Row.createWhereSyntax(): Pair<String, List<String>> {
    val ids = vals.filter { it.isId }
    if (ids.isEmpty()) {
        throw DbBadgerDataSetException(
                """Please set at least one Id Col [e.g. @Col(name = "id", value = "1", isId = true)]"""
        )
    }
    val conditions = ids.map { "${it.name} = ?" }
    return "${conditions.joinToString(" AND ")}" to ids.map { it.value }
}