package com.github.yuizho.dbraccoon.processor

import com.github.yuizho.dbraccoon.ColType
import com.github.yuizho.dbraccoon.annotation.Col
import com.github.yuizho.dbraccoon.annotation.DataSet
import com.github.yuizho.dbraccoon.annotation.Row
import com.github.yuizho.dbraccoon.annotation.Table
import com.github.yuizho.dbraccoon.exception.DbRaccoonDataSetException
import com.github.yuizho.dbraccoon.operation.QueryOperator
import com.github.yuizho.dbraccoon.operation.QuerySource

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
                        params = it.second.map { col ->
                            QuerySource.Parameter(
                                    value = col.value,
                                    type = this.getType(col.name)
                            )
                        }
                )
            }
}

private fun Table.createDeleteQuerySources(): List<QuerySource> {
    return rows
            .map { it.createWhereSyntax() }
            .map {
                QuerySource(
                        sql = "DELETE FROM $name WHERE ${it.first}",
                        params = it.second.map { col ->
                            QuerySource.Parameter(
                                    value = col.value,
                                    type = this.getType(col.name)
                            )
                        }
                )
            }
}

private fun Table.getType(name: String): ColType {
    return types.firstOrNull { it.name == name }?.type ?: ColType.DEFAULT
}

private fun Row.createValuesSyntax(): Pair<String, List<Col>> {
    val keys = vals.map { it.name }
    return "(${keys.joinToString(", ")}) VALUES (${keys.map { "?" }.joinToString(", ")})" to
            vals.toList()
}

private fun Row.createWhereSyntax(): Pair<String, List<Col>> {
    val ids = vals.filter { it.isId }
    if (ids.isEmpty()) {
        throw DbRaccoonDataSetException(
                """Please set at least one Id Col [e.g. @Col(name = "id", value = "1", isId = true)]"""
        )
    }
    val conditions = ids.map { "${it.name} = ?" }
    return "${conditions.joinToString(" AND ")}" to ids
}