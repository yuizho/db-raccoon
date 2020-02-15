package com.github.yuizho.dbraccoon.processor

import com.github.yuizho.dbraccoon.ColType
import com.github.yuizho.dbraccoon.annotation.Col
import com.github.yuizho.dbraccoon.annotation.DataSet
import com.github.yuizho.dbraccoon.annotation.Row
import com.github.yuizho.dbraccoon.annotation.Table
import com.github.yuizho.dbraccoon.exception.DbRaccoonDataSetException
import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import com.github.yuizho.dbraccoon.operation.*


internal fun DataSet.createColumnMetadataOperator(): ColumnMetadataScanOperator =
        ColumnMetadataScanOperator(
                testData.map { it.createColumnMetadataScanner() }
        )

internal fun DataSet.createInsertQueryOperator(columnByTable: Map<String, TypeByColumn>): QueryOperator =
        QueryOperator(testData.flatMap {
            it.createInsertQueries(
                    columnByTable.get(it.name)
                            ?: throw DbRaccoonException("the table name [${it.name}] is not stored in columnByTable.")
            )
        })

internal fun DataSet.createDeleteQueryOperator(columnByTable: Map<String, TypeByColumn>): QueryOperator {
    return QueryOperator(testData.flatMap {
        it.createDeleteQueries(
                columnByTable.get(it.name)
                        ?: throw DbRaccoonException("the table name [${it.name}] is not stored in columnByTable.")
        )
    }.reversed())
}

private fun Table.createColumnMetadataScanner(): ColumnMetadataScanner = ColumnMetadataScanner(name)

private fun Table.createInsertQueries(typeByCol: TypeByColumn): List<Query> {
    return rows
            .map { it.createValuesSyntax() }
            .map {
                Query(
                        sql = "INSERT INTO $name ${it.first}",
                        params = it.second.map { col ->
                            Query.Parameter(
                                    value = col.value,
                                    type = this.getType(col.name)
                                            ?: typeByCol.getOrDefault(col.name, ColType.DEFAULT)
                            )
                        }
                )
            }
}

private fun Table.createDeleteQueries(typeByCol: TypeByColumn): List<Query> {
    return rows
            .map { it.createWhereSyntax() }
            .map {
                Query(
                        sql = "DELETE FROM $name WHERE ${it.first}",
                        params = it.second.map { col ->
                            Query.Parameter(
                                    value = col.value,
                                    type = this.getType(col.name)
                                            ?: typeByCol.getOrDefault(col.name, ColType.DEFAULT)
                            )
                        }
                )
            }
}

private fun Table.getType(name: String): ColType? {
    return types.firstOrNull { it.name == name }?.type
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