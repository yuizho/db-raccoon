package com.github.yuizho.dbraccoon.processor

import com.github.yuizho.dbraccoon.ColType
import com.github.yuizho.dbraccoon.annotation.CsvDataSet
import com.github.yuizho.dbraccoon.annotation.CsvTable
import com.github.yuizho.dbraccoon.annotation.TypeHint
import com.github.yuizho.dbraccoon.exception.DbRaccoonDataSetException
import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import com.github.yuizho.dbraccoon.operation.*

internal fun CsvDataSet.createColumnMetadataOperator(): ColumnMetadataScanOperator =
        ColumnMetadataScanOperator(
                testData.map { it.createColumnMetadataScanner() }
        )

internal fun CsvDataSet.createInsertQueryOperator(columnByTable: Map<String, TypeByColumn>): QueryOperator =
        QueryOperator(testData.flatMap {
            it.createInsertQueries(
                    columnByTable.get(it.name)
                            ?: throw DbRaccoonException("the table name [${it.name}] is not stored in columnByTable."),
                    nullValue
            )
        })

internal fun CsvDataSet.createDeleteQueryOperator(columnByTable: Map<String, TypeByColumn>): QueryOperator {
    return QueryOperator(testData.flatMap {
        it.createDeleteQueries(
                columnByTable.get(it.name)
                        ?: throw DbRaccoonException("the table name [${it.name}] is not stored in columnByTable."),
                nullValue
        )
    }.reversed())
}

private fun CsvTable.createColumnMetadataScanner(): ColumnMetadataScanner = ColumnMetadataScanner(name)

private fun CsvTable.createInsertQueries(typeByCol: TypeByColumn, nullValue: String): List<Query> {
    return parseCsv(nullValue)
            .map { row ->
                Query(
                        sql = "INSERT INTO $name ${row.createValuesSyntax()}",
                        params = row.createQueryParameter(types.toList(), typeByCol)
                )
            }
}

private fun CsvTable.createDeleteQueries(typeByCol: TypeByColumn, nullValue: String): List<Query> {
    return parseCsv(nullValue)
            .map { row ->
                Query(
                        sql = "DELETE FROM $name WHERE ${createWhereSyntax(id.toList())}",
                        params = row
                                .filter { id.contains(it.key) }
                                .also {
                                    if (it.containsValue(null))
                                        throw DbRaccoonDataSetException("""id column can not set null value""")
                                    it
                                }
                                .createQueryParameter(types.toList(), typeByCol)
                )
            }
}

private fun CsvTable.parseCsv(nullValue: String): List<Column> {
    val csv = rows.joinToString("\n")
    return CsvParser(nullValue).parse(csv)
}

private fun Column.createQueryParameter(typeHints: List<TypeHint>, typeByCol: TypeByColumn): List<Query.Parameter> {
    return entries
            .map { entry ->
                Query.Parameter(
                        value = entry.value,
                        type = typeHints.firstOrNull { it.name == entry.key }?.type
                                ?: typeByCol.getOrDefault(entry.key, ColType.DEFAULT)
                )
            }
}

private fun Column.createValuesSyntax(): String {
    return "(${keys.joinToString(", ")}) VALUES (${keys.map { "?" }.joinToString(", ")})"
}

private fun createWhereSyntax(ids: List<String>): String {
    if (ids.isEmpty()) {
        throw DbRaccoonDataSetException("""Please set at least one id [e.g. @CsvTable(id={"id"}, ...)]""")
    }
    val conditions = ids.map { "$it = ?" }
    return "${conditions.joinToString(" AND ")}"
}

internal typealias Column = Map<String, String?>