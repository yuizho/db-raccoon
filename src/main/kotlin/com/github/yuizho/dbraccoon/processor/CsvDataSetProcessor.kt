package com.github.yuizho.dbraccoon.processor

import com.github.yuizho.dbraccoon.CleanupStrategy
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
                    columnByTable[it.name.toLowerCase()]
                            ?: throw DbRaccoonException("the table name [${it.name}] is not stored in columnByTable."),
                    nullValue
            )
        })

internal fun CsvDataSet.createDeleteQueryOperator(columnByTable: Map<String, TypeByColumn>, cleanupStrategy: CleanupStrategy): QueryOperator {
    return QueryOperator(testData.flatMap {
        when(cleanupStrategy) {
            CleanupStrategy.USED_ROWS -> it.createDeleteQueries(
                columnByTable[it.name.toLowerCase()]
                        ?: throw DbRaccoonException("the table name [${it.name}] is not stored in columnByTable."),
                nullValue
            )
            CleanupStrategy.USED_TABLES -> it.createDeleteAllQueries()
        }
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

private fun CsvTable.createDeleteAllQueries(): List<Query> {
    return listOf(Query(sql = "DELETE FROM $name"))
}

private fun CsvTable.createDeleteQueries(typeByCol: TypeByColumn, nullValue: String): List<Query> {
    return parseCsv(nullValue)
            .map { row ->
                Query(
                        sql = "DELETE FROM $name WHERE ${createWhereSyntax(id.toList())}",
                        params = row
                                .filter { col -> id.map { it.toLowerCase() }.contains(col.key.toLowerCase()) }
                                .also {
                                    if (it.isEmpty())
                                        throw DbRaccoonDataSetException("The id value is not collect column name. Please confirm the id value in @CsvTable.")
                                    if (it.containsValue(null))
                                        throw DbRaccoonDataSetException("The id column can not set null value. Please confirm the id column value in @CsvTable.")
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
                        type = typeHints.firstOrNull { hint -> hint.name.toLowerCase() == entry.key.toLowerCase() }?.type
                                ?: typeByCol.getOrDefault(entry.key.toLowerCase(), ColType.DEFAULT)
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