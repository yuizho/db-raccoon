package com.github.yuizho.dbraccoon.processor

import com.github.yuizho.dbraccoon.ColType
import com.github.yuizho.dbraccoon.annotation.CsvDataSet
import com.github.yuizho.dbraccoon.annotation.CsvTable
import com.github.yuizho.dbraccoon.annotation.TypeHint
import com.github.yuizho.dbraccoon.exception.DbRaccoonDataSetException
import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import com.github.yuizho.dbraccoon.operation.*
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import java.io.StringReader

internal fun CsvDataSet.createColumnMetadataOperator(): ColumnMetadataScanOperator =
        ColumnMetadataScanOperator(
                testData.map { it.createColumnMetadataScanner() }
        )

internal fun CsvDataSet.createInsertQueryOperator(columnByTable: Map<String, TypeByColumn>): QueryOperator =
        QueryOperator(testData.flatMap {
            it.createInsertQueries(
                    columnByTable.get(it.name)
                            ?: throw DbRaccoonException("the table name [${it.name}] is not stored in columnByTable.")
            )
        })

internal fun CsvDataSet.createDeleteQueryOperator(columnByTable: Map<String, TypeByColumn>): QueryOperator {
    return QueryOperator(testData.flatMap {
        it.createDeleteQueries(
                columnByTable.get(it.name)
                        ?: throw DbRaccoonException("the table name [${it.name}] is not stored in columnByTable.")
        )
    }.reversed())
}

private fun CsvTable.createColumnMetadataScanner(): ColumnMetadataScanner = ColumnMetadataScanner(name)

private fun CsvTable.createInsertQueries(typeByCol: TypeByColumn): List<Query> {
    return parseCsv()
            .map { row ->
                Query(
                        sql = "INSERT INTO $name ${row.createValuesSyntax()}",
                        params = row.createQueryParameter(types.toList(), typeByCol)
                )
            }
}

private fun CsvTable.createDeleteQueries(typeByCol: TypeByColumn): List<Query> {
    return parseCsv()
            .map { row ->
                Query(
                        sql = "DELETE FROM $name WHERE ${createWhereSyntax(id.toList())}",
                        params = row
                                .filter { id.contains(it.key) }
                                .createQueryParameter(types.toList(), typeByCol)
                )
            }
}

// TODO: create parser factory class
private fun CsvTable.parseCsv(): List<Column> {
    val csv = rows.joinToString("\n")
    return StringReader(csv).use { sr ->
        val records: Iterable<CSVRecord> =
                CSVFormat.DEFAULT
                        .withAllowDuplicateHeaderNames(false)
                        .withQuote('\'')
                        .withEscape('\\')
                        .withRecordSeparator("\n")
                        .withIgnoreSurroundingSpaces()
                        .withFirstRecordAsHeader()
                        .parse(sr)
        records.map { it.toMap() }
    }
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
        // TODO: decent comments
        throw DbRaccoonDataSetException("Please set at least one Id")
    }
    val conditions = ids.map { "$it = ?" }
    return "${conditions.joinToString(" AND ")}"
}

internal typealias Column = Map<String, String>