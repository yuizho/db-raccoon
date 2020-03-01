package com.github.yuizho.dbraccoon.processor

import org.apache.commons.csv.CSVFormat
import java.io.StringReader

internal class CsvParser(private val nullValue: String) {
    private val csvFormat: CSVFormat = CSVFormat.DEFAULT
            .withAllowDuplicateHeaderNames(false)
            .withQuote('\'')
            .withEscape('\\')
            .withRecordSeparator("\n")
            .withIgnoreSurroundingSpaces()
            .withFirstRecordAsHeader()
            .withNullString(nullValue)

    fun parse(csv: String): List<Map<String, String?>> {
        return StringReader(csv).use { sr ->
            val records = csvFormat.parse(sr)
            records.map { it.toMap() }
        }
    }
}