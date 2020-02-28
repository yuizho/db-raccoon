package com.github.yuizho.dbraccoon.processor

import com.github.yuizho.dbraccoon.ColType
import com.github.yuizho.dbraccoon.annotation.*
import com.github.yuizho.dbraccoon.exception.DbRaccoonDataSetException
import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import com.github.yuizho.dbraccoon.operation.ColumnMetadataScanner
import com.github.yuizho.dbraccoon.operation.Query
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.groups.Tuple
import org.junit.jupiter.api.Test

class CsvDataSetProcessorTest {

    @CsvDataSet([
        CsvTable("test", [
            "id, name",
            "1, foo"
        ], ["id"])
    ])
    class SingleTableSingleId {}

    @Test
    fun `CsvDataSet#createColumnMetadataOperator creates ColumnMetadataScanOperator`() {
        val csvDataSet = SingleTableSingleId::class.java.getAnnotation(CsvDataSet::class.java)
        val actual = csvDataSet.createColumnMetadataOperator()
        val expected = listOf(ColumnMetadataScanner("test"))
        assertThat(actual.scanners).extracting("tableName")
                .containsExactly("test")
    }

    @Test
    fun `CsvDataSet#createInsertQueryOperator creates QueryOperator`() {
        val csvDataSet = SingleTableSingleId::class.java.getAnnotation(CsvDataSet::class.java)
        val actual = csvDataSet.createInsertQueryOperator(
                mapOf("test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT))
        )

        assertThat(actual.querySources)
                .extracting("sql", "params")
                .containsExactly(
                        Tuple(
                                "INSERT INTO test (id, name) VALUES (?, ?)",
                                listOf(
                                        Query.Parameter("1", ColType.DEFAULT),
                                        Query.Parameter("foo", ColType.DEFAULT)
                                )
                        )
                )
    }

    @Test
    fun `CsvDataSet#createInsertQueryOperator throws exception when wrong table name is passed`() {
        val dataSet = SingleTableSingleId::class.java.getAnnotation(CsvDataSet::class.java)

        assertThatThrownBy { dataSet.createInsertQueryOperator(mapOf("wrong_table" to mapOf())) }
                .isExactlyInstanceOf(DbRaccoonException::class.java)
    }

    @Test
    fun `CsvDataSet#createDeleteQueryOperator creates QueryOperator`() {
        val dataSet = SingleTableSingleId::class.java.getAnnotation(CsvDataSet::class.java)
        val actual = dataSet.createDeleteQueryOperator(
                mapOf("test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT))
        )

        assertThat(actual.querySources)
                .extracting("sql", "params")
                .containsExactly(
                        Tuple(
                                "DELETE FROM test WHERE id = ?",
                                listOf(
                                        Query.Parameter("1", ColType.DEFAULT)
                                )
                        )
                )
    }

    @Test
    fun `CsvDataSet#createDeleteQueryOperator throws exception when wrong table name is passed`() {
        val dataSet = SingleTableSingleId::class.java.getAnnotation(CsvDataSet::class.java)

        assertThatThrownBy { dataSet.createDeleteQueryOperator(mapOf("wrong_table" to mapOf())) }
                .isExactlyInstanceOf(DbRaccoonException::class.java)
    }

    @CsvDataSet([
        CsvTable("test", [
            "id, name",
            "1, foo"
        ], ["id", "name"]),
        CsvTable("test2", [
            "id2, name2",
            "2, bar"
        ], ["id2", "name2"])
    ])
    class MultipleTableMultipleId {}

    @Test
    fun `CsvDataSet#createInsertQueryOperator creates QueryOperator which has multiple query`() {
        val csvDataSet = MultipleTableMultipleId::class.java.getAnnotation(CsvDataSet::class.java)
        val actual = csvDataSet.createInsertQueryOperator(
                mapOf(
                        "test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT),
                        "test2" to mapOf("id2" to ColType.INTEGER, "name2" to ColType.VARCHAR)
                )
        )

        assertThat(actual.querySources)
                .extracting("sql", "params")
                .containsExactly(
                        Tuple(
                                "INSERT INTO test (id, name) VALUES (?, ?)",
                                listOf(
                                        Query.Parameter("1", ColType.DEFAULT),
                                        Query.Parameter("foo", ColType.DEFAULT)
                                )
                        ),
                        Tuple(
                                "INSERT INTO test2 (id2, name2) VALUES (?, ?)",
                                listOf(
                                        Query.Parameter("2", ColType.INTEGER),
                                        Query.Parameter("bar", ColType.VARCHAR)
                                )
                        )
                )
    }

    @Test
    fun `CsvDataSet#createDeleteQueryOperator creates QueryOperator which has multiple query`() {
        val csvDataSet = MultipleTableMultipleId::class.java.getAnnotation(CsvDataSet::class.java)
        val actual = csvDataSet.createDeleteQueryOperator(
                mapOf(
                        "test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT),
                        "test2" to mapOf("id2" to ColType.INTEGER, "name2" to ColType.VARCHAR)
                )
        )

        assertThat(actual.querySources)
                .extracting("sql", "params")
                .containsExactly(
                        // the order is reversed to delete child table before parent table
                        Tuple(
                                "DELETE FROM test2 WHERE id2 = ? AND name2 = ?",
                                listOf(
                                        Query.Parameter("2", ColType.INTEGER),
                                        Query.Parameter("bar", ColType.VARCHAR)
                                )
                        ),
                        Tuple(
                                "DELETE FROM test WHERE id = ? AND name = ?",
                                listOf(
                                        Query.Parameter("1", ColType.DEFAULT),
                                        Query.Parameter("foo", ColType.DEFAULT)
                                )
                        )

                )
    }

    @CsvDataSet([
        CsvTable("test", [
            "id, name",
            "1, foo"
        ], [])
    ])
    class NoIdColumn {}

    @Test
    fun `CsvDataSet#createDeleteQueryOperator throws exception when the row doesn't have Id column`() {
        val csvDataSet = NoIdColumn::class.java.getAnnotation(CsvDataSet::class.java)

        assertThatThrownBy {
            csvDataSet.createDeleteQueryOperator(
                    mapOf("test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT))
            )
        }.isExactlyInstanceOf(DbRaccoonDataSetException::class.java)
    }
}