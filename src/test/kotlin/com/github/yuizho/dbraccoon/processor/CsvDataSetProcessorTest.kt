package com.github.yuizho.dbraccoon.processor

import com.github.yuizho.dbraccoon.CleanupStrategy
import com.github.yuizho.dbraccoon.ColType
import com.github.yuizho.dbraccoon.annotation.CsvDataSet
import com.github.yuizho.dbraccoon.annotation.CsvTable
import com.github.yuizho.dbraccoon.annotation.TypeHint
import com.github.yuizho.dbraccoon.exception.DbRaccoonDataSetException
import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import com.github.yuizho.dbraccoon.operation.ColumnMetadataScanner
import com.github.yuizho.dbraccoon.operation.Query
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.groups.Tuple
import org.junit.jupiter.api.Test
import java.lang.reflect.Type

class CsvDataSetProcessorTest {

    @CsvDataSet([
        CsvTable("test", [
            "id, name",
            "1, foo",
            "2, ''",
            "3, [null]"
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
                        ),
                        Tuple(
                                "INSERT INTO test (id, name) VALUES (?, ?)",
                                listOf(
                                        Query.Parameter("2", ColType.DEFAULT),
                                        Query.Parameter("", ColType.DEFAULT)
                                )
                        ),
                        Tuple(
                                "INSERT INTO test (id, name) VALUES (?, ?)",
                                listOf(
                                        Query.Parameter("3", ColType.DEFAULT),
                                        Query.Parameter(null, ColType.DEFAULT)
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
    fun `CsvDataSet#createDeleteQueryOperator creates Delete ALL QueryOperator`() {
        val dataSet = SingleTableSingleId::class.java.getAnnotation(CsvDataSet::class.java)
        val actual = dataSet.createDeleteQueryOperator(
            mapOf("test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT)),
            CleanupStrategy.USED_TABLES
        )

        assertThat(actual.querySources)
            .extracting("sql", "params")
            .containsExactly(
                Tuple(
                    "DELETE FROM test",
                    emptyList<Query>()
                )
            )
    }

    @Test
    fun `CsvDataSet#createDeleteQueryOperator creates QueryOperator`() {
        val dataSet = SingleTableSingleId::class.java.getAnnotation(CsvDataSet::class.java)
        val actual = dataSet.createDeleteQueryOperator(
                mapOf("test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT)),
                CleanupStrategy.USED_ROWS
        )

        assertThat(actual.querySources)
                .extracting("sql", "params")
                .containsExactly(
                        Tuple(
                                "DELETE FROM test WHERE id = ?",
                                listOf(
                                        Query.Parameter("3", ColType.DEFAULT)
                                )
                        ),
                        Tuple(
                                "DELETE FROM test WHERE id = ?",
                                listOf(
                                        Query.Parameter("2", ColType.DEFAULT)
                                )
                        ),
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

        assertThatThrownBy { dataSet.createDeleteQueryOperator(mapOf("wrong_table" to mapOf()), CleanupStrategy.USED_ROWS) }
                .isExactlyInstanceOf(DbRaccoonException::class.java)
    }

    @CsvDataSet([
        CsvTable(name = "test",
                rows = [
                    "id, name",
                    "1, foo"
                ],
                id = ["ID", "NAME"],
                types = [
                    TypeHint("id", ColType.SMALLINT),
                    TypeHint("Name", ColType.CHAR)
                ]
        ),
        CsvTable("TEST2", [
            "ID2, NAME2",
            "2, bar"
        ], ["id2", "name2"])
    ])
    class MultipleTableMultipleId {}

    @Test
    fun `CsvDataSet#createInsertQueryOperator creates QueryOperator which has multiple query`() {
        val csvDataSet = MultipleTableMultipleId::class.java.getAnnotation(CsvDataSet::class.java)
        val actual = csvDataSet.createInsertQueryOperator(
                mapOf(
                        "test" to mapOf("id" to ColType.INTEGER, "name" to ColType.VARCHAR),
                        "test2" to mapOf("id2" to ColType.INTEGER, "name2" to ColType.VARCHAR)
                )
        )

        assertThat(actual.querySources)
                .extracting("sql", "params")
                .containsExactly(
                        Tuple(
                                "INSERT INTO test (id, name) VALUES (?, ?)",
                                listOf(
                                        Query.Parameter("1", ColType.SMALLINT),
                                        Query.Parameter("foo", ColType.CHAR)
                                )
                        ),
                        Tuple(
                                "INSERT INTO TEST2 (ID2, NAME2) VALUES (?, ?)",
                                listOf(
                                        Query.Parameter("2", ColType.INTEGER),
                                        Query.Parameter("bar", ColType.VARCHAR)
                                )
                        )
                )
    }

    @Test
    fun `CsvDataSet#createDeleteQueryOperator creates Delete ALL QueryOperator which has multiple query`() {
        val csvDataSet = MultipleTableMultipleId::class.java.getAnnotation(CsvDataSet::class.java)
        val actual = csvDataSet.createDeleteQueryOperator(
            mapOf(
                "test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT),
                "test2" to mapOf("id2" to ColType.INTEGER, "name2" to ColType.VARCHAR)
            ),
            CleanupStrategy.USED_TABLES
        )

        assertThat(actual.querySources)
            .extracting("sql", "params")
            .containsExactly(
                // the order is reversed to delete child table before parent table
                Tuple(
                    "DELETE FROM TEST2",
                    emptyList<Query>()
                ),
                Tuple(
                    "DELETE FROM test",
                    emptyList<Query>()
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
                ),
                CleanupStrategy.USED_ROWS
        )

        assertThat(actual.querySources)
                .extracting("sql", "params")
                .containsExactly(
                        // the order is reversed to delete child table before parent table
                        Tuple(
                                "DELETE FROM TEST2 WHERE id2 = ? AND name2 = ?",
                                listOf(
                                        Query.Parameter("2", ColType.INTEGER),
                                        Query.Parameter("bar", ColType.VARCHAR)
                                )
                        ),
                        Tuple(
                                "DELETE FROM test WHERE ID = ? AND NAME = ?",
                                listOf(
                                        Query.Parameter("1", ColType.SMALLINT),
                                        Query.Parameter("foo", ColType.CHAR)
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
                    mapOf("test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT)),
                    CleanupStrategy.USED_ROWS
            )
        }.isExactlyInstanceOf(DbRaccoonDataSetException::class.java)
                .hasMessage("""Please set at least one id [e.g. @CsvTable(id={"id"}, ...)]""")
    }

    @CsvDataSet([
        CsvTable("test", [
            "id, name",
            "1, foo"
        ], ["wrong_id"])
    ])
    class IdValueIsWrong {}

    @Test
    fun `CsvDataSet#createDeleteQueryOperator throws exception when the id value is wrong`() {
        val csvDataSet = IdValueIsWrong::class.java.getAnnotation(CsvDataSet::class.java)

        assertThatThrownBy {
            csvDataSet.createDeleteQueryOperator(
                    mapOf("test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT)),
                    CleanupStrategy.USED_ROWS
            )
        }.isExactlyInstanceOf(DbRaccoonDataSetException::class.java)
                .hasMessage("The id value is not collect column name. Please confirm the id value in @CsvTable.")
    }

    @CsvDataSet([
        CsvTable("test", [
            "id, name",
            "NIL, foo"
        ], ["id"])
    ], "NIL")
    class NullId {}

    @Test
    fun `CsvDataSet#createDeleteQueryOperator throws exception when there is null id column`() {
        val csvDataSet = NullId::class.java.getAnnotation(CsvDataSet::class.java)

        assertThatThrownBy {
            csvDataSet.createDeleteQueryOperator(
                    mapOf("test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT)),
                    CleanupStrategy.USED_ROWS
            )
        }.isExactlyInstanceOf(DbRaccoonDataSetException::class.java)
                .hasMessage("The id column can not set null value. Please confirm the id column value in @CsvTable.")

    }
}