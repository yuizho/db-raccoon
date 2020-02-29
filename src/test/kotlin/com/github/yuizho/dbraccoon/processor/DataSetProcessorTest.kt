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

class DataSetProcessorTest {

    @DataSet([
        Table("test", [
            Row([
                Col("id", "1", true),
                Col("name", "foo")
            ])
        ])
    ])
    class SingleTableSingleId {}

    @Test
    fun `DataSet#createColumnMetadataOperator creates ColumnMetadataScanOperator`() {
        val dataSet = SingleTableSingleId::class.java.getAnnotation(DataSet::class.java)
        val actual = dataSet.createColumnMetadataOperator()
        val expected = listOf(ColumnMetadataScanner("test"))
        assertThat(actual.scanners).extracting("tableName")
                .containsExactly("test")
    }

    @Test
    fun `DataSet#createInsertQueryOperator creates QueryOperator`() {
        val dataSet = SingleTableSingleId::class.java.getAnnotation(DataSet::class.java)
        val actual = dataSet.createInsertQueryOperator(
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
    fun `DataSet#createInsertQueryOperator throws exception when wrong table name is passed`() {
        val dataSet = SingleTableSingleId::class.java.getAnnotation(DataSet::class.java)

        assertThatThrownBy { dataSet.createInsertQueryOperator(mapOf("wrong_table" to mapOf())) }
                .isExactlyInstanceOf(DbRaccoonException::class.java)
    }

    @Test
    fun `DataSet#createDeleteQueryOperator creates QueryOperator`() {
        val dataSet = SingleTableSingleId::class.java.getAnnotation(DataSet::class.java)
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
    fun `DataSet#createDeleteQueryOperator throws exception when wrong table name is passed`() {
        val dataSet = SingleTableSingleId::class.java.getAnnotation(DataSet::class.java)

        assertThatThrownBy { dataSet.createDeleteQueryOperator(mapOf("wrong_table" to mapOf())) }
                .isExactlyInstanceOf(DbRaccoonException::class.java)
    }

    @DataSet([
        Table(name = "test", rows = [
            Row([
                Col("id", "1", true),
                Col("name", "foo", true)
            ])
        ], types = [
            TypeHint("id", ColType.SMALLINT),
            TypeHint("Name", ColType.CHAR)
        ]),
        Table("TEST2", [
            Row([
                Col("ID2", "2", true),
                Col("NAME2", "bar", true)
            ])
        ])
    ])
    class MultipleTableMultipleId {}

    @Test
    fun `DataSet#createInsertQueryOperator creates QueryOperator which has multiple query`() {
        val dataSet = MultipleTableMultipleId::class.java.getAnnotation(DataSet::class.java)
        val actual = dataSet.createInsertQueryOperator(
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
    fun `DataSet#createDeleteQueryOperator creates QueryOperator which has multiple query`() {
        val dataSet = MultipleTableMultipleId::class.java.getAnnotation(DataSet::class.java)
        val actual = dataSet.createDeleteQueryOperator(
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
                                "DELETE FROM TEST2 WHERE ID2 = ? AND NAME2 = ?",
                                listOf(
                                        Query.Parameter("2", ColType.INTEGER),
                                        Query.Parameter("bar", ColType.VARCHAR)
                                )
                        ),
                        Tuple(
                                "DELETE FROM test WHERE id = ? AND name = ?",
                                listOf(
                                        Query.Parameter("1", ColType.SMALLINT),
                                        Query.Parameter("foo", ColType.CHAR)
                                )
                        )

                )
    }

    @DataSet([
        Table("test", [
            Row([
                Col("id", "1"),
                Col("name", "foo")
            ])
        ])
    ])
    class NoIdColumn {}

    @Test
    fun `DataSet#createDeleteQueryOperator throws exception when the row doesn't have Id column`() {
        val dataSet = NoIdColumn::class.java.getAnnotation(DataSet::class.java)

        assertThatThrownBy {
            dataSet.createDeleteQueryOperator(
                    mapOf("test" to mapOf("id" to ColType.DEFAULT, "name" to ColType.DEFAULT))
            )
        }.isExactlyInstanceOf(DbRaccoonDataSetException::class.java)
    }
}