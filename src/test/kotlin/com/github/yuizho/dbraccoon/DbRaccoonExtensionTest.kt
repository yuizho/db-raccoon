package com.github.yuizho.dbraccoon

import com.github.yuizho.dbraccoon.annotation.CsvDataSet
import com.github.yuizho.dbraccoon.annotation.CsvTable
import com.github.yuizho.dbraccoon.annotation.DataSet
import com.github.yuizho.dbraccoon.annotation.Table
import com.github.yuizho.dbraccoon.operation.ColumnMetadataScanOperator
import com.github.yuizho.dbraccoon.operation.QueryOperator
import com.github.yuizho.dbraccoon.processor.createColumnMetadataOperator
import com.github.yuizho.dbraccoon.processor.createDeleteQueryOperator
import com.github.yuizho.dbraccoon.processor.createInsertQueryOperator
import io.mockk.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mockito.`when`
import org.mockito.Mockito.mock
import java.sql.Connection
import javax.sql.DataSource

class DbRaccoonExtensionTest {
    companion object {
        @JvmStatic
        @BeforeAll
        fun setUp() {
            mockkStatic("com.github.yuizho.dbraccoon.processor.DataSetProcessorKt")
            mockkStatic("com.github.yuizho.dbraccoon.processor.CsvDataSetProcessorKt")
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            unmockkStatic("com.github.yuizho.dbraccoon.processor.DataSetProcessorKt")
            unmockkStatic("com.github.yuizho.dbraccoon.processor.CsvDataSetProcessorKt")
        }
    }

    private fun dataSourceMocks(): Pair<DataSource, Connection> {
        val dataSourceMock = mockk<DataSource>()
        val connMock = mockk<Connection>(relaxUnitFun = true)
        every { dataSourceMock.connection } returns connMock
        return Pair(dataSourceMock, connMock)
    }

    private fun dataSetMocks(): Triple<DataSet, ColumnMetadataScanOperator, QueryOperator> {
        val dataSetMock = mockk<DataSet>()
        val metadataOperatorMock = mockk<ColumnMetadataScanOperator>()
        val queryOperatorMock = mockk<QueryOperator>(relaxUnitFun = true)
        every { dataSetMock.createColumnMetadataOperator() } returns metadataOperatorMock
        every { dataSetMock.createDeleteQueryOperator(any()) } returns queryOperatorMock
        every { dataSetMock.createInsertQueryOperator(any()) } returns queryOperatorMock
        return Triple(dataSetMock, metadataOperatorMock, queryOperatorMock)
    }

    private fun csvDataSetMocks(): Triple<CsvDataSet, ColumnMetadataScanOperator, QueryOperator> {
        val csvDataSetMock = mockk<CsvDataSet>()
        val metadataOperatorMock = mockk<ColumnMetadataScanOperator>()
        val queryOperatorMock = mockk<QueryOperator>(relaxUnitFun = true)
        every { csvDataSetMock.createColumnMetadataOperator() } returns metadataOperatorMock
        every { csvDataSetMock.createDeleteQueryOperator(any()) } returns queryOperatorMock
        every { csvDataSetMock.createInsertQueryOperator(any()) } returns queryOperatorMock
        return Triple(csvDataSetMock, metadataOperatorMock, queryOperatorMock)
    }

    @Nested
    @DisplayName("The test cases that @DataSet is only applied")
    inner class OnlyDataSetAppliedCases {

        @ParameterizedTest(name = "when {0} is passed")
        @CsvSource(value = ["BEFORE_AND_AFTER_TEST", "BEFORE_TEST"])
        fun `beforeTestExecution delete-insert works`(cleanupPhase: CleanupPhase) {
            // mocks
            val (dataSourceMock, connMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()

            val expectedColumnByTable = mapOf("table" to mapOf("1" to ColType.INTEGER))
            every { metadataOperatorMock.execute(connMock) } returns expectedColumnByTable

            // given
            val dbRaccoonExtension = spyk(
                    DbRaccoonExtension(
                            dataSourceMock,
                            cleanupPhase
                    )
            )
            every { dbRaccoonExtension.getDataSet(contextMock) } returns dataSetMock
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns null
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock

            // when
            dbRaccoonExtension.beforeTestExecution(contextMock)

            // then
            verify(exactly = 1) { dataSetMock.createDeleteQueryOperator(expectedColumnByTable) }
            verify(exactly = 1) { dataSetMock.createInsertQueryOperator(expectedColumnByTable) }
            verify(exactly = 2) { queryOperatorMock.executeQueries(connMock) }
            verify(exactly = 1) { storeMock.put("columnMetadataByTable", expectedColumnByTable) }
        }

        @ParameterizedTest(name = "when {0} is passed")
        @CsvSource(value = ["AFTER_TEST"])
        fun `beforeTestExecution insert(no delete) works`(cleanupPhase: CleanupPhase) {
            // mocks
            val (dataSourceMock, connMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()

            val expectedColumnByTable = mapOf("table" to mapOf("1" to ColType.INTEGER))
            every { metadataOperatorMock.execute(connMock) } returns expectedColumnByTable

            // given
            val dbRaccoonExtension = spyk(
                    DbRaccoonExtension(
                            dataSourceMock,
                            cleanupPhase
                    )
            )
            every { dbRaccoonExtension.getDataSet(contextMock) } returns dataSetMock
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns null
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock

            // when
            dbRaccoonExtension.beforeTestExecution(contextMock)

            // then
            verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 1) { dataSetMock.createInsertQueryOperator(expectedColumnByTable) }
            verify(exactly = 1) { queryOperatorMock.executeQueries(connMock) }
            verify(exactly = 1) { storeMock.put("columnMetadataByTable", expectedColumnByTable) }
        }

        @ParameterizedTest(name = "when {0} is passed")
        @CsvSource(value = ["BEFORE_AND_AFTER_TEST", "AFTER_TEST"])
        fun `afterTestExecution delete works`(cleanupPhase: CleanupPhase) {
            // mocks
            val (dataSourceMock, connMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()

            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            val expectedColumnByTable = mapOf("table" to mapOf("1" to ColType.INTEGER))
            every { storeMock.remove(any()) } returns expectedColumnByTable

            // given
            val dbRaccoonExtension = spyk(
                    DbRaccoonExtension(
                            dataSourceMock,
                            cleanupPhase
                    )
            )
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock
            every { dbRaccoonExtension.getDataSet(contextMock) } returns dataSetMock
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns null

            // when
            dbRaccoonExtension.afterTestExecution(contextMock)

            // then
            verify(exactly = 1) { dataSetMock.createDeleteQueryOperator(expectedColumnByTable) }
            verify(exactly = 1) { queryOperatorMock.executeQueries(connMock) }
            verify(exactly = 1) { storeMock.remove("columnMetadataByTable") }
        }

        @DataSet([
            Table(name = "onClass", rows = [])
        ])
        private inner class AppliedOnClass {
            fun methodNoDataSet() {}

            @DataSet([
                Table(name = "onMethod", rows = [])
            ])
            fun methodHasDataSet() {
            }
        }

        @Test
        fun `getDataSet returns @DataSet on a Class when @DataSet annotation is just applied on Class`() {
            // mock (use mockito. because when mockk makes a method class mock, StackOverflow error is occurred.)
            val contextMock = mock(ExtensionContext::class.java)
            `when`(contextMock.requiredTestMethod).thenReturn(AppliedOnClass::class.java.getMethod("methodNoDataSet"))

            // when
            val actual = DbRaccoonExtension(mock(DataSource::class.java)).getDataSet(contextMock)

            // then
            assertThat(actual).isEqualTo(AppliedOnClass::class.java.getAnnotation(DataSet::class.java))
            assertThat(actual!!.testData)
                    .extracting("name")
                    .containsExactly("onClass")
        }

        @Test
        fun `getDataSet returns @DataSet on a Method when @DataSet annotation is applied on Method`() {
            // mock (use mockito. because when mockk makes a method class mock, StackOverflow error is occurred.)
            val contextMock = mock(ExtensionContext::class.java)
            `when`(contextMock.requiredTestMethod).thenReturn(AppliedOnClass::class.java.getMethod("methodHasDataSet"))

            // when
            val actual = DbRaccoonExtension(mock(DataSource::class.java)).getDataSet(contextMock)

            // then
            assertThat(actual)
                    .isEqualTo(AppliedOnClass::class.java.getMethod("methodHasDataSet").getAnnotation(DataSet::class.java))
            assertThat(actual!!.testData)
                    .extracting("name")
                    .containsExactly("onMethod")
        }

        private inner class ClassNoDataSet {
            fun methodNoDataSet() {}
        }

        @Test
        fun `getDataSet returns null when @DataSet annotation is not applied on Class and Method`() {
            // mock (use mockito. because when mockk makes a method class mock, StackOverflow error is occurred.)
            val contextMock = mock(ExtensionContext::class.java)
            `when`(contextMock.requiredTestMethod).thenReturn(ClassNoDataSet::class.java.getMethod("methodNoDataSet"))

            // when
            val actual = DbRaccoonExtension(mock(DataSource::class.java)).getDataSet(contextMock)

            // then
            assertThat(actual).isNull()
        }
    }

    @Nested
    @DisplayName("The test cases that @CsvDataSet is only applied")
    inner class OnlyCsvDataSetAppliedCases {

        @ParameterizedTest(name = "when {0} is passed")
        @CsvSource(value = ["BEFORE_AND_AFTER_TEST", "BEFORE_TEST"])
        fun `beforeTestExecution delete-insert works`(cleanupPhase: CleanupPhase) {
            // mocks
            val (dataSourceMock, connMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            val (csvDataSetMock, metadataOperatorMock, queryOperatorMock) = csvDataSetMocks()

            val expectedColumnByTable = mapOf("table" to mapOf("1" to ColType.INTEGER))
            every { metadataOperatorMock.execute(connMock) } returns expectedColumnByTable

            // given
            val dbRaccoonExtension = spyk(
                    DbRaccoonExtension(
                            dataSourceMock,
                            cleanupPhase
                    )
            )
            every { dbRaccoonExtension.getDataSet(contextMock) } returns null
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns csvDataSetMock
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock

            // when
            dbRaccoonExtension.beforeTestExecution(contextMock)

            // then
            verify(exactly = 1) { csvDataSetMock.createDeleteQueryOperator(expectedColumnByTable) }
            verify(exactly = 1) { csvDataSetMock.createInsertQueryOperator(expectedColumnByTable) }
            verify(exactly = 2) { queryOperatorMock.executeQueries(connMock) }
            verify(exactly = 1) { storeMock.put("columnMetadataByTable", expectedColumnByTable) }
        }

        @ParameterizedTest(name = "when {0} is passed")
        @CsvSource(value = ["AFTER_TEST"])
        fun `beforeTestExecution insert(no delete) works`(cleanupPhase: CleanupPhase) {
            // mocks
            val (dataSourceMock, connMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            val (csvDataSetMock, metadataOperatorMock, queryOperatorMock) = csvDataSetMocks()

            val expectedColumnByTable = mapOf("table" to mapOf("1" to ColType.INTEGER))
            every { metadataOperatorMock.execute(connMock) } returns expectedColumnByTable

            // given
            val dbRaccoonExtension = spyk(
                    DbRaccoonExtension(
                            dataSourceMock,
                            cleanupPhase
                    )
            )
            every { dbRaccoonExtension.getDataSet(contextMock) } returns null
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns csvDataSetMock
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock

            // when
            dbRaccoonExtension.beforeTestExecution(contextMock)

            // then
            verify(exactly = 0) { csvDataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 1) { csvDataSetMock.createInsertQueryOperator(expectedColumnByTable) }
            verify(exactly = 1) { queryOperatorMock.executeQueries(connMock) }
            verify(exactly = 1) { storeMock.put("columnMetadataByTable", expectedColumnByTable) }
        }

        @ParameterizedTest(name = "when {0} is passed")
        @CsvSource(value = ["BEFORE_AND_AFTER_TEST", "AFTER_TEST"])
        fun `afterTestExecution delete works`(cleanupPhase: CleanupPhase) {
            // mocks
            val (dataSourceMock, connMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val (csvDataSetMock, metadataOperatorMock, queryOperatorMock) = csvDataSetMocks()

            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            val expectedColumnByTable = mapOf("table" to mapOf("1" to ColType.INTEGER))
            every { storeMock.remove(any()) } returns expectedColumnByTable

            // given
            val dbRaccoonExtension = spyk(
                    DbRaccoonExtension(
                            dataSourceMock,
                            cleanupPhase
                    )
            )
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock
            every { dbRaccoonExtension.getDataSet(contextMock) } returns null
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns csvDataSetMock

            // when
            dbRaccoonExtension.afterTestExecution(contextMock)

            // then
            verify(exactly = 1) { csvDataSetMock.createDeleteQueryOperator(expectedColumnByTable) }
            verify(exactly = 1) { queryOperatorMock.executeQueries(connMock) }
            verify(exactly = 1) { storeMock.remove("columnMetadataByTable") }
        }

        @CsvDataSet([
            CsvTable(name = "onClass", rows = [], id = [])
        ])
        private inner class AppliedOnClass {
            fun methodNoDataSet() {}

            @CsvDataSet([
                CsvTable(name = "onMethod", rows = [], id = [])
            ])
            fun methodHasDataSet() {
            }
        }

        @Test
        fun `getDataSet returns @CsvDataSet on a Class when @DataSet annotation is just applied on Class`() {
            // mock (use mockito. because when mockk makes a method class mock, StackOverflow error is occurred.)
            val contextMock = mock(ExtensionContext::class.java)
            `when`(contextMock.requiredTestMethod).thenReturn(AppliedOnClass::class.java.getMethod("methodNoDataSet"))

            // when
            val actual = DbRaccoonExtension(mock(DataSource::class.java)).getCsvDataSet(contextMock)

            // then
            assertThat(actual).isEqualTo(AppliedOnClass::class.java.getAnnotation(CsvDataSet::class.java))
            assertThat(actual!!.testData)
                    .extracting("name")
                    .containsExactly("onClass")
        }

        @Test
        fun `getDataSet returns @CsvDataSet on a Method when @DataSet annotation is applied on Method`() {
            // mock (use mockito. because when mockk makes a method class mock, StackOverflow error is occurred.)
            val contextMock = mock(ExtensionContext::class.java)
            `when`(contextMock.requiredTestMethod).thenReturn(AppliedOnClass::class.java.getMethod("methodHasDataSet"))

            // when
            val actual = DbRaccoonExtension(mock(DataSource::class.java)).getCsvDataSet(contextMock)

            // then
            assertThat(actual)
                    .isEqualTo(AppliedOnClass::class.java.getMethod("methodHasDataSet").getAnnotation(CsvDataSet::class.java))
            assertThat(actual!!.testData)
                    .extracting("name")
                    .containsExactly("onMethod")
        }

        private inner class ClassNoDataSet {
            fun methodNoDataSet() {}
        }

        @Test
        fun `getDataSet returns null when @CsvDataSet annotation is not applied on Class and Method`() {
            // mock (use mockito. because when mockk makes a method class mock, StackOverflow error is occurred.)
            val contextMock = mock(ExtensionContext::class.java)
            `when`(contextMock.requiredTestMethod).thenReturn(ClassNoDataSet::class.java.getMethod("methodNoDataSet"))

            // when
            val actual = DbRaccoonExtension(mock(DataSource::class.java)).getCsvDataSet(contextMock)

            // then
            assertThat(actual).isNull()
        }
    }

    @Nested
    @DisplayName("The test cases that All data set annotations are applied")
    inner class AllDataSetAppliedCases {

        @ParameterizedTest(name = "when {0} is passed")
        @CsvSource(value = ["BEFORE_AND_AFTER_TEST", "BEFORE_TEST"])
        fun `beforeTestExecution delete-insert works`(cleanupPhase: CleanupPhase) {
            // mocks
            val (dataSourceMock, connMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()
            val (csvDataSetMock, csvMetadataOperatorMock, csvQueryOperatorMock) = csvDataSetMocks()

            val expectedColumnByTableForDataSet = mapOf("table" to mapOf("1" to ColType.INTEGER))
            every { metadataOperatorMock.execute(connMock) } returns expectedColumnByTableForDataSet
            val expectedColumnByTableForCsvDataSet = mapOf("table" to mapOf("1" to ColType.INTEGER), "table2" to mapOf("1" to ColType.INTEGER))
            every { csvMetadataOperatorMock.execute(connMock) } returns expectedColumnByTableForCsvDataSet


            // given
            val dbRaccoonExtension = spyk(
                    DbRaccoonExtension(
                            dataSourceMock,
                            cleanupPhase
                    )
            )
            every { dbRaccoonExtension.getDataSet(contextMock) } returns dataSetMock
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns csvDataSetMock
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock

            // when
            dbRaccoonExtension.beforeTestExecution(contextMock)

            // then
            verify(exactly = 1) { dataSetMock.createDeleteQueryOperator(expectedColumnByTableForDataSet) }
            verify(exactly = 1) { dataSetMock.createInsertQueryOperator(expectedColumnByTableForDataSet) }
            verify(exactly = 2) { queryOperatorMock.executeQueries(connMock) }
            verify(exactly = 1) { csvDataSetMock.createDeleteQueryOperator(expectedColumnByTableForCsvDataSet) }
            verify(exactly = 1) { csvDataSetMock.createInsertQueryOperator(expectedColumnByTableForCsvDataSet) }
            verify(exactly = 2) { csvQueryOperatorMock.executeQueries(connMock) }
            verify(exactly = 1) {
                storeMock.put(
                        "columnMetadataByTable",
                        // merged map is passed
                        mapOf("table" to mapOf("1" to ColType.INTEGER), "table2" to mapOf("1" to ColType.INTEGER))
                )
            }
        }

        @ParameterizedTest(name = "when {0} is passed")
        @CsvSource(value = ["BEFORE_AND_AFTER_TEST", "AFTER_TEST"])
        fun `afterTestExecution delete works`(cleanupPhase: CleanupPhase) {
            // mocks
            val (dataSourceMock, connMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()
            val (csvDataSetMock, csvMetadataOperatorMock, csvQueryOperatorMock) = csvDataSetMocks()
            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            val expectedColumnByTable = mapOf("table" to mapOf("1" to ColType.INTEGER))
            every { storeMock.remove(any()) } returns expectedColumnByTable

            // given
            val dbRaccoonExtension = spyk(
                    DbRaccoonExtension(
                            dataSourceMock,
                            cleanupPhase
                    )
            )
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock
            every { dbRaccoonExtension.getDataSet(contextMock) } returns dataSetMock
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns csvDataSetMock

            // when
            dbRaccoonExtension.afterTestExecution(contextMock)

            // then
            verify(exactly = 1) { dataSetMock.createDeleteQueryOperator(expectedColumnByTable) }
            verify(exactly = 1) { queryOperatorMock.executeQueries(connMock) }
            verify(exactly = 1) { csvDataSetMock.createDeleteQueryOperator(expectedColumnByTable) }
            verify(exactly = 1) { csvQueryOperatorMock.executeQueries(connMock) }
            verify(exactly = 1) { storeMock.remove("columnMetadataByTable") }
        }

        @Test
        fun `beforeTestExecution do nothing when there are no annotations`() {
            // mocks
            val (dataSourceMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()
            val (csvDataSetMock, csvMetadataOperatorMock, csvQueryOperatorMock) = csvDataSetMocks()
            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)

            // given
            val dbRaccoonExtension = spyk(DbRaccoonExtension(dataSourceMock))
            every { dbRaccoonExtension.getDataSet(contextMock) } returns null
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns null
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock

            // when
            dbRaccoonExtension.beforeTestExecution(contextMock)

            // then
            verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 0) { dataSetMock.createInsertQueryOperator(any()) }
            verify(exactly = 0) { metadataOperatorMock.execute(any()) }
            verify(exactly = 0) { queryOperatorMock.executeQueries(any()) }

            verify(exactly = 0) { csvDataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 0) { csvDataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 0) { csvDataSetMock.createInsertQueryOperator(any()) }
            verify(exactly = 0) { csvMetadataOperatorMock.execute(any()) }
            verify(exactly = 0) { csvQueryOperatorMock.executeQueries(any()) }

            verify(exactly = 0) { storeMock.put(any(), any()) }
        }

        @Test
        fun `afterTestExecution do nothing when there are no annotations`() {
            // mocks
            val (dataSourceMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()
            val (csvDataSetMock, csvMetadataOperatorMock, csvQueryOperatorMock) = csvDataSetMocks()
            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            val expectedColumnByTable = mapOf("table" to mapOf("1" to ColType.INTEGER))
            every { storeMock.remove(any()) } returns expectedColumnByTable

            // given
            val dbRaccoonExtension = spyk(DbRaccoonExtension(dataSourceMock))
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock
            every { dbRaccoonExtension.getDataSet(contextMock) } returns null
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns null

            // when
            dbRaccoonExtension.afterTestExecution(contextMock)

            // then
            verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 0) { dataSetMock.createInsertQueryOperator(any()) }
            verify(exactly = 0) { queryOperatorMock.executeQueries(any()) }

            verify(exactly = 0) { csvDataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 0) { csvDataSetMock.createInsertQueryOperator(any()) }
            verify(exactly = 0) { csvQueryOperatorMock.executeQueries(any()) }

            verify(exactly = 1) { storeMock.remove("columnMetadataByTable") }
        }

        @ParameterizedTest(name = "when {0} is passed")
        @CsvSource(value = ["BEFORE_TEST"])
        fun `afterTestExecution delete does not work`(cleanupPhase: CleanupPhase) {
            // mocks
            val (dataSourceMock, connMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()
            val (csvDataSetMock, csvMetadataOperatorMock, csvQueryOperatorMock) = csvDataSetMocks()

            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            val expectedColumnByTable = mapOf("table" to mapOf("1" to ColType.INTEGER))
            every { storeMock.remove(any()) } returns expectedColumnByTable

            // given
            val dbRaccoonExtension = spyk(
                    DbRaccoonExtension(
                            dataSourceMock,
                            cleanupPhase
                    )
            )
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock
            every { dbRaccoonExtension.getDataSet(contextMock) } returns dataSetMock
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns csvDataSetMock

            // when
            dbRaccoonExtension.afterTestExecution(contextMock)

            // then
            verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 0) { queryOperatorMock.executeQueries(any()) }
            verify(exactly = 0) { csvDataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 0) { csvQueryOperatorMock.executeQueries(any()) }
            verify(exactly = 0) { storeMock.remove(any()) }
        }

        @Test
        fun `afterTestExecution do nothing when there is no store data`() {
            // mocks
            val (dataSourceMock) = dataSourceMocks()
            val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
            val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()
            val (csvDataSetMock, csvMetadataOperatorMock, csvQueryOperatorMock) = csvDataSetMocks()
            val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)
            // no store data
            every { storeMock.remove(any()) } returns null

            // given
            val dbRaccoonExtension = spyk(DbRaccoonExtension(dataSourceMock))
            every { dbRaccoonExtension.getStore(contextMock) } returns storeMock
            every { dbRaccoonExtension.getDataSet(contextMock) } returns dataSetMock
            every { dbRaccoonExtension.getCsvDataSet(contextMock) } returns csvDataSetMock

            // when
            dbRaccoonExtension.afterTestExecution(contextMock)

            // then
            verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 0) { dataSetMock.createInsertQueryOperator(any()) }
            verify(exactly = 0) { queryOperatorMock.executeQueries(any()) }

            verify(exactly = 0) { csvDataSetMock.createDeleteQueryOperator(any()) }
            verify(exactly = 0) { csvDataSetMock.createInsertQueryOperator(any()) }
            verify(exactly = 0) { csvQueryOperatorMock.executeQueries(any()) }

            verify(exactly = 1) { storeMock.remove("columnMetadataByTable") }
        }
    }
}