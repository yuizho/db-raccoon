package com.github.yuizho.dbraccoon

import com.github.yuizho.dbraccoon.annotation.DataSet
import com.github.yuizho.dbraccoon.annotation.Table
import com.github.yuizho.dbraccoon.operation.ColumnMetadataScanOperator
import com.github.yuizho.dbraccoon.operation.QueryOperator
import com.github.yuizho.dbraccoon.processor.createColumnMetadataOperator
import com.github.yuizho.dbraccoon.processor.createDeleteQueryOperator
import com.github.yuizho.dbraccoon.processor.createInsertQueryOperator
import io.mockk.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
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
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            unmockkStatic("com.github.yuizho.dbraccoon.processor.DataSetProcessorKt")
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
        every { dbRaccoonExtension.getStore(contextMock) } returns storeMock

        // when
        dbRaccoonExtension.beforeTestExecution(contextMock)

        // then
        verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
        verify(exactly = 1) { dataSetMock.createInsertQueryOperator(expectedColumnByTable) }
        verify(exactly = 1) { queryOperatorMock.executeQueries(connMock) }
        verify(exactly = 1) { storeMock.put("columnMetadataByTable", expectedColumnByTable) }
    }

    @Test
    fun `beforeTestExecution do nothing when there is no @DataSet annotation`() {
        // mocks
        val (dataSourceMock) = dataSourceMocks()
        val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
        val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()
        val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)

        // given
        val dbRaccoonExtension = spyk(DbRaccoonExtension(dataSourceMock))
        every { dbRaccoonExtension.getDataSet(contextMock) } returns null

        // when
        dbRaccoonExtension.beforeTestExecution(contextMock)

        // then
        verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
        verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
        verify(exactly = 0) { dataSetMock.createInsertQueryOperator(any()) }
        verify(exactly = 0) { queryOperatorMock.executeQueries(any()) }
        verify(exactly = 0) { storeMock.put(any(), any()) }
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
        every { dbRaccoonExtension.getDataSet(contextMock) } returns dataSetMock
        every { dbRaccoonExtension.getStore(contextMock) } returns storeMock

        // when
        dbRaccoonExtension.afterTestExecution(contextMock)

        // then
        verify(exactly = 1) { dataSetMock.createDeleteQueryOperator(expectedColumnByTable) }
        verify(exactly = 1) { queryOperatorMock.executeQueries(connMock) }
        verify(exactly = 1) { storeMock.remove("columnMetadataByTable") }
    }

    @ParameterizedTest(name = "when {0} is passed")
    @CsvSource(value = ["BEFORE_TEST"])
    fun `afterTestExecution delete does not work`(cleanupPhase: CleanupPhase) {
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
        every { dbRaccoonExtension.getDataSet(contextMock) } returns dataSetMock
        every { dbRaccoonExtension.getStore(contextMock) } returns storeMock

        // when
        dbRaccoonExtension.afterTestExecution(contextMock)

        // then
        verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
        verify(exactly = 0) { queryOperatorMock.executeQueries(any()) }
        verify(exactly = 0) { storeMock.remove(any()) }
    }

    @Test
    fun `afterTestExecution do nothing when there is no @DataSet annotation`() {
        // mocks
        val (dataSourceMock) = dataSourceMocks()
        val contextMock = mockk<ExtensionContext>(relaxUnitFun = true)
        val (dataSetMock, metadataOperatorMock, queryOperatorMock) = dataSetMocks()
        val storeMock = mockk<ExtensionContext.Store>(relaxUnitFun = true)

        // given
        val dbRaccoonExtension = spyk(DbRaccoonExtension(dataSourceMock))
        every { dbRaccoonExtension.getDataSet(contextMock) } returns null

        // when
        dbRaccoonExtension.afterTestExecution(contextMock)

        // then
        verify(exactly = 0) { dataSetMock.createDeleteQueryOperator(any()) }
        verify(exactly = 0) { dataSetMock.createInsertQueryOperator(any()) }
        verify(exactly = 0) { queryOperatorMock.executeQueries(any()) }
        verify(exactly = 0) { storeMock.put(any(), any()) }
    }

    @DataSet([
        Table(name = "onClass", rows = [])
    ])
    private class AppliedOnClass {
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

    private class ClassNoDataSet {
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