package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.ColType
import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import java.sql.Connection
import java.sql.SQLException


class ColumnMetaDataScanOperatorTest {
    @Test
    fun `execute works when collect parameters are passed`() {
        // mocks
        val connMock = mock(Connection::class.java)
        val scannerMock = mock(ColumnMetadataScanner::class.java)
        `when`(scannerMock.tableName).thenReturn("table_name")
        `when`(scannerMock.execute(connMock)).thenReturn(mapOf("id" to ColType.INTEGER))

        // when
        val actual = ColumnMetadataScanOperator(listOf(scannerMock)).execute(connMock)

        // then
        assertThat(actual).containsEntry("table_name", mapOf("id" to ColType.INTEGER))
        verify(connMock).autoCommit = false
        verify(scannerMock).execute(connMock)
    }

    @Test
    fun `execute works(rollback) when Exception is thrown by scanner object`() {
        // mocks
        val connMock = mock(Connection::class.java)
        val scannerMock = mock(ColumnMetadataScanner::class.java)
        `when`(scannerMock.execute(connMock)).thenAnswer { throw SQLException("some error") }

        // when
        assertThatThrownBy { ColumnMetadataScanOperator(listOf(scannerMock)).execute(connMock) }
                .isExactlyInstanceOf(DbRaccoonException::class.java)
                .hasCauseExactlyInstanceOf(SQLException::class.java)

        // then
        verify(connMock).autoCommit = false
        verify(connMock).rollback()
    }
}