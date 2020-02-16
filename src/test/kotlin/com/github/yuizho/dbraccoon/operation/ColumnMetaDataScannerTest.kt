package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.ColType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Statement


class ColumnMetaDataScannerTest {
    @Test
    fun `execute works when collect parameters are passed`() {
        // mocks
        val connMock = mock(Connection::class.java)
        val stmtMock = mock(Statement::class.java)
        val rsMock = mock(ResultSet::class.java)
        val rsMetaMock = mock(ResultSetMetaData::class.java)
        `when`(connMock.createStatement()).thenReturn(stmtMock)
        `when`(stmtMock.executeQuery(anyString())).thenReturn(rsMock)
        `when`(rsMock.metaData).thenReturn(rsMetaMock)
        `when`(rsMetaMock.columnCount).thenReturn(1)
        `when`(rsMetaMock.getColumnName(1)).thenReturn("ID")
        `when`(rsMetaMock.getColumnType(1)).thenReturn(java.sql.Types.INTEGER)

        // when
        val actual = ColumnMetadataScanner("name").execute(connMock)

        // then
        assertThat(actual).containsEntry("id", ColType.INTEGER)
    }
}