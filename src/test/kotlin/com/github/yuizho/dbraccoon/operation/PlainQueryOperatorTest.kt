package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement


class PlainQueryOperatorTest {
    @Test
    fun `executeQueries works when collect queries are passed`() {
        // mocks
        val connMock = mock(Connection::class.java)
        val stmtMock = mock(Statement::class.java)
        `when`(connMock.createStatement()).thenReturn(stmtMock)

        // when
        PlainQueryOperator(listOf("query1", "query2")).executeQueries(connMock)

        // then
        verify(connMock).autoCommit = false
        verify(connMock).commit()
        verify(stmtMock).addBatch("query1")
        verify(stmtMock).addBatch("query2")
        verify(stmtMock).executeBatch()
    }

    @Test
    fun `executeQueries works(rollback) when Exception is thrown by query object`() {
        // mocks
        val connMock = mock(Connection::class.java)
        val stmtMock = mock(Statement::class.java)
        `when`(connMock.createStatement()).thenReturn(stmtMock)
        `when`(stmtMock.executeBatch()).thenAnswer { throw SQLException("some error") }

        // when
        assertThatThrownBy { PlainQueryOperator(listOf("query1")).executeQueries(connMock) }
                .isExactlyInstanceOf(DbRaccoonException::class.java)
                .hasCauseExactlyInstanceOf(SQLException::class.java)

        // then
        verify(connMock).autoCommit = false
        verify(connMock).rollback()
    }
}