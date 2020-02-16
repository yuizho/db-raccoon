package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import java.sql.Connection
import java.sql.SQLException


class QueryOperatorTest {
    @Test
    fun `executeQueries works when collect parameters are passed`() {
        // mocks
        val connMock = mock(Connection::class.java)
        val queryMock1 = mock(Query::class.java)
        val queryMock2 = mock(Query::class.java)

        // when
        QueryOperator(listOf(queryMock1, queryMock2)).executeQueries(connMock)

        // then
        verify(connMock).autoCommit = false
        verify(connMock).commit()
        verify(queryMock1).execute(connMock)
        verify(queryMock2).execute(connMock)
    }

    @Test
    fun `executeQueries works(rollback) when Exception is thrown by query object`() {
        // mocks
        val connMock = mock(Connection::class.java)
        val queryMock = mock(Query::class.java)
        `when`(queryMock.execute(connMock)).thenAnswer { throw SQLException("some error") }

        // when
        assertThatThrownBy { QueryOperator(listOf(queryMock)).executeQueries(connMock) }
                .isExactlyInstanceOf(DbRaccoonException::class.java)
                .hasCauseExactlyInstanceOf(SQLException::class.java)

        // then
        verify(connMock).autoCommit = false
        verify(connMock).rollback()
    }
}