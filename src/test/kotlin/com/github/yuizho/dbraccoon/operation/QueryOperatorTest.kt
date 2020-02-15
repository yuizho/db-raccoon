package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.ColType
import com.github.yuizho.dbraccoon.exception.DbRaccoonException
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException


class QueryOperatorTest {
    @Test
    fun `executeQueries works(execute query, commmit) when collect parameters are passed`() {
        // mocks
        val connMock = mock(Connection::class.java)
        val pstmtMock = mock(PreparedStatement::class.java)
        `when`(connMock.prepareStatement(anyString())).thenReturn(pstmtMock)

        // given
        val sql = "insert int foo value(?, ?)"
        val queries = listOf(
                Query(sql,
                        listOf(
                                Query.Parameter("1", ColType.INTEGER),
                                Query.Parameter("name", ColType.DEFAULT)
                        )
                )
        )

        // when
        QueryOperator(queries).executeQueries(connMock)

        // then
        verify(connMock).setAutoCommit(false)
        verify(connMock).prepareStatement(sql)
        verify(connMock).commit()
        verify(pstmtMock).setObject(1, 1, ColType.INTEGER.sqlType)
        verify(pstmtMock).setString(2, "name")
        verify(pstmtMock).executeUpdate()
    }

    @Test
    fun `executeQueries works(rollback) when incollect parameters are passed`() {
        // mocks
        val connMock = mock(Connection::class.java)
        `when`(connMock.prepareStatement(anyString())).thenThrow(SQLException("some error"))

        // given
        val sql = "insert int foo value(?)"
        val queries = listOf(
                Query(sql, listOf(Query.Parameter("1", ColType.INTEGER)))
        )

        // when
        assertThatThrownBy { QueryOperator(queries).executeQueries(connMock) }
                .isExactlyInstanceOf(DbRaccoonException::class.java)
                .hasCauseExactlyInstanceOf(SQLException::class.java)

        // then
        verify(connMock).setAutoCommit(false)
        verify(connMock).prepareStatement(sql)
        verify(connMock).rollback()
    }
}