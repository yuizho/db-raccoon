package com.github.yuizho.dbraccoon.operation

import com.github.yuizho.dbraccoon.ColType
import com.github.yuizho.dbraccoon.exception.DbRaccoonDataSetException
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import java.sql.Connection
import java.sql.PreparedStatement


class QueryTest {
    @Test
    fun `execute works when collect parameters are passed`() {
        // mocks
        val connMock = mock(Connection::class.java)
        val pstmtMock = mock(PreparedStatement::class.java)
        `when`(connMock.prepareStatement(anyString())).thenReturn(pstmtMock)

        // given
        val sql = "insert int foo value(?, ?)"
        val query = Query(sql,
                listOf(
                        Query.Parameter("1", ColType.INTEGER),
                        Query.Parameter("name", ColType.DEFAULT)
                )
        )

        // when
        query.execute(connMock)

        // then
        verify(connMock).prepareStatement(sql)
        verify(pstmtMock).setObject(1, 1, ColType.INTEGER.sqlType)
        verify(pstmtMock).setString(2, "name")
        verify(pstmtMock).executeUpdate()
    }

    @Test
    fun `execute throws DbRaccoonDataSetException when incollect parameters are passed`() {
        // mocks
        val connMock = mock(Connection::class.java)
        val pstmtMock = mock(PreparedStatement::class.java)
        `when`(connMock.prepareStatement(anyString())).thenReturn(pstmtMock)

        // given
        val sql = "insert int foo value(?)"
        val query = Query(sql, listOf(Query.Parameter("1aa", ColType.INTEGER)))

        // when
        assertThatThrownBy { query.execute(connMock) }
                .isExactlyInstanceOf(DbRaccoonDataSetException::class.java)
    }
}