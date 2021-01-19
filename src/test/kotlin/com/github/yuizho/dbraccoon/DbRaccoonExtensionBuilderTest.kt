package com.github.yuizho.dbraccoon

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import javax.sql.DataSource

class DbRaccoonExtensionBuilderTest {
    @Test
    fun `the builder of DbRaccoonExtension works fine`() {
        // mocks
        val dataSourceMock = mock(DataSource::class.java)

        // when
        // TODO: add cleanup strategy
        val actual = DbRaccoonExtension.Builder(dataSourceMock)
                .cleanupPhase(CleanupPhase.BEFORE_TEST)
                .setUpQueries(listOf("query1", "query2"))
                .tearDownQueries(listOf("query3"))
                .build()
        val actualDataSource = getPirvateFieldValue<DataSource>(actual, "dataSource")
        val actualCleanupPhase = getPirvateFieldValue<CleanupPhase>(actual, "cleanupPhase")
        val actualSetUpQueries = getPirvateFieldValue<List<String>>(actual, "setUpQueries")
        val actualTearDownQueries = getPirvateFieldValue<List<String>>(actual, "tearDownQueries")

        // then
        assertThat(actualDataSource).isEqualTo(dataSourceMock)
        assertThat(actualCleanupPhase).isEqualTo(CleanupPhase.BEFORE_TEST)
        assertThat(actualSetUpQueries).containsExactly("query1", "query2")
        assertThat(actualTearDownQueries).containsExactly("query3")
    }

    private fun <T> getPirvateFieldValue(obj: Any, fieldName: String): T {
        return obj.javaClass.getDeclaredField(fieldName).let { field ->
            field.isAccessible = true
            field.get(obj)
        } as T
    }
}