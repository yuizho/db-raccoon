package com.github.yuizho.dbraccoon.processor

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class CsvParserTest {
    @Test
    fun `csv parse works`() {
        val csv = """ 
            id, name
            1, foo
            2, 'foo'
            3, 'foo, bar'
            4, 'foo\nbar'
            5, 'foo\'bar'
            6,\'foo\'
            7, '"foo bar"'
            8, "foo"
            9 , foo
            10,foo
            11, あいうえお
            12,
            13, ''
            14, [null]
            15, '[null]'
        """.trimIndent()

        val actual = CsvParser("[null]").parse(csv)

        assertThat(actual)
                .containsExactly(
                        mapOf("id" to "1", "name" to "foo"),
                        mapOf("id" to "2", "name" to "foo"),
                        mapOf("id" to "3", "name" to "foo, bar"),
                        mapOf("id" to "4", "name" to "foo\nbar"),
                        mapOf("id" to "5", "name" to "foo'bar"),
                        mapOf("id" to "6", "name" to "'foo'"),
                        mapOf("id" to "7", "name" to "\"foo bar\""),
                        mapOf("id" to "8", "name" to "\"foo\""),
                        mapOf("id" to "9", "name" to "foo"),
                        mapOf("id" to "10", "name" to "foo"),
                        mapOf("id" to "11", "name" to "あいうえお"),
                        mapOf("id" to "12", "name" to ""),
                        mapOf("id" to "13", "name" to ""),
                        mapOf("id" to "14", "name" to null),
                        mapOf("id" to "15", "name" to null)
                )
    }

    @Test
    fun `IllegalArgumentException is thrown when duplicateHeader is defined`() {
        val csv = """ 
            id, name, id
            1, foo, 2
            """.trimIndent()

        assertThatThrownBy {
            CsvParser("[null]").parse(csv)
        }.isExactlyInstanceOf(IllegalArgumentException::class.java)
    }
}