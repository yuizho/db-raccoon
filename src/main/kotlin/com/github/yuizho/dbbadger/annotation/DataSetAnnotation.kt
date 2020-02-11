package com.github.yuizho.dbbadger.annotation

import com.github.yuizho.dbbadger.ColType

@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
annotation class DataSet(val testData: Array<Table>)

annotation class Table(val name: String, val rows: Array<Row>, val types: Array<TypeHint> = [])

annotation class TypeHint(val name: String, val type: ColType)

annotation class Row(val vals: Array<Col>)

annotation class Col(val name: String, val value: String, val isId: Boolean = false)