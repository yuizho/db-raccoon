package com.github.yuizho.dbraccoon.annotation

import com.github.yuizho.dbraccoon.ColType

/**
 * The main annotation to specify the test data.
 *
 * You can apply this annotation to the test method that needs the test data before execution.
 *
 * Each test data is inserted in a defined order. And the inserted test data is cleaned up at the proper timing.
 *
 * When this annotation is applied to both the test class and the test method,
 * the annotation applied to the method will be used
 *
 * ## usage
 * ### Java
 * ```
 * @DataSet(testData = {
 *     @Table(name = "parent_table", rows = {
 *         @Row(columns = {
 *             @Col(name = "id", value = "2", isId = true),
 *             @Col(name = "name", value = "parent_record")
 *         })
 *     }),
 *     @Table(name = "child_table", rows = {
 *         @Row(columns = {
 *             @Col(name = "id", value = "2", isId = true),
 *             @Col(name = "name", value = "child_record"),
 *             @Col(name = "parent_table_id", value = "2"),
 *         })
 *     })
 * })
 * ```
 *
 * ### Kotlin
 * ```
 * @DataSet([
 *     Table("parent", [
 *         Row([
 *             Col("id", "2", true),
 *             Col("name", "class-parent")
 *         ])
 *     ]),
 *     Table("child", [
 *         Row([
 *             Col("id", "2", true),
 *             Col("name", "class-child"),
 *             Col("parent_id", "2")
 *         ])
 *     ])
 * ])
 * ```
 *
 * @property testData test data
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
annotation class DataSet(val testData: Array<Table>)

/**
 * The annotation to specify the table to insert the test data.
 *
 * @property name the table name
 * @property rows rows
 * @property types the column type hints (optional)
 */
annotation class Table(val name: String, val rows: Array<Row>, val types: Array<TypeHint> = [])

/**
 * The annotation to apply a type hint to the sql.
 *
 * By default, the column type is scanned before executing an insert query.
 * But sometimes, the type scanning returns unexpected column types.
 *
 * In this case, you can specify the column type expressly by this annotation.
 *
 * ## usage
 * ### Kotlin
 * ```
 * @DataSet([
 *     Table("sample_table", [
 *         Row([
 *             Col("id", "1", true),
 *             Col("binary_column", "YWJjZGVmZzE=") // this column is inserted as binary type
 *         ])
 *         ], [TypeHint("binary_column", ColType.BINARY)]
 *     )
 * ])
 * ```
 *
 * @property name the column name
 * @property type the column type to identify generic SQL types
 */
annotation class TypeHint(val name: String, val type: ColType)

/**
 * The annotation to specify the row to insert the test data.
 *
 * @property columns columns
 */
annotation class Row(val columns: Array<Col>)

/**
 * The annotation to specify the row to insert the test data.
 *
 * At least one Id column (the Col `isId` parameter is true) requires in each row.
 * The Id column is used when the delete task is executed.
 *
 * When you specify binary data, convert the data into base64 string.
 *
 *
 * ## usage
 * ### Kotlin
 * ```
 * @DataSet([
 *     Table("sample_table", [
 *         Row([
 *             Col("id", "1", true),                                // at least one Id column is required
 *             Col("boolean_column", "true"),                       // boolean type column
 *             Col("time_column", "12:33:49.123"),                  // time type column
 *             Col("date_column", "2014-01-10"),                    // date type column
 *             Col("timestamp_column", "2014-01-10 12:33:49.123"),  // timestamp type column
 *             Col("binary_column", "YWJjZGVmZzE=")                 // binary type column
 *         ])
 *     ])
 * ])
 * ```
 *
 * @property name the column name
 * @property value the column value
 * @property isId whether this column is id (false is default)
 */
annotation class Col(val name: String, val value: String, val isId: Boolean = false)

@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
annotation class CsvDataSet(val testData: Array<CsvTable>, val nullValue: String = "[null]")

annotation class CsvTable(val name: String, val rows: Array<String>, val id: Array<String>, val types: Array<TypeHint> = [])