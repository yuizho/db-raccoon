# DbRaccoon 🦝
[![Actions Status](https://github.com/yuizho/db-raccoon/workflows/build/badge.svg)](https://github.com/yuizho/db-raccoon/actions)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=yuizho_db-raccoon&metric=alert_status)](https://sonarcloud.io/dashboard?id=yuizho_db-raccoon)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.yuizho/db-raccoon/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.yuizho/db-raccoon)
[![javadoc](https://javadoc.io/badge2/com.github.yuizho/db-raccoon/javadoc.svg)](https://javadoc.io/doc/com.github.yuizho/db-raccoon)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/yuizho/db-raccoon/blob/master/LICENSE)

A JUnit5 extension to make the test data setup easier.

You can write the test data definition on your test codes by the annotations.
And the test data is cleaned up and inserted by 🦝 !

## Features
- cleanup-insert the specified test data before test execution
- cleanup the specified test data after test execution

## Usage

### Registering DbRaccoonExtension
You can register DbRaccoonExtension programmatically by annotating the field in your test classes that are written in JUnit 5.

For more information about JUnit 5 Extension Model, please refer to this document.
https://junit.org/junit5/docs/current/user-guide/#extensions

#### The parameters to create DbRaccoonExtension instance
| Parameter | Required / Optional | Default value | Description |
| ---- | ---- | ---- | ---- |
| dataSource | Required | - | The JDBC data source object to connect to the database that should be registered test data. For more details about the options, see  [CleanupPhase options](#cleanupphase-optoins) . |
| cleanupPhase | Optional | `CleanupPhase.BEFORE_AND_AFTER_TEST` | The execution phase of the cleanup task. |
| setUpQueries | Optional | - | The queries to execute before clean-insert tasks on beforeTestExecution. For example, when you want to temporarily disable a foreign key constraint, you can set the query here. |
| tearDownQueries | Optional | - | The queries to execute after clean tasks on afterTestExecution. For example, when you want to enable again the foreign key constraint, you can set the query here. |

#### Java
For Java, We recommend using builder API when you create the object.

```java
@RegisterExtension
DbRaccoonExtension dbRaccoonExtension;
{
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setUrl("jdbc:h2:file:./target/db-raccoon");
    dataSource.setUser("sa");
    dbRaccoonExtension = new DbRaccoonExtension.Builder(dataSource)
        .cleanupPhase(CleanupPhase.BEFORE_AND_AFTER_TEST) // clean up the test data on before and after each test
        .setUpQueries(Arrays.asList("SET REFERENTIAL_INTEGRITY FALSE")) // disable a foreign key constraint in H2 before each test
        .tearDownQueries(Arrays.asList("SET REFERENTIAL_INTEGRITY TRUE")) // enable a foreign key constraint in H2 after each test
        .build();
}
```

#### Kotlin

```kotlin
companion object {
    @JvmField
    @RegisterExtension
    val dbRaccoonExtension = DbRaccoonExtension(
            dataSource = MysqlDataSource().also {
                it.setUrl("jdbc:mysql://127.0.0.1:13306/testdb")
                it.user = "test"
                it.password = "password"
            },
            cleanupPhase = CleanupPhase.BEFORE_TEST, // clean up the test data on before each test
            setUpQueries = listOf("SET FOREIGN_KEY_CHECKS = 0"), // disable a foreign key constraint in MySQL before each test
            tearDownQueries = listOf("SET FOREIGN_KEY_CHECKS = 1") // disable a foreign key constraint in MySQL before each test
    )
}
```

### CleanupPhase optoins
You can configure the execution phase of the cleanup task that deletes the test data by CleanupPhase enum.

| The option name | Description |
| ---- | ---- |
| BEFORE_TEST | The cleanup task just executes before each test case. |
| AFTER_TEST | The cleanup task just executes after each test case. |
| BEFORE_AND_AFTER_TEST |The cleanup task executes  before and after each test case. |

### The annotations to set up the test data
You can use the following annotations to the test class or method that needs test data before execution.

Each test data is inserted in a defined order. And the inserted test data is cleaned up at the timing that is specified by the [CleanupPhase options](#cleanupphase-optoins) .

When the annotations are applied to both the test class and the test method, the annotation applied to the method will be used

#### @DataSet
`@DataSet` allows you to define the test data by several annotations.

##### Java

```java
@Test
@DataSet(testData = {
     @Table(name = "parent_table", rows = {
         @Row(columns = {
             @Col(name = "id", value = "2", isId = true),
             @Col(name = "name", value = "parent_record")
         })
     }),
     @Table(name = "child_table", rows = {
         @Row(columns = {
             @Col(name = "id", value = "2", isId = true),
             @Col(name = "name", value = "child_record"),
             @Col(name = "parent_table_id", value = "2"),
         })
     })
})
public void test() {
    // ...
}
```

##### Kotlin

```kotlin
@Test
@DataSet([
     Table("parent_table", [
         Row([
             Col("id", "2", true),
             Col("name", "parent_record")
         ])
     ]),
     Table("child_table", [
         Row([
             Col("id", "2", true),
             Col("name", "child_record"),
             Col("parent_id", "2")
         ])
     ])
])
fun `test`() {
    // ...
}
```

##### Id column
At least one Id column (`isId` parameter in `@Col` is true) requires in each `@Row`.
The Id column is used when the cleanup task is executed.

#### @CsvDataSet (since 0.2.0)
`@DataSet` allows you to define the test data as comma-separated values.

##### Java
```java
@Test
@CsvDataSet(testData = {
   @CsvTable(name = "parent_table", rows = {
       "id, name",
       "1, parent_record"
   }, id = "id"),
   @CsvTable(name = "child_table", rows = {
       "id, name, parent_id",
       "1, child_record, 1"
   }, id = "id")
})
public void test() {
  // ...
}
```

##### Kotlin
```kotlin
@Test
@CsvDataSet([
   CsvTable("parent_table", [
       "id, name",
       "1, parent_record"
   ], ["id"]),
   CsvTable("child_table", [
       "id, name, parent_id",
       "1, child_record, 1"
   ], ["id"])
])
fun `test`() {
  // ...
}
 ```

##### Id column
At least one id column requires. The Id column is used when the delete task is executed.

##### Null value
When you define null data, you can use the null value string like this.

```kotlin
@CsvDataSet([
   CsvTable("table", [
       "id, name",
       "1, [null]" // name is registered as null
   ], ["id"])
])
```

And when you change the null value string, you can define own null value string.

```kotlin
@CsvDataSet([
   CsvTable("table", [
       "id, name",
       "1, <NULL>" // name is registered as null
   ], ["id"])
], "<NULL>")
```

##### The csv style
###### Header
The first row of the comma-separated values is a header row to set the column names.

###### Quote, Escape character
You can use a single quote (') as the quote character.
And you can also use backslash (\\) as the escape character. Refer char_column value in the following examples.

```kotlin
@CsvDataSet([
   CsvTable(name = "sample_table", rows = [
       "id, char_column, timestamp_column",
       "1, 'foo, \'bar\'', '2014-01-10 12:33:49.123'"
   ], id = ["id"])
])
```

##### The example of csv parsing
| `@CsvTable.rows` Example  | Parsing Result |
| ---- | ---- |
| "col1, col2",<br>"foo, bar" | { "col1": "foo", "col2": "bar" } |
| "col1, col2",<br>"'foo, bar', baz" | { "col1": "foo, bar", "col2": "baz" } |
| "col1, col2",<br>"\\'foo\\', bar" | { "col1": "'foo'", "col2": "bar" } |
| "col1, col2",<br>"'foo, \\'bar\\'', baz" | { "col1": "foo, 'bar'", "col2": "baz" } |
| "col1, col2",<br>"foo, ''" | { "col1": "foo", "col2": "" } |
| "col1, col2",<br>"foo, " | { "col1": "foo", "col2": "" } |
| "col1, col2",<br>"foo, [null]" | { "col1": "foo", "col2": null } |



### Converting specified values
String instances defined as column value are converted to the types corresponding to `ColType`.

`ColType` is obtained by Table scannning in default.
But you can also specify explicitly by `@TypeHint`.

#### @DataSet
```kotlin
@DataSet([
     Table("sample_table", [
         Row([
             Col("id", "1", true),
             Col("binary_column", "YWJjZGVmZzE=") // this column is inserted as BINARY type
         ])
         ], [TypeHint("binary_column", ColType.BINARY)]
     )
 ])
```

#### @CsvDataSet (since 0.2.0)
```kotlin
@CsvDataSet([
   CsvTable("sample_table", [
       "id, binary_column",
       "1, YWJjZGVmZzE=" // binary_column is inserted as binary type
   ], ["id"],
   [TypeHint("binary_column", ColType.BINARY)])
])
```

The conversion is executed before building a SQL.

#### Binary data conversion
When you specify binary data, convert the value into base64 string.

```kotlin
Col("binary_column", "YWJjZGVmZzE=")
```

#### Conversion Table
The column value (`value` parameter in the column) are converted to the following types corresponding to `ColType`.

|  ColType  |  Column Value Example  | Result |
| ---- | ---- | ---- |
|  CHAR<br>VARCHAR<br>LONGVARCHAR  |  "abcdef" | "abcdef" |
|  VARBINARY<br>LONGVARBINARY  |  "abcdef" |  "abcdef".getBytes() |
|  BINARY  |  "YWJjZGUxMjM0NQ=="  | Base64.getDecoder().decode("YWJjZGUxMjM0NQ==") |
|  BLOB  |  "YWJjZGUxMjM0NQ==" | new SerialBlob(Base64.getDecoder().decode("YWJjZGUxMjM0NQ==")) |
|  CLOB  |  "abcdef" | new SerialClob(value.toCharArray()) |
|  BOOLEAN<br>BIT  |  "true" | true |
|  DATE  |  "2019-01-11" | java.sql.Date.valueOf("2019-01-11") |
|  TIME  |  "12:50:59" | java.sql.Time.valueOf("12:50:59") |
|  TIMESTAMP  |  "2019-12-31 01:02:03.123456789" | java.sql.Timestamp.valueOf("2019-12-31 01:02:03.123456789") |
|  TIMESTAMP_WITH_TIMEZONE  |  "2019-10-09T03:53:01+09:00" | java.time.OffsetDateTime.parse("2019-10-09T03:53:01+09:00") |
|  TINYINT  |  "32767" | (short) 32767 |
|  INTEGER<br>SMALLINT  |  "2147483647" | 2147483647 |
|  BIGINT  |  "9223372036854775807" | 9223372036854775807L |
|  REAL  |  "1.0" | 1.0f |
|  FLOAT<br>DOUBLE  |  "1.0" | 1.0d |
|  DECIMAL  |  "1.0" | new BigDecimal("1.0") |



## License
[MIT License](https://github.com/yuizho/db-raccoon/blob/master/LICENSE)
