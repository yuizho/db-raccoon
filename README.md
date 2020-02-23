# DbRaccoon 🦝
[![Actions Status](https://github.com/yuizho/db-raccoon/workflows/build/badge.svg)](https://github.com/yuizho/db-raccoon/actions)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=yuizho_db-raccoon&metric=alert_status)](https://sonarcloud.io/dashboard?id=yuizho_db-raccoon)

A JUnit5 extension to make test data setup easier.

You can write test data definition on your test codes by the annotations.
And the test data is cleaned up and is inserted by 🦝 !

## Features
- cleanup-insert the specified test data before test execution
- cleanup the specified test data after test execution

## Usage

### Registering DbRaccoonExtension
You can register DbRaccoonExtension programmatically by annotating the field in your test classes that are written in JUnit 5.

You have to set several parameters when creating an Instance of DbRaccoonExtension.

#### dataSource
The JDBC data source object to connect to the database that should be inserted test data.

#### cleanupPhase
The execution phase of the cleanup task (BEFORE_AND_AFTER_TEST is the default).

#### Java

```Java
@RegisterExtension
 private final DbRaccoonExtension dbRaccoonExtension;
 {
     JdbcDataSource dataSource = new JdbcDataSource();
     dataSource.setUrl("jdbc:h2:file:./target/db-raccoon");
     dataSource.setUser("sa");
     dbRaccoonExtension = new DbRaccoonExtension(dataSource);
 }
```

#### Kotlin

```kotlin
kotlincompanion object {
     @JvmField
     @RegisterExtension
     val dbRaccoon = DbRaccoonExtension(
         dataSource = JdbcDataSource().also {
             it.setUrl("jdbc:h2:file:./target/db-raccoon")
             it.user = "sa"
         },
        cleanupPhase = CleanupPhase.BEFORE_AND_AFTER_TEST
     )
 }
```

For more information about JUnit 5 Extension Model, please refer to this document.
https://junit.org/junit5/docs/current/user-guide/#extensions

### Setting up test data
You can apply `@DataSet` annotation to the test class or method that needs test data before execution.

When `@DataSet` is applied to both the test class and the test method, the annotation applied to the method will be used

#### Java

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

#### Kotlin

```kotlin
@Test
@DataSet([
     Table("parent", [
         Row([
             Col("id", "2", true),
             Col("name", "class-parent")
         ])
     ]),
     Table("child", [
         Row([
             Col("id", "2", true),
             Col("name", "class-child"),
             Col("parent_id", "2")
         ])
     ])
])
fun `test`() {
    // ...
}
```

#### Id column
At least one Id column (`@Col.isId` parameter is true) requires in each `@Row`.
The Id column is used when the cleanup task is executed.

### Converting specified values
String instances defined as Column value are converted to the following types corresponding to `ColType`.

`ColType` is obtained by Table scannning in default.
But you can also specify explicitly by `@TypeHint`.


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

The conversion is executed before building a SQL.

#### Binary data conversion
When you specify binary data, convert the value into base64 string.


```kotlin
Col("binary_column", "YWJjZGVmZzE=")
```

#### Conversion Table

TODO: write all pattern

|  ColType  |  Example  |
| ---- | ---- |
|  VARBINARY<br>LONGVARBINARY  |  "abcdef" ->   |
|  BINARY  |  "abcdef" ->   |
|  BLOB  |  "abcdef" ->   |
|  BOOLEAN<br>BIT  |  "true" ->  true |


## License
[MIT License](https://github.com/yuizho/db-raccoon/blob/master/LICENSE)
