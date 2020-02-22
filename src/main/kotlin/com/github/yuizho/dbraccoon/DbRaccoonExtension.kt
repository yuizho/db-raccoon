package com.github.yuizho.dbraccoon

import com.github.yuizho.dbraccoon.annotation.DataSet
import com.github.yuizho.dbraccoon.operation.ColumnMetadataByTable
import com.github.yuizho.dbraccoon.processor.createColumnMetadataOperator
import com.github.yuizho.dbraccoon.processor.createDeleteQueryOperator
import com.github.yuizho.dbraccoon.processor.createInsertQueryOperator
import org.junit.jupiter.api.extension.AfterTestExecutionCallback
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.reflect.Method
import javax.sql.DataSource

/**
 * The JUnit 5 extension class of [DbRaccoon](https://github.com/yuizho/db-raccoon).
 *
 * ## features
 * - clean-insert the specified test data before test execution
 * - clean the specified test data after test execution
 *
 * ## usage
 * You can register this extension programmatically by annotating the field in your test classes.
 *
 * ### Java
 * ```
 * @RegisterExtension
 * private final DbRaccoonExtension dbRaccoonExtension;
 * {
 *     JdbcDataSource dataSource = new JdbcDataSource();
 *     dataSource.setUrl("jdbc:h2:file:./target/db-raccoon");
 *     dataSource.setUser("sa");
 *     dbRaccoonExtension = new DbRaccoonExtension(dataSource);
 * }
 * ```
 *
 * ### Kotlin
 * ```
 * companion object {
 *     @JvmField
 *     @RegisterExtension
 *     val dbRaccoon = DbRaccoonExtension(
 *         dataSource = JdbcDataSource().also {
 *             it.setUrl("jdbc:h2:file:./target/db-raccoon")
 *             it.user = "sa"
 *         }
 *     )
 * }
 * ```
 *
 * @property dataSource the jdbc data source to connect the database
 * @property cleanupPhase the execution phase of the cleanup task (BEFORE_AND_AFTER_TEST is default)
 */
class DbRaccoonExtension @JvmOverloads constructor(
        private val dataSource: DataSource,
        private val cleanupPhase: CleanupPhase = CleanupPhase.BEFORE_AND_AFTER_TEST
) : BeforeTestExecutionCallback, AfterTestExecutionCallback {
    companion object {
        const val COLUMN_BY_TABLE = "columnMetadataByTable"
        val logger: Logger = LoggerFactory.getLogger(DbRaccoonExtension::class.java)
    }

    /**
     * The Callback method that is invoked immediately before each test.
     *
     * @param context the test context
     */
    override fun beforeTestExecution(context: ExtensionContext) {
        // When @DataSet is neither applied to Method nor Class, do nothing
        val dataSet = getDataSet(context) ?: return

        logger.info("start test data preparation before test execution")
        val columnMetadataByTable = dataSource.connection.use { conn ->
            val metas = dataSet.createColumnMetadataOperator().execute(conn)
            if (cleanupPhase.shouldCleanupBeforeTestExecution) {
                dataSet.createDeleteQueryOperator(metas).executeQueries(conn)
            }
            dataSet.createInsertQueryOperator(metas).executeQueries(conn)
            metas
        }

        // store column metadata map to pass after test execution phase
        getStore(context).put(COLUMN_BY_TABLE, columnMetadataByTable)
    }

    /**
     * The Callback method that is invoked immediately after each test.
     *
     * @param context the test context
     */
    override fun afterTestExecution(context: ExtensionContext) {
        // When @DataSet is neither applied to Method nor Class, do nothing
        val dataSet = getDataSet(context) ?: return
        if (!cleanupPhase.shouldCleanupAfterTestExecution) {
            return
        }

        dataSource.connection.use { conn ->
            logger.info("start test data cleanup after test execution")
            val metas = getStore(context).remove(COLUMN_BY_TABLE) as ColumnMetadataByTable
            dataSet.createDeleteQueryOperator(metas).executeQueries(conn)
        }
    }

    internal fun getDataSet(context: ExtensionContext): DataSet? {
        val testMethod = context.requiredTestMethod
        return testMethod.getAnnotationFromMethodOrClass(DataSet::class.java)
    }

    internal fun getStore(context: ExtensionContext): ExtensionContext.Store {
        return context.getStore(ExtensionContext.Namespace.create(javaClass, context.requiredTestMethod))
    }

    private fun <T : Annotation> Method.getAnnotationFromMethodOrClass(annotationClass: Class<T>): T? {
        return getAnnotation(annotationClass)
                ?: declaringClass.getAnnotation(annotationClass)
    }
}