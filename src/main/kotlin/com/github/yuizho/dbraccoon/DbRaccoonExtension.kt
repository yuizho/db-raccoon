package com.github.yuizho.dbraccoon

import com.github.yuizho.dbraccoon.annotation.CsvDataSet
import com.github.yuizho.dbraccoon.annotation.DataSet
import com.github.yuizho.dbraccoon.operation.ColumnMetadataByTable
import com.github.yuizho.dbraccoon.operation.PlainQueryOperator
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
 * DbRaccoonExtension dbRaccoonExtension;
 * {
 *     JdbcDataSource dataSource = new JdbcDataSource();
 *     dataSource.setUrl("jdbc:h2:file:./target/db-raccoon");
 *     dataSource.setUser("sa");
 *     dbRaccoonExtension = new DbRaccoonExtension.Builder(dataSource)
 *         .cleanupPhase(CleanupPhase.BEFORE_AND_AFTER_TEST)
 *         .setUpQueries(Arrays.asList("SET REFERENTIAL_INTEGRITY FALSE"))
 *         .tearDownQueries(Arrays.asList("SET REFERENTIAL_INTEGRITY TRUE))
 *         .build();
 * }
 * ```
 *
 * ### Kotlin
 * ```
 * companion object {
 *     @JvmField
 *     @RegisterExtension
 *     val dbRaccoonExtension = DbRaccoonExtension(
 *             dataSource = JdbcDataSource().also {
 *                 it.setUrl("jdbc:h2:file:./target/db-raccoon")
 *                 it.user = "sa"
 *             },
 *             cleanupPhase = CleanupPhase.BEFORE_TEST,
 *             setUpQueries = listOf("SET REFERENTIAL_INTEGRITY FALSE"),
 *             tearDownQueries = listOf("SET REFERENTIAL_INTEGRITY TRUE")
 *     )
 * }
 * ```
 *
 * @property dataSource the jdbc data source to connect the database
 * @property cleanupPhase the execution phase of the cleanup task (Optional. CleanupPhase.BEFORE_AND_AFTER_TEST is default.)
 * @property cleanupStrategy the strategy of the cleanup task (Optional. CleanupStrategy.USED_ROWS is default.)
 * @property setUpQueries the queries to execute before clean-insert tasks on beforeTestExecution (Optional)
 * @property tearDownQueries the queries to execute after clean tasks on afterTestExecution (Optional)
 */
class DbRaccoonExtension @JvmOverloads constructor(
        private val dataSource: DataSource,
        private val cleanupPhase: CleanupPhase = CleanupPhase.BEFORE_AND_AFTER_TEST,
        private val cleanupStrategy: CleanupStrategy = CleanupStrategy.USED_ROWS,
        private val setUpQueries: List<String> = emptyList(),
        private val tearDownQueries: List<String> = emptyList()
) : BeforeTestExecutionCallback, AfterTestExecutionCallback {
    companion object {
        private const val COLUMN_BY_TABLE = "columnMetadataByTable"
        private val logger: Logger = LoggerFactory.getLogger(DbRaccoonExtension::class.java)
    }

    /**
     * The builder class to create DbRaccoonExtension instance.
     *
     * @since 0.2.0
     * @property dataSource the jdbc data source to connect the database
     */
    class Builder(private val dataSource: DataSource) {
        private var cleanupPhaseValue: CleanupPhase = CleanupPhase.BEFORE_AND_AFTER_TEST
        private var cleanupStrategy: CleanupStrategy = CleanupStrategy.USED_ROWS
        private var setUpQueriesValue: List<String> = emptyList()
        private var tearDownQueriesValue: List<String> = emptyList()

        /**
         * The setter class to set CleanupPhase value.
         *
         * @param value the execution phase of the cleanup task (Optional. CleanupPhase.BEFORE_AND_AFTER_TEST is default.)
         * @return this Builder instance
         */
        fun cleanupPhase(value: CleanupPhase): Builder {
            cleanupPhaseValue = value
            return this
        }

        /**
         * The setter to set CleanupStrategy value.
         *
         * @param value the strategy of the cleanup task (Optional. CleanupStrategy.USED_ROWS is default.)
         * @return this Builder instance
         */
        fun cleanupStrategy(value: CleanupStrategy): Builder {
            cleanupStrategy = value
            return this
        }

        /**
         * The setter class to set setUpQueries value.
         *
         * @param value the queries to execute before clean-insert tasks on beforeTestExecution (Optional)
         * @return this Builder instance
         */
        fun setUpQueries(value: List<String>): Builder {
            setUpQueriesValue = value
            return this
        }

        /**
         * The setter class to set tearDownQueries value
         *
         * @param value the queries to execute after clean tasks on afterTestExecution (Optional)
         * @return this Builder instance
         */
        fun tearDownQueries(value: List<String>): Builder {
            tearDownQueriesValue = value
            return this
        }

        /**
         * The build method to create DbRaccoonExtension instance.
         *
         * @return DbRaccoonExtension instance
         */
        fun build(): DbRaccoonExtension = DbRaccoonExtension(
                dataSource = dataSource,
                cleanupPhase = cleanupPhaseValue,
                cleanupStrategy = cleanupStrategy,
                setUpQueries = setUpQueriesValue,
                tearDownQueries = tearDownQueriesValue
        )
    }

    /**
     * The Callback method that is invoked immediately before each test.
     *
     * @param context the test context
     */
    override fun beforeTestExecution(context: ExtensionContext) {
        logger.info("start test data preparation before test execution")
        val columnMetadataByTable = dataSource.connection.use { conn ->
            if (setUpQueries.isNotEmpty()) {
                logger.info("start executing set up queries")
                PlainQueryOperator(setUpQueries).executeQueries(conn)
            }

            // When @DataSet is neither applied to Method nor Class, do nothing
            val dataSetMetas = getDataSet(context)?.let { dataSet ->
                logger.info("start handling @DataSet test data")
                val metas = dataSet.createColumnMetadataOperator().execute(conn)
                if (cleanupPhase.shouldCleanupBeforeTestExecution) {
                    dataSet.createDeleteQueryOperator(metas, cleanupStrategy).executeQueries(conn)
                }
                dataSet.createInsertQueryOperator(metas).executeQueries(conn)
                metas
            } ?: emptyMap()

            // When @CsvDataSet is neither applied to Method nor Class, do nothing
            val csvDataSetMetas = getCsvDataSet(context)?.let { csvDataSet ->
                logger.info("start handling @CsvDataSet test data")
                val metas = csvDataSet.createColumnMetadataOperator().execute(conn)
                if (cleanupPhase.shouldCleanupBeforeTestExecution) {
                    csvDataSet.createDeleteQueryOperator(metas).executeQueries(conn)
                }
                csvDataSet.createInsertQueryOperator(metas).executeQueries(conn)
                metas
            } ?: emptyMap()

            // merge meta data maps reutned by each DataSet annotation
            (dataSetMetas + csvDataSetMetas)
        }

        // store column metadata map to pass after test execution phase
        if (columnMetadataByTable.isNotEmpty()) {
            getStore(context).put(COLUMN_BY_TABLE, columnMetadataByTable)
        }
    }

    /**
     * The Callback method that is invoked immediately after each test.
     *
     * @param context the test context
     */
    override fun afterTestExecution(context: ExtensionContext) {
        logger.info("start test data cleanup after test execution")
        val metas = getStore(context).remove(COLUMN_BY_TABLE) as? ColumnMetadataByTable ?: return

        dataSource.connection.use { conn ->
            if (cleanupPhase.shouldCleanupAfterTestExecution) {
                // When @DataSet is neither applied to Method nor Class, do nothing
                getDataSet(context)?.also { dataSet ->
                    logger.info("start handling @DataSet test data")
                    dataSet.createDeleteQueryOperator(metas, cleanupStrategy).executeQueries(conn)
                }

                // When @CsvDataSet is neither applied to Method nor Class, do nothing
                getCsvDataSet(context)?.also { csvDataSet ->
                    logger.info("start handling @CsvDataSet test data")
                    csvDataSet.createDeleteQueryOperator(metas).executeQueries(conn)
                }
            }

            if (tearDownQueries.isNotEmpty()) {
                logger.info("start executing tear down queries")
                PlainQueryOperator(tearDownQueries).executeQueries(conn)
            }
        }
    }

    internal fun getDataSet(context: ExtensionContext): DataSet? {
        val testMethod = context.requiredTestMethod
        return testMethod.getAnnotationFromMethodOrClass(DataSet::class.java)
    }

    internal fun getCsvDataSet(context: ExtensionContext): CsvDataSet? {
        val testMethod = context.requiredTestMethod
        return testMethod.getAnnotationFromMethodOrClass(CsvDataSet::class.java)
    }

    internal fun getStore(context: ExtensionContext): ExtensionContext.Store {
        return context.getStore(ExtensionContext.Namespace.create(javaClass, context.requiredTestMethod))
    }

    private fun <T : Annotation> Method.getAnnotationFromMethodOrClass(annotationClass: Class<T>): T? {
        return getAnnotation(annotationClass)
                ?: declaringClass.getAnnotation(annotationClass)
    }
}