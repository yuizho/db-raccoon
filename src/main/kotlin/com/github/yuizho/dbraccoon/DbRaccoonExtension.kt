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

class DbRaccoonExtension @JvmOverloads constructor(
        private val dataSource: DataSource,
        private val cleanupPhase: CleanupPhase = CleanupPhase.BEFORE_AND_AFTER_TEST
) : BeforeTestExecutionCallback, AfterTestExecutionCallback {
    companion object {
        const val COLUMN_BY_TABLE = "columnMetadataByTable"
        val logger: Logger = LoggerFactory.getLogger(DbRaccoonExtension::class.java)
    }

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