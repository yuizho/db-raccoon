package com.github.yuizho.dbbadger

import com.github.yuizho.dbbadger.annotation.DataSet
import com.github.yuizho.dbbadger.processor.createDeleteQueryOperator
import com.github.yuizho.dbbadger.processor.createInsertQueryOperator
import org.junit.jupiter.api.extension.AfterTestExecutionCallback
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.reflect.Method
import javax.sql.DataSource

class DbBadgerExtension @JvmOverloads constructor(
        private val dataSource: DataSource,
        private val cleanupPhase: CleanupPhase = CleanupPhase.BeforeAndAfterTest
) : BeforeTestExecutionCallback, AfterTestExecutionCallback {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(DbBadgerExtension::class.java)
    }

    override fun beforeTestExecution(context: ExtensionContext) {
        // When @DataSet is neither applied to Method nor Class, do nothing
        val testMethod = context.requiredTestMethod
        val dataSet = testMethod.getAnnotationFromMethodOrClass(DataSet::class.java) ?: return

        logger.info("start test data preparation before test execution")
        val conn = dataSource.connection
        if (cleanupPhase.shouldCleanupBeforeTestExecution) {
            dataSet.createDeleteQueryOperator().executeQueries(conn)
        }
        dataSet.createInsertQueryOperator().executeQueries(conn)
    }

    override fun afterTestExecution(context: ExtensionContext) {
        // When @DataSet is neither applied to Method nor Class, do nothing
        val testMethod = context.requiredTestMethod
        val dataSet = testMethod.getAnnotationFromMethodOrClass(DataSet::class.java) ?: return

        val conn = dataSource.connection
        if (cleanupPhase.shouldCleanupAfterTestExecution) {
            logger.info("start test data cleanup after test execution")
            dataSet.createDeleteQueryOperator().executeQueries(conn)
        }
    }

    private fun <T : Annotation> Method.getAnnotationFromMethodOrClass(annotationClass: Class<T>): T? {
        return getAnnotation(annotationClass)
                ?: declaringClass.getAnnotation(annotationClass)
    }
}