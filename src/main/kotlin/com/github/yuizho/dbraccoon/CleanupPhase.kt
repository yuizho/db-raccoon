package com.github.yuizho.dbraccoon

/**
 * The enum that defines the execution phase of the cleanup task.
 *
 * @property shouldCleanupBeforeTestExecution whether the cleanup task executes before the test execution
 * @property shouldCleanupAfterTestExecution whether the cleanup task executes after the test execution
 */
enum class CleanupPhase(internal val shouldCleanupBeforeTestExecution: Boolean,
                        internal val shouldCleanupAfterTestExecution: Boolean) {
    /**
     * The cleanup task just executes before each test case.
     */
    BEFORE_TEST(true, false),
    /**
     * The cleanup task just executes after each test case.
     */
    AFTER_TEST(false, true),
    /**
     * The cleanup task executes before and after each test case.
     */
    BEFORE_AND_AFTER_TEST(true, true)
}