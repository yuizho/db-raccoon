package com.github.yuizho.dbraccoon

/**
 * The enum that defines the phase of the cleanup task.
 *
 * @property shouldCleanupBeforeTestExecution whether the cleanup task executes before the test execution
 * @property shouldCleanupAfterTestExecution whether the cleanup task executes after the test execution
 */
enum class CleanupPhase(internal val shouldCleanupBeforeTestExecution: Boolean,
                        internal val shouldCleanupAfterTestExecution: Boolean) {
    BEFORE_TEST(true, false),
    AFTER_TEST(false, true),
    BEFORE_AND_AFTER_TEST(true, true)
}