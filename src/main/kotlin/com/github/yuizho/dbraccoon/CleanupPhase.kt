package com.github.yuizho.dbraccoon

enum class CleanupPhase(internal val shouldCleanupBeforeTestExecution: Boolean,
                        internal val shouldCleanupAfterTestExecution: Boolean) {
    BEFORE_TEST(true, false),
    AFTER_TEST(false, true),
    BEFORE_AND_AFTER_TEST(true, true)
}