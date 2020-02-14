package com.github.yuizho.dbraccoon

enum class CleanupPhase(internal val shouldCleanupBeforeTestExecution: Boolean,
                        internal val shouldCleanupAfterTestExecution: Boolean) {
    BeforeTest(true, false),
    AfterTest(false, true),
    BeforeAndAfterTest(true, true)
}