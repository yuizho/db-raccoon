package com.github.yuizho.dbbadger

enum class CleanupPhase(internal val shouldCleanupBeforeTestExecution: Boolean,
                        internal val shouldCleanupAfterTestExecution: Boolean) {
    BeforeTest(true, false),
    AfterTest(false, true),
    BeforeAndAfterTest(true, true)
}