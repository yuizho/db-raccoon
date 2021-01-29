package com.github.yuizho.dbraccoon

/**
 * A definition of cleanup strategy
 */
enum class CleanupStrategy {
    /**
     * This strategy only deletes test data row.
     */
    USED_ROWS,

    /**
     * This strategy deletes all rows of the table which was inserted test data.
     */
    USED_TABLES
}