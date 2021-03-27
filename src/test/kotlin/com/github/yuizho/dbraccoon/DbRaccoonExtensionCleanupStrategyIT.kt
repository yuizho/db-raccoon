package com.github.yuizho.dbraccoon

import com.github.yuizho.dbraccoon.annotation.*
import org.assertj.core.api.Assertions.assertThat
import org.h2.jdbcx.JdbcDataSource
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.RegisterExtension

class DbRaccoonExtensionCleanupStrategyIT {
    companion object {
        val dataSource = JdbcDataSource().also {
            it.setUrl("jdbc:h2:file:./target/db-raccoon")
            it.user = "sa"
        }
    }

    @Nested
    @DisplayName("The test cases that CleanupStrategy.USED_TABLES is used")
    inner class UsedTablesCases {
        @RegisterExtension
        @JvmField
        val sampleExtension = DbRaccoonExtension(
            dataSource = dataSource,
            cleanupPhase = CleanupPhase.BEFORE_AND_AFTER_TEST,
            cleanupStrategy = CleanupStrategy.USED_TABLES
        )

        @BeforeEach
        fun setUp() {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeUpdate(
                        "INSERT INTO parent(id, name) VALUES (1, 'inserted by setup')"
                    )
                }
                conn.commit()
            }
        }

        @AfterEach
        fun tearDown() {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT count(*) as cnt FROM parent").use { rs ->
                        // assert that all of the table data is deleted after test
                        rs.next()
                        assertThat(rs.getInt("cnt")).isEqualTo(0)
                    }
                }
            }
        }

        @Test
        @CsvDataSet(
            [
                CsvTable(
                    "parent", [
                        "id, name",
                        "2, inserted by raccoon"
                    ], ["id"]
                )
            ]
        )
        fun `when USED_Tables is used all of the table data is deleted`() {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT count(*) as cnt FROM parent").use { rs ->
                        // assert that all of the table data is deleted before test
                        rs.next()
                        assertThat(rs.getInt("cnt")).isEqualTo(1)
                    }
                }

                // insert another data to check data cleaning of after test
                conn.createStatement().use { stmt ->
                    stmt.executeUpdate(
                        "INSERT INTO parent(id, name) VALUES (1, 'inserted by test method')"
                    )
                }
                conn.commit()
            }
        }
    }

    @Nested
    @DisplayName("The test cases that CleanupStrategy.USED_ROWS is used")
    inner class UsedRowsCases {
        @RegisterExtension
        @JvmField
        val sampleExtension = DbRaccoonExtension(
            dataSource = dataSource,
            cleanupPhase = CleanupPhase.BEFORE_AND_AFTER_TEST,
            cleanupStrategy = CleanupStrategy.USED_ROWS
        )

        @BeforeEach
        fun setUp() {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeUpdate(
                        "DELETE FROM parent"
                    )
                    stmt.executeUpdate(
                        "INSERT INTO parent(id, name) VALUES (1, 'inserted by setup')"
                    )
                }
                conn.commit()
            }
        }

        @AfterEach
        fun tearDown() {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT count(*) as cnt FROM parent").use { rs ->
                        // assert that just used row data is deleted after test
                        rs.next()
                        assertThat(rs.getInt("cnt")).isEqualTo(2)
                    }
                    stmt.executeUpdate(
                        "DELETE FROM parent"
                    )
                }
                conn.commit()
            }
        }

        @Test
        @CsvDataSet(
            [
                CsvTable(
                    "parent", [
                        "id, name",
                        "2, inserted by raccoon"
                    ], ["id"]
                )
            ]
        )
        fun `when USED_Tables is used all of the table data is deleted`() {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT count(*) as cnt FROM parent").use { rs ->
                        // assert that just used row data is deleted before test
                        rs.next()
                        assertThat(rs.getInt("cnt")).isEqualTo(2)
                    }
                }

                // insert another data to check data cleaning of after test
                conn.createStatement().use { stmt ->
                    stmt.executeUpdate(
                        "INSERT INTO parent(id, name) VALUES (3, 'inserted by test method')"
                    )
                }
                conn.commit()
            }
        }
    }
}
