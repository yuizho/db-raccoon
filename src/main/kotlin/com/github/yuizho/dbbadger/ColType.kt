package com.github.yuizho.dbbadger

import java.sql.Types

enum class ColType(val type: Int) {
    INTEGER(Types.INTEGER),
    DEFAULT(Types.VARCHAR)
}