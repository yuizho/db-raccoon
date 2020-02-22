create table type(
    int_c INTEGER not null,
    boolean_c BOOLEAN not null,
    smallint_c SMALLINT not null,
    bigint_c BIGINT not null,
    decimal_c DECIMAL(15, 5) not null,
    double_c DOUBLE PRECISION not null,
    float_c FLOAT not null,
    real_c REAL not null,
    time_c TIME(6) not null,
    date_c DATE not null,
    timestamp_c TIMESTAMP(6) not null,
    binary_c BYTEA null,
    varchar_c VARCHAR(40) not null,
    char_c CHAR(14) not null,
    clob_c TEXT not null,
    PRIMARY KEY(int_c)
);