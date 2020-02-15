create table type(
    int_c INT not null,
    boolean_c BOOLEAN not null,
    tinyint_c TINYINT not null,
    smallint_c SMALLINT not null,
    bigint_c BIGINT not null,
    decimal_c DECIMAL not null,
    double_c DOUBLE not null,
    float_c FLOAT not null,
    real_c REAL not null,
    time_c TIME(6) not null,
    date_c DATE not null,
    timestamp_c TIMESTAMP(6) not null,
    binary_c BINARY(8) null,
    varbinary_c VARBINARY(40) null,
    varchar_c VARCHAR(40) not null,
    char_c CHAR(14) not null,
    blob_c BLOB null,
    clob_c TEXT not null,
    bit_c BIT not null,
    PRIMARY KEY(int_c)
);