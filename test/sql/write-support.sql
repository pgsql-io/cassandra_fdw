--
-- DDL
--

DROP FOREIGN TABLE IF EXISTS write_type_mapping;

CREATE FOREIGN TABLE write_type_mapping (
    id int,
    smallint_value smallint DEFAULT 0,
    int_value int,
    bigint_value bigint,
    bool_value boolean,
    float_value float4,
    double_value float8,
    text_value text,
    ascii_value text,
    varchar_value text
) SERVER cass_serv OPTIONS (
    schema_name 'example', table_name 'write_type_mapping', primary_key 'id'
);

--
-- DML
--

--
-- DELETE
--

-- DELETE if rows exist.  Not strictly necessary with the Cassandra
-- (KEY-predicated) "UPSERT" behavior but we get to sanity-test our DELETE
-- support.

DELETE FROM write_type_mapping WHERE ID IN (1, 2, 3);

SELECT * FROM write_type_mapping;

--
-- INSERT
--

INSERT INTO write_type_mapping
    (id, int_value, bigint_value, bool_value, float_value, double_value, text_value, varchar_value)
VALUES
    (1, 1, 1, TRUE, 1.2, 1.2, 'foo', 'foo');

INSERT INTO write_type_mapping
    (id, int_value, bigint_value, bool_value, float_value, double_value, text_value, varchar_value)
VALUES
    (2, 2, 2, FALSE, 2.1, 2.1, 'bar', 'bar');

INSERT INTO write_type_mapping
    (ID, int_value, bigint_value, bool_value, float_value, double_value, text_value, varchar_value)
VALUES
    (3, 3, 3, FALSE, 3.2, 3.2, 'baz', 'baz');

--
-- UPDATE
--

-- SET a boolean value to FALSE.

SELECT id, bool_value FROM write_type_mapping WHERE id = 1;

UPDATE write_type_mapping SET bool_value = FALSE WHERE id = 1;

SELECT id, bool_value FROM write_type_mapping WHERE id = 1;

-- SET a boolean value to TRUE.

SELECT id, bool_value FROM write_type_mapping WHERE id = 3;

UPDATE write_type_mapping SET bool_value = TRUE WHERE id = 3;

SELECT id, bool_value FROM write_type_mapping WHERE id = 3;

-- UPDATE a bigint value.

SELECT id, bigint_value FROM write_type_mapping WHERE id = 3;

UPDATE write_type_mapping SET bigint_value = 5 WHERE id = 3;

SELECT id, bigint_value FROM write_type_mapping WHERE id = 3;

-- UPDATE a double value.

SELECT id, double_value FROM write_type_mapping WHERE id = 3;

UPDATE write_type_mapping SET double_value = 3.14159 WHERE id = 3;

SELECT id, double_value FROM write_type_mapping WHERE id = 3;

--
-- (More) DELETEs
--

DELETE FROM write_type_mapping WHERE id IN (1, 2);
DELETE FROM write_type_mapping WHERE double_value = 3.14159;

SELECT COUNT(id) FROM write_type_mapping;
