CREATE TABLE udf_test_1 (col INT[]);

INSERT INTO udf_test_1 VALUES({1, 2, 3, 4})
INSERT INTO udf_test_1 VALUES({2, 3, 4, 5})
INSERT INTO udf_test_1 VALUES({3, 4, 5, 6})
INSERT INTO udf_test_1 VALUES({4, 5, 6, 7})

CREATE OR REPLACE FUNCTION array_add(array1 int[], array2 int[]) RETURNS int[] AS $$
DECLARE
    result int[] := ARRAY[]::integer[];
    l int;
BEGIN
    ---
    --- First check if either input is NULL, and return the other if it is
    ---
    IF array1 IS NULL OR array1 = '{}' THEN
        RETURN array2;
    ELSEIF array2 IS NULL OR array2 = '{}' THEN
        RETURN array1;
    END IF;

    l := array_upper(array2, 1);

    SELECT array_agg(array1[i] + array2[i]) FROM generate_series(1, l) i INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
