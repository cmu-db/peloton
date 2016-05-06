DROP FUNCTION IF EXISTS concat_text_plpgsql(text, text);
CREATE OR REPLACE FUNCTION concat_text_plpgsql(x text, y text) RETURNS text AS
$$ DECLARE strresult text;
BEGIN
    strresult := x || y;
    RETURN strresult;
END;
$$ LANGUAGE 'plpgsql';


DROP FUNCTION IF EXISTS calc_tax_plpgsql(price decimal(5,2));

CREATE OR REPLACE FUNCTION calc_tax_plpgsql(price decimal(5,2)) RETURNS float8 AS
$$ DECLARE tax float8;
BEGIN
    tax := 0.0;
    IF price < 10.0 THEN
        tax := 0.0;
    ELSIF price < 50.0 THEN
        tax := 0.06 * (price - 10.0);
    ELSE
        tax := 0.06 * (50.0 - 10.0) + 0.09 * (price - 50.0);
    END IF;
    RETURN tax;
END;
$$ LANGUAGE 'plpgsql';

DROP FUNCTION IF EXISTS replace_vowel_plpgsql(text);
CREATE OR REPLACE FUNCTION replace_vowel_plpgsql(x text) RETURNS text AS
$$ DECLARE result text;
BEGIN
    result := regexp_replace(x, '[aeiou]', '*', 'g');
    RETURN result;
END;
$$ LANGUAGE 'plpgsql';


DROP FUNCTION IF EXISTS integer_manipulate_plpgsql(integer);
CREATE OR REPLACE FUNCTION integer_manipulate_plpgsql(x integer) RETURNS integer AS
$$ DECLARE result integer;
BEGIN
    result := (x * 9 + 999) / 5 - 100;
    RETURN result;
END;
$$ LANGUAGE 'plpgsql';

DROP FUNCTION IF EXISTS insert_table_plpgsql(integer);
CREATE FUNCTION insert_table_plpgsql(num integer) RETURNS void AS
$$ DECLARE i numeric;
BEGIN
    i = 0;
    WHILE i < num LOOP
        INSERT INTO A VALUES(1);
        i := i + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS item_sales_sum_plpgsql(int);
CREATE OR REPLACE FUNCTION item_sales_sum_plpgsql(item_id int)
RETURNS numeric AS $$
     DECLARE tmp RECORD; result numeric;
     BEGIN
          result := 0.00;
          FOR tmp IN select item.i_price from order_line,item where order_line.ol_i_id = item_id and order_line.ol_i_id = item.i_id LOOP
              result := result + tmp.i_price;
          END LOOP;
     RETURN result;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS modify_table_plpgsql();
CREATE FUNCTION modify_table_plpgsql() RETURNS void AS
$$ DECLARE i numeric;
BEGIN
    i = 0;
    WHILE i < 500 LOOP
        UPDATE A SET test=99999 where test=i;
        i := i + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS multiply_string(integer, text);

CREATE OR REPLACE FUNCTION multiply_string(times integer, msg text) RETURNS TEXT AS
$$ DECLARE result text;
BEGIN
    result := '';
    IF times > 0 THEN
        FOR i IN 1 .. times LOOP
            result := result || msg || E'\r\n';
        END LOOP;
    END IF;
    RETURN result;
END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;


DROP FUNCTION IF EXISTS countdown_plpgsql(integer);

CREATE OR REPLACE FUNCTION countdown_plpgsql(start integer) RETURNS text AS
$$ DECLARE
    result text := '';
    sql text := '';
    tmp text := '';
BEGIN
    sql := 'SELECT n AS countdown
            FROM generate_series(' || CAST(start AS text) || ', 1, -1) AS n ';
    FOR tmp IN EXECUTE(sql) LOOP
        NULL;
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;


DROP FUNCTION IF EXISTS fib_plpgsql(integer);

CREATE OR REPLACE FUNCTION fib_plpgsql(num integer) RETURNS integer AS
$$
DECLARE
    result int := 0;
    a int := 1;
    b int := 1;
    c int := 0;
BEGIN
    IF num < 1 THEN
        result := -1;
    ELSIF num = 1 OR num = 2 THEN
        result := 1;
    ELSE
        FOR i IN 3 .. num LOOP
            c := a + b;
            a := b;
            b := c;
        END LOOP;
        result := c;
    END IF;
    RETURN result;
END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;