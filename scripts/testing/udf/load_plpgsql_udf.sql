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


DROP FUNCTION IF EXISTS multiply_string(times integer, msg text);

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


DROP FUNCTION IF EXISTS countdown(start integer);