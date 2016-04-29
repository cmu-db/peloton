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