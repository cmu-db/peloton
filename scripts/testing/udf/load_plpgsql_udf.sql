DROP FUNCTION IF EXISTS concat_text_plpgsql(text, text);
CREATE OR REPLACE FUNCTION concat_text_plpgsql(x text, y text) RETURNS text AS
$$ DECLARE strresult text;
BEGIN
    strresult := x || y;
    RETURN strresult;
END;
$$ LANGUAGE 'plpgsql';