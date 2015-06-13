-- Language-specific snowball dictionaries
-- src/backend/snowball/snowball_func.sql.in$

SET search_path = pg_catalog;

CREATE FUNCTION dsnowball_init(INTERNAL)
    RETURNS INTERNAL AS '$libdir/dict_snowball', 'dsnowball_init'
LANGUAGE C STRICT;

CREATE FUNCTION dsnowball_lexize(INTERNAL, INTERNAL, INTERNAL, INTERNAL)
    RETURNS INTERNAL AS '$libdir/dict_snowball', 'dsnowball_lexize'
LANGUAGE C STRICT;

CREATE TEXT SEARCH TEMPLATE snowball
	(INIT = dsnowball_init,
	LEXIZE = dsnowball_lexize);

COMMENT ON TEXT SEARCH TEMPLATE snowball IS 'snowball stemmer';
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for danish language
CREATE TEXT SEARCH DICTIONARY danish_stem
	(TEMPLATE = snowball, Language = danish , StopWords=danish);

COMMENT ON TEXT SEARCH DICTIONARY danish_stem IS 'snowball stemmer for danish language';

CREATE TEXT SEARCH CONFIGURATION danish
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION danish IS 'configuration for danish language';

ALTER TEXT SEARCH CONFIGURATION danish ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION danish ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH danish_stem;

ALTER TEXT SEARCH CONFIGURATION danish ADD MAPPING
    FOR word, hword_part, hword
	WITH danish_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for dutch language
CREATE TEXT SEARCH DICTIONARY dutch_stem
	(TEMPLATE = snowball, Language = dutch , StopWords=dutch);

COMMENT ON TEXT SEARCH DICTIONARY dutch_stem IS 'snowball stemmer for dutch language';

CREATE TEXT SEARCH CONFIGURATION dutch
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION dutch IS 'configuration for dutch language';

ALTER TEXT SEARCH CONFIGURATION dutch ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION dutch ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH dutch_stem;

ALTER TEXT SEARCH CONFIGURATION dutch ADD MAPPING
    FOR word, hword_part, hword
	WITH dutch_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for english language
CREATE TEXT SEARCH DICTIONARY english_stem
	(TEMPLATE = snowball, Language = english , StopWords=english);

COMMENT ON TEXT SEARCH DICTIONARY english_stem IS 'snowball stemmer for english language';

CREATE TEXT SEARCH CONFIGURATION english
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION english IS 'configuration for english language';

ALTER TEXT SEARCH CONFIGURATION english ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION english ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH english_stem;

ALTER TEXT SEARCH CONFIGURATION english ADD MAPPING
    FOR word, hword_part, hword
	WITH english_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for finnish language
CREATE TEXT SEARCH DICTIONARY finnish_stem
	(TEMPLATE = snowball, Language = finnish , StopWords=finnish);

COMMENT ON TEXT SEARCH DICTIONARY finnish_stem IS 'snowball stemmer for finnish language';

CREATE TEXT SEARCH CONFIGURATION finnish
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION finnish IS 'configuration for finnish language';

ALTER TEXT SEARCH CONFIGURATION finnish ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION finnish ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH finnish_stem;

ALTER TEXT SEARCH CONFIGURATION finnish ADD MAPPING
    FOR word, hword_part, hword
	WITH finnish_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for french language
CREATE TEXT SEARCH DICTIONARY french_stem
	(TEMPLATE = snowball, Language = french , StopWords=french);

COMMENT ON TEXT SEARCH DICTIONARY french_stem IS 'snowball stemmer for french language';

CREATE TEXT SEARCH CONFIGURATION french
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION french IS 'configuration for french language';

ALTER TEXT SEARCH CONFIGURATION french ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION french ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH french_stem;

ALTER TEXT SEARCH CONFIGURATION french ADD MAPPING
    FOR word, hword_part, hword
	WITH french_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for german language
CREATE TEXT SEARCH DICTIONARY german_stem
	(TEMPLATE = snowball, Language = german , StopWords=german);

COMMENT ON TEXT SEARCH DICTIONARY german_stem IS 'snowball stemmer for german language';

CREATE TEXT SEARCH CONFIGURATION german
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION german IS 'configuration for german language';

ALTER TEXT SEARCH CONFIGURATION german ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION german ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH german_stem;

ALTER TEXT SEARCH CONFIGURATION german ADD MAPPING
    FOR word, hword_part, hword
	WITH german_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for hungarian language
CREATE TEXT SEARCH DICTIONARY hungarian_stem
	(TEMPLATE = snowball, Language = hungarian , StopWords=hungarian);

COMMENT ON TEXT SEARCH DICTIONARY hungarian_stem IS 'snowball stemmer for hungarian language';

CREATE TEXT SEARCH CONFIGURATION hungarian
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION hungarian IS 'configuration for hungarian language';

ALTER TEXT SEARCH CONFIGURATION hungarian ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION hungarian ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH hungarian_stem;

ALTER TEXT SEARCH CONFIGURATION hungarian ADD MAPPING
    FOR word, hword_part, hword
	WITH hungarian_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for italian language
CREATE TEXT SEARCH DICTIONARY italian_stem
	(TEMPLATE = snowball, Language = italian , StopWords=italian);

COMMENT ON TEXT SEARCH DICTIONARY italian_stem IS 'snowball stemmer for italian language';

CREATE TEXT SEARCH CONFIGURATION italian
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION italian IS 'configuration for italian language';

ALTER TEXT SEARCH CONFIGURATION italian ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION italian ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH italian_stem;

ALTER TEXT SEARCH CONFIGURATION italian ADD MAPPING
    FOR word, hword_part, hword
	WITH italian_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for norwegian language
CREATE TEXT SEARCH DICTIONARY norwegian_stem
	(TEMPLATE = snowball, Language = norwegian , StopWords=norwegian);

COMMENT ON TEXT SEARCH DICTIONARY norwegian_stem IS 'snowball stemmer for norwegian language';

CREATE TEXT SEARCH CONFIGURATION norwegian
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION norwegian IS 'configuration for norwegian language';

ALTER TEXT SEARCH CONFIGURATION norwegian ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION norwegian ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH norwegian_stem;

ALTER TEXT SEARCH CONFIGURATION norwegian ADD MAPPING
    FOR word, hword_part, hword
	WITH norwegian_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for portuguese language
CREATE TEXT SEARCH DICTIONARY portuguese_stem
	(TEMPLATE = snowball, Language = portuguese , StopWords=portuguese);

COMMENT ON TEXT SEARCH DICTIONARY portuguese_stem IS 'snowball stemmer for portuguese language';

CREATE TEXT SEARCH CONFIGURATION portuguese
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION portuguese IS 'configuration for portuguese language';

ALTER TEXT SEARCH CONFIGURATION portuguese ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION portuguese ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH portuguese_stem;

ALTER TEXT SEARCH CONFIGURATION portuguese ADD MAPPING
    FOR word, hword_part, hword
	WITH portuguese_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for romanian language
CREATE TEXT SEARCH DICTIONARY romanian_stem
	(TEMPLATE = snowball, Language = romanian );

COMMENT ON TEXT SEARCH DICTIONARY romanian_stem IS 'snowball stemmer for romanian language';

CREATE TEXT SEARCH CONFIGURATION romanian
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION romanian IS 'configuration for romanian language';

ALTER TEXT SEARCH CONFIGURATION romanian ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION romanian ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH romanian_stem;

ALTER TEXT SEARCH CONFIGURATION romanian ADD MAPPING
    FOR word, hword_part, hword
	WITH romanian_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for russian language
CREATE TEXT SEARCH DICTIONARY russian_stem
	(TEMPLATE = snowball, Language = russian , StopWords=russian);

COMMENT ON TEXT SEARCH DICTIONARY russian_stem IS 'snowball stemmer for russian language';

CREATE TEXT SEARCH CONFIGURATION russian
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION russian IS 'configuration for russian language';

ALTER TEXT SEARCH CONFIGURATION russian ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION russian ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH english_stem;

ALTER TEXT SEARCH CONFIGURATION russian ADD MAPPING
    FOR word, hword_part, hword
	WITH russian_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for spanish language
CREATE TEXT SEARCH DICTIONARY spanish_stem
	(TEMPLATE = snowball, Language = spanish , StopWords=spanish);

COMMENT ON TEXT SEARCH DICTIONARY spanish_stem IS 'snowball stemmer for spanish language';

CREATE TEXT SEARCH CONFIGURATION spanish
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION spanish IS 'configuration for spanish language';

ALTER TEXT SEARCH CONFIGURATION spanish ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION spanish ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH spanish_stem;

ALTER TEXT SEARCH CONFIGURATION spanish ADD MAPPING
    FOR word, hword_part, hword
	WITH spanish_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for swedish language
CREATE TEXT SEARCH DICTIONARY swedish_stem
	(TEMPLATE = snowball, Language = swedish , StopWords=swedish);

COMMENT ON TEXT SEARCH DICTIONARY swedish_stem IS 'snowball stemmer for swedish language';

CREATE TEXT SEARCH CONFIGURATION swedish
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION swedish IS 'configuration for swedish language';

ALTER TEXT SEARCH CONFIGURATION swedish ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION swedish ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH swedish_stem;

ALTER TEXT SEARCH CONFIGURATION swedish ADD MAPPING
    FOR word, hword_part, hword
	WITH swedish_stem;
-- src/backend/snowball/snowball.sql.in$

-- text search configuration for turkish language
CREATE TEXT SEARCH DICTIONARY turkish_stem
	(TEMPLATE = snowball, Language = turkish , StopWords=turkish);

COMMENT ON TEXT SEARCH DICTIONARY turkish_stem IS 'snowball stemmer for turkish language';

CREATE TEXT SEARCH CONFIGURATION turkish
	(PARSER = default);

COMMENT ON TEXT SEARCH CONFIGURATION turkish IS 'configuration for turkish language';

ALTER TEXT SEARCH CONFIGURATION turkish ADD MAPPING
	FOR email, url, url_path, host, file, version,
	    sfloat, float, int, uint,
	    numword, hword_numpart, numhword
	WITH simple;

ALTER TEXT SEARCH CONFIGURATION turkish ADD MAPPING
    FOR asciiword, hword_asciipart, asciihword
	WITH turkish_stem;

ALTER TEXT SEARCH CONFIGURATION turkish ADD MAPPING
    FOR word, hword_part, hword
	WITH turkish_stem;
