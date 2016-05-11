-- SQL_ASCII --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION ascii_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/ascii_and_mic', 'ascii_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION ascii_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for SQL_ASCII to MULE_INTERNAL';
DROP CONVERSION pg_catalog.ascii_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.ascii_to_mic FOR 'SQL_ASCII' TO 'MULE_INTERNAL' FROM ascii_to_mic;
COMMENT ON CONVERSION pg_catalog.ascii_to_mic IS 'conversion for SQL_ASCII to MULE_INTERNAL';
-- MULE_INTERNAL --> SQL_ASCII
CREATE OR REPLACE FUNCTION mic_to_ascii (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/ascii_and_mic', 'mic_to_ascii' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_ascii(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to SQL_ASCII';
DROP CONVERSION pg_catalog.mic_to_ascii;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_ascii FOR 'MULE_INTERNAL' TO 'SQL_ASCII' FROM mic_to_ascii;
COMMENT ON CONVERSION pg_catalog.mic_to_ascii IS 'conversion for MULE_INTERNAL to SQL_ASCII';
-- KOI8R --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION koi8r_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'koi8r_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION koi8r_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for KOI8R to MULE_INTERNAL';
DROP CONVERSION pg_catalog.koi8_r_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.koi8_r_to_mic FOR 'KOI8R' TO 'MULE_INTERNAL' FROM koi8r_to_mic;
COMMENT ON CONVERSION pg_catalog.koi8_r_to_mic IS 'conversion for KOI8R to MULE_INTERNAL';
-- MULE_INTERNAL --> KOI8R
CREATE OR REPLACE FUNCTION mic_to_koi8r (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'mic_to_koi8r' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_koi8r(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to KOI8R';
DROP CONVERSION pg_catalog.mic_to_koi8_r;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_koi8_r FOR 'MULE_INTERNAL' TO 'KOI8R' FROM mic_to_koi8r;
COMMENT ON CONVERSION pg_catalog.mic_to_koi8_r IS 'conversion for MULE_INTERNAL to KOI8R';
-- ISO-8859-5 --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION iso_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'iso_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for ISO-8859-5 to MULE_INTERNAL';
DROP CONVERSION pg_catalog.iso_8859_5_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_5_to_mic FOR 'ISO-8859-5' TO 'MULE_INTERNAL' FROM iso_to_mic;
COMMENT ON CONVERSION pg_catalog.iso_8859_5_to_mic IS 'conversion for ISO-8859-5 to MULE_INTERNAL';
-- MULE_INTERNAL --> ISO-8859-5
CREATE OR REPLACE FUNCTION mic_to_iso (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'mic_to_iso' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_iso(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to ISO-8859-5';
DROP CONVERSION pg_catalog.mic_to_iso_8859_5;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_iso_8859_5 FOR 'MULE_INTERNAL' TO 'ISO-8859-5' FROM mic_to_iso;
COMMENT ON CONVERSION pg_catalog.mic_to_iso_8859_5 IS 'conversion for MULE_INTERNAL to ISO-8859-5';
-- WIN1251 --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION win1251_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'win1251_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION win1251_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1251 to MULE_INTERNAL';
DROP CONVERSION pg_catalog.windows_1251_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.windows_1251_to_mic FOR 'WIN1251' TO 'MULE_INTERNAL' FROM win1251_to_mic;
COMMENT ON CONVERSION pg_catalog.windows_1251_to_mic IS 'conversion for WIN1251 to MULE_INTERNAL';
-- MULE_INTERNAL --> WIN1251
CREATE OR REPLACE FUNCTION mic_to_win1251 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'mic_to_win1251' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_win1251(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to WIN1251';
DROP CONVERSION pg_catalog.mic_to_windows_1251;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_windows_1251 FOR 'MULE_INTERNAL' TO 'WIN1251' FROM mic_to_win1251;
COMMENT ON CONVERSION pg_catalog.mic_to_windows_1251 IS 'conversion for MULE_INTERNAL to WIN1251';
-- WIN866 --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION win866_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'win866_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION win866_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN866 to MULE_INTERNAL';
DROP CONVERSION pg_catalog.windows_866_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.windows_866_to_mic FOR 'WIN866' TO 'MULE_INTERNAL' FROM win866_to_mic;
COMMENT ON CONVERSION pg_catalog.windows_866_to_mic IS 'conversion for WIN866 to MULE_INTERNAL';
-- MULE_INTERNAL --> WIN866
CREATE OR REPLACE FUNCTION mic_to_win866 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'mic_to_win866' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_win866(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to WIN866';
DROP CONVERSION pg_catalog.mic_to_windows_866;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_windows_866 FOR 'MULE_INTERNAL' TO 'WIN866' FROM mic_to_win866;
COMMENT ON CONVERSION pg_catalog.mic_to_windows_866 IS 'conversion for MULE_INTERNAL to WIN866';
-- KOI8R --> WIN1251
CREATE OR REPLACE FUNCTION koi8r_to_win1251 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'koi8r_to_win1251' LANGUAGE C STRICT;
COMMENT ON FUNCTION koi8r_to_win1251(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for KOI8R to WIN1251';
DROP CONVERSION pg_catalog.koi8_r_to_windows_1251;
CREATE DEFAULT CONVERSION pg_catalog.koi8_r_to_windows_1251 FOR 'KOI8R' TO 'WIN1251' FROM koi8r_to_win1251;
COMMENT ON CONVERSION pg_catalog.koi8_r_to_windows_1251 IS 'conversion for KOI8R to WIN1251';
-- WIN1251 --> KOI8R
CREATE OR REPLACE FUNCTION win1251_to_koi8r (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'win1251_to_koi8r' LANGUAGE C STRICT;
COMMENT ON FUNCTION win1251_to_koi8r(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1251 to KOI8R';
DROP CONVERSION pg_catalog.windows_1251_to_koi8_r;
CREATE DEFAULT CONVERSION pg_catalog.windows_1251_to_koi8_r FOR 'WIN1251' TO 'KOI8R' FROM win1251_to_koi8r;
COMMENT ON CONVERSION pg_catalog.windows_1251_to_koi8_r IS 'conversion for WIN1251 to KOI8R';
-- KOI8R --> WIN866
CREATE OR REPLACE FUNCTION koi8r_to_win866 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'koi8r_to_win866' LANGUAGE C STRICT;
COMMENT ON FUNCTION koi8r_to_win866(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for KOI8R to WIN866';
DROP CONVERSION pg_catalog.koi8_r_to_windows_866;
CREATE DEFAULT CONVERSION pg_catalog.koi8_r_to_windows_866 FOR 'KOI8R' TO 'WIN866' FROM koi8r_to_win866;
COMMENT ON CONVERSION pg_catalog.koi8_r_to_windows_866 IS 'conversion for KOI8R to WIN866';
-- WIN866 --> KOI8R
CREATE OR REPLACE FUNCTION win866_to_koi8r (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'win866_to_koi8r' LANGUAGE C STRICT;
COMMENT ON FUNCTION win866_to_koi8r(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN866 to KOI8R';
DROP CONVERSION pg_catalog.windows_866_to_koi8_r;
CREATE DEFAULT CONVERSION pg_catalog.windows_866_to_koi8_r FOR 'WIN866' TO 'KOI8R' FROM win866_to_koi8r;
COMMENT ON CONVERSION pg_catalog.windows_866_to_koi8_r IS 'conversion for WIN866 to KOI8R';
-- WIN866 --> WIN1251
CREATE OR REPLACE FUNCTION win866_to_win1251 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'win866_to_win1251' LANGUAGE C STRICT;
COMMENT ON FUNCTION win866_to_win1251(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN866 to WIN1251';
DROP CONVERSION pg_catalog.windows_866_to_windows_1251;
CREATE DEFAULT CONVERSION pg_catalog.windows_866_to_windows_1251 FOR 'WIN866' TO 'WIN1251' FROM win866_to_win1251;
COMMENT ON CONVERSION pg_catalog.windows_866_to_windows_1251 IS 'conversion for WIN866 to WIN1251';
-- WIN1251 --> WIN866
CREATE OR REPLACE FUNCTION win1251_to_win866 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'win1251_to_win866' LANGUAGE C STRICT;
COMMENT ON FUNCTION win1251_to_win866(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1251 to WIN866';
DROP CONVERSION pg_catalog.windows_1251_to_windows_866;
CREATE DEFAULT CONVERSION pg_catalog.windows_1251_to_windows_866 FOR 'WIN1251' TO 'WIN866' FROM win1251_to_win866;
COMMENT ON CONVERSION pg_catalog.windows_1251_to_windows_866 IS 'conversion for WIN1251 to WIN866';
-- ISO-8859-5 --> KOI8R
CREATE OR REPLACE FUNCTION iso_to_koi8r (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'iso_to_koi8r' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso_to_koi8r(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for ISO-8859-5 to KOI8R';
DROP CONVERSION pg_catalog.iso_8859_5_to_koi8_r;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_5_to_koi8_r FOR 'ISO-8859-5' TO 'KOI8R' FROM iso_to_koi8r;
COMMENT ON CONVERSION pg_catalog.iso_8859_5_to_koi8_r IS 'conversion for ISO-8859-5 to KOI8R';
-- KOI8R --> ISO-8859-5
CREATE OR REPLACE FUNCTION koi8r_to_iso (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'koi8r_to_iso' LANGUAGE C STRICT;
COMMENT ON FUNCTION koi8r_to_iso(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for KOI8R to ISO-8859-5';
DROP CONVERSION pg_catalog.koi8_r_to_iso_8859_5;
CREATE DEFAULT CONVERSION pg_catalog.koi8_r_to_iso_8859_5 FOR 'KOI8R' TO 'ISO-8859-5' FROM koi8r_to_iso;
COMMENT ON CONVERSION pg_catalog.koi8_r_to_iso_8859_5 IS 'conversion for KOI8R to ISO-8859-5';
-- ISO-8859-5 --> WIN1251
CREATE OR REPLACE FUNCTION iso_to_win1251 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'iso_to_win1251' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso_to_win1251(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for ISO-8859-5 to WIN1251';
DROP CONVERSION pg_catalog.iso_8859_5_to_windows_1251;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_5_to_windows_1251 FOR 'ISO-8859-5' TO 'WIN1251' FROM iso_to_win1251;
COMMENT ON CONVERSION pg_catalog.iso_8859_5_to_windows_1251 IS 'conversion for ISO-8859-5 to WIN1251';
-- WIN1251 --> ISO-8859-5
CREATE OR REPLACE FUNCTION win1251_to_iso (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'win1251_to_iso' LANGUAGE C STRICT;
COMMENT ON FUNCTION win1251_to_iso(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1251 to ISO-8859-5';
DROP CONVERSION pg_catalog.windows_1251_to_iso_8859_5;
CREATE DEFAULT CONVERSION pg_catalog.windows_1251_to_iso_8859_5 FOR 'WIN1251' TO 'ISO-8859-5' FROM win1251_to_iso;
COMMENT ON CONVERSION pg_catalog.windows_1251_to_iso_8859_5 IS 'conversion for WIN1251 to ISO-8859-5';
-- ISO-8859-5 --> WIN866
CREATE OR REPLACE FUNCTION iso_to_win866 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'iso_to_win866' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso_to_win866(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for ISO-8859-5 to WIN866';
DROP CONVERSION pg_catalog.iso_8859_5_to_windows_866;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_5_to_windows_866 FOR 'ISO-8859-5' TO 'WIN866' FROM iso_to_win866;
COMMENT ON CONVERSION pg_catalog.iso_8859_5_to_windows_866 IS 'conversion for ISO-8859-5 to WIN866';
-- WIN866 --> ISO-8859-5
CREATE OR REPLACE FUNCTION win866_to_iso (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/cyrillic_and_mic', 'win866_to_iso' LANGUAGE C STRICT;
COMMENT ON FUNCTION win866_to_iso(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN866 to ISO-8859-5';
DROP CONVERSION pg_catalog.windows_866_to_iso_8859_5;
CREATE DEFAULT CONVERSION pg_catalog.windows_866_to_iso_8859_5 FOR 'WIN866' TO 'ISO-8859-5' FROM win866_to_iso;
COMMENT ON CONVERSION pg_catalog.windows_866_to_iso_8859_5 IS 'conversion for WIN866 to ISO-8859-5';
-- EUC_CN --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION euc_cn_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_cn_and_mic', 'euc_cn_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_cn_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_CN to MULE_INTERNAL';
DROP CONVERSION pg_catalog.euc_cn_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.euc_cn_to_mic FOR 'EUC_CN' TO 'MULE_INTERNAL' FROM euc_cn_to_mic;
COMMENT ON CONVERSION pg_catalog.euc_cn_to_mic IS 'conversion for EUC_CN to MULE_INTERNAL';
-- MULE_INTERNAL --> EUC_CN
CREATE OR REPLACE FUNCTION mic_to_euc_cn (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_cn_and_mic', 'mic_to_euc_cn' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_euc_cn(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to EUC_CN';
DROP CONVERSION pg_catalog.mic_to_euc_cn;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_euc_cn FOR 'MULE_INTERNAL' TO 'EUC_CN' FROM mic_to_euc_cn;
COMMENT ON CONVERSION pg_catalog.mic_to_euc_cn IS 'conversion for MULE_INTERNAL to EUC_CN';
-- EUC_JP --> SJIS
CREATE OR REPLACE FUNCTION euc_jp_to_sjis (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_jp_and_sjis', 'euc_jp_to_sjis' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_jp_to_sjis(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_JP to SJIS';
DROP CONVERSION pg_catalog.euc_jp_to_sjis;
CREATE DEFAULT CONVERSION pg_catalog.euc_jp_to_sjis FOR 'EUC_JP' TO 'SJIS' FROM euc_jp_to_sjis;
COMMENT ON CONVERSION pg_catalog.euc_jp_to_sjis IS 'conversion for EUC_JP to SJIS';
-- SJIS --> EUC_JP
CREATE OR REPLACE FUNCTION sjis_to_euc_jp (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_jp_and_sjis', 'sjis_to_euc_jp' LANGUAGE C STRICT;
COMMENT ON FUNCTION sjis_to_euc_jp(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for SJIS to EUC_JP';
DROP CONVERSION pg_catalog.sjis_to_euc_jp;
CREATE DEFAULT CONVERSION pg_catalog.sjis_to_euc_jp FOR 'SJIS' TO 'EUC_JP' FROM sjis_to_euc_jp;
COMMENT ON CONVERSION pg_catalog.sjis_to_euc_jp IS 'conversion for SJIS to EUC_JP';
-- EUC_JP --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION euc_jp_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_jp_and_sjis', 'euc_jp_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_jp_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_JP to MULE_INTERNAL';
DROP CONVERSION pg_catalog.euc_jp_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.euc_jp_to_mic FOR 'EUC_JP' TO 'MULE_INTERNAL' FROM euc_jp_to_mic;
COMMENT ON CONVERSION pg_catalog.euc_jp_to_mic IS 'conversion for EUC_JP to MULE_INTERNAL';
-- SJIS --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION sjis_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_jp_and_sjis', 'sjis_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION sjis_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for SJIS to MULE_INTERNAL';
DROP CONVERSION pg_catalog.sjis_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.sjis_to_mic FOR 'SJIS' TO 'MULE_INTERNAL' FROM sjis_to_mic;
COMMENT ON CONVERSION pg_catalog.sjis_to_mic IS 'conversion for SJIS to MULE_INTERNAL';
-- MULE_INTERNAL --> EUC_JP
CREATE OR REPLACE FUNCTION mic_to_euc_jp (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_jp_and_sjis', 'mic_to_euc_jp' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_euc_jp(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to EUC_JP';
DROP CONVERSION pg_catalog.mic_to_euc_jp;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_euc_jp FOR 'MULE_INTERNAL' TO 'EUC_JP' FROM mic_to_euc_jp;
COMMENT ON CONVERSION pg_catalog.mic_to_euc_jp IS 'conversion for MULE_INTERNAL to EUC_JP';
-- MULE_INTERNAL --> SJIS
CREATE OR REPLACE FUNCTION mic_to_sjis (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_jp_and_sjis', 'mic_to_sjis' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_sjis(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to SJIS';
DROP CONVERSION pg_catalog.mic_to_sjis;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_sjis FOR 'MULE_INTERNAL' TO 'SJIS' FROM mic_to_sjis;
COMMENT ON CONVERSION pg_catalog.mic_to_sjis IS 'conversion for MULE_INTERNAL to SJIS';
-- EUC_KR --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION euc_kr_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_kr_and_mic', 'euc_kr_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_kr_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_KR to MULE_INTERNAL';
DROP CONVERSION pg_catalog.euc_kr_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.euc_kr_to_mic FOR 'EUC_KR' TO 'MULE_INTERNAL' FROM euc_kr_to_mic;
COMMENT ON CONVERSION pg_catalog.euc_kr_to_mic IS 'conversion for EUC_KR to MULE_INTERNAL';
-- MULE_INTERNAL --> EUC_KR
CREATE OR REPLACE FUNCTION mic_to_euc_kr (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_kr_and_mic', 'mic_to_euc_kr' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_euc_kr(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to EUC_KR';
DROP CONVERSION pg_catalog.mic_to_euc_kr;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_euc_kr FOR 'MULE_INTERNAL' TO 'EUC_KR' FROM mic_to_euc_kr;
COMMENT ON CONVERSION pg_catalog.mic_to_euc_kr IS 'conversion for MULE_INTERNAL to EUC_KR';
-- EUC_TW --> BIG5
CREATE OR REPLACE FUNCTION euc_tw_to_big5 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_tw_and_big5', 'euc_tw_to_big5' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_tw_to_big5(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_TW to BIG5';
DROP CONVERSION pg_catalog.euc_tw_to_big5;
CREATE DEFAULT CONVERSION pg_catalog.euc_tw_to_big5 FOR 'EUC_TW' TO 'BIG5' FROM euc_tw_to_big5;
COMMENT ON CONVERSION pg_catalog.euc_tw_to_big5 IS 'conversion for EUC_TW to BIG5';
-- BIG5 --> EUC_TW
CREATE OR REPLACE FUNCTION big5_to_euc_tw (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_tw_and_big5', 'big5_to_euc_tw' LANGUAGE C STRICT;
COMMENT ON FUNCTION big5_to_euc_tw(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for BIG5 to EUC_TW';
DROP CONVERSION pg_catalog.big5_to_euc_tw;
CREATE DEFAULT CONVERSION pg_catalog.big5_to_euc_tw FOR 'BIG5' TO 'EUC_TW' FROM big5_to_euc_tw;
COMMENT ON CONVERSION pg_catalog.big5_to_euc_tw IS 'conversion for BIG5 to EUC_TW';
-- EUC_TW --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION euc_tw_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_tw_and_big5', 'euc_tw_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_tw_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_TW to MULE_INTERNAL';
DROP CONVERSION pg_catalog.euc_tw_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.euc_tw_to_mic FOR 'EUC_TW' TO 'MULE_INTERNAL' FROM euc_tw_to_mic;
COMMENT ON CONVERSION pg_catalog.euc_tw_to_mic IS 'conversion for EUC_TW to MULE_INTERNAL';
-- BIG5 --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION big5_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_tw_and_big5', 'big5_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION big5_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for BIG5 to MULE_INTERNAL';
DROP CONVERSION pg_catalog.big5_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.big5_to_mic FOR 'BIG5' TO 'MULE_INTERNAL' FROM big5_to_mic;
COMMENT ON CONVERSION pg_catalog.big5_to_mic IS 'conversion for BIG5 to MULE_INTERNAL';
-- MULE_INTERNAL --> EUC_TW
CREATE OR REPLACE FUNCTION mic_to_euc_tw (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_tw_and_big5', 'mic_to_euc_tw' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_euc_tw(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to EUC_TW';
DROP CONVERSION pg_catalog.mic_to_euc_tw;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_euc_tw FOR 'MULE_INTERNAL' TO 'EUC_TW' FROM mic_to_euc_tw;
COMMENT ON CONVERSION pg_catalog.mic_to_euc_tw IS 'conversion for MULE_INTERNAL to EUC_TW';
-- MULE_INTERNAL --> BIG5
CREATE OR REPLACE FUNCTION mic_to_big5 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc_tw_and_big5', 'mic_to_big5' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_big5(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to BIG5';
DROP CONVERSION pg_catalog.mic_to_big5;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_big5 FOR 'MULE_INTERNAL' TO 'BIG5' FROM mic_to_big5;
COMMENT ON CONVERSION pg_catalog.mic_to_big5 IS 'conversion for MULE_INTERNAL to BIG5';
-- LATIN2 --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION latin2_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin2_and_win1250', 'latin2_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION latin2_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN2 to MULE_INTERNAL';
DROP CONVERSION pg_catalog.iso_8859_2_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_2_to_mic FOR 'LATIN2' TO 'MULE_INTERNAL' FROM latin2_to_mic;
COMMENT ON CONVERSION pg_catalog.iso_8859_2_to_mic IS 'conversion for LATIN2 to MULE_INTERNAL';
-- MULE_INTERNAL --> LATIN2
CREATE OR REPLACE FUNCTION mic_to_latin2 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin2_and_win1250', 'mic_to_latin2' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_latin2(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to LATIN2';
DROP CONVERSION pg_catalog.mic_to_iso_8859_2;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_iso_8859_2 FOR 'MULE_INTERNAL' TO 'LATIN2' FROM mic_to_latin2;
COMMENT ON CONVERSION pg_catalog.mic_to_iso_8859_2 IS 'conversion for MULE_INTERNAL to LATIN2';
-- WIN1250 --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION win1250_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin2_and_win1250', 'win1250_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION win1250_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1250 to MULE_INTERNAL';
DROP CONVERSION pg_catalog.windows_1250_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.windows_1250_to_mic FOR 'WIN1250' TO 'MULE_INTERNAL' FROM win1250_to_mic;
COMMENT ON CONVERSION pg_catalog.windows_1250_to_mic IS 'conversion for WIN1250 to MULE_INTERNAL';
-- MULE_INTERNAL --> WIN1250
CREATE OR REPLACE FUNCTION mic_to_win1250 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin2_and_win1250', 'mic_to_win1250' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_win1250(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to WIN1250';
DROP CONVERSION pg_catalog.mic_to_windows_1250;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_windows_1250 FOR 'MULE_INTERNAL' TO 'WIN1250' FROM mic_to_win1250;
COMMENT ON CONVERSION pg_catalog.mic_to_windows_1250 IS 'conversion for MULE_INTERNAL to WIN1250';
-- LATIN2 --> WIN1250
CREATE OR REPLACE FUNCTION latin2_to_win1250 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin2_and_win1250', 'latin2_to_win1250' LANGUAGE C STRICT;
COMMENT ON FUNCTION latin2_to_win1250(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN2 to WIN1250';
DROP CONVERSION pg_catalog.iso_8859_2_to_windows_1250;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_2_to_windows_1250 FOR 'LATIN2' TO 'WIN1250' FROM latin2_to_win1250;
COMMENT ON CONVERSION pg_catalog.iso_8859_2_to_windows_1250 IS 'conversion for LATIN2 to WIN1250';
-- WIN1250 --> LATIN2
CREATE OR REPLACE FUNCTION win1250_to_latin2 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin2_and_win1250', 'win1250_to_latin2' LANGUAGE C STRICT;
COMMENT ON FUNCTION win1250_to_latin2(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1250 to LATIN2';
DROP CONVERSION pg_catalog.windows_1250_to_iso_8859_2;
CREATE DEFAULT CONVERSION pg_catalog.windows_1250_to_iso_8859_2 FOR 'WIN1250' TO 'LATIN2' FROM win1250_to_latin2;
COMMENT ON CONVERSION pg_catalog.windows_1250_to_iso_8859_2 IS 'conversion for WIN1250 to LATIN2';
-- LATIN1 --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION latin1_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin_and_mic', 'latin1_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION latin1_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN1 to MULE_INTERNAL';
DROP CONVERSION pg_catalog.iso_8859_1_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_1_to_mic FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic;
COMMENT ON CONVERSION pg_catalog.iso_8859_1_to_mic IS 'conversion for LATIN1 to MULE_INTERNAL';
-- MULE_INTERNAL --> LATIN1
CREATE OR REPLACE FUNCTION mic_to_latin1 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin_and_mic', 'mic_to_latin1' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_latin1(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to LATIN1';
DROP CONVERSION pg_catalog.mic_to_iso_8859_1;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_iso_8859_1 FOR 'MULE_INTERNAL' TO 'LATIN1' FROM mic_to_latin1;
COMMENT ON CONVERSION pg_catalog.mic_to_iso_8859_1 IS 'conversion for MULE_INTERNAL to LATIN1';
-- LATIN3 --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION latin3_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin_and_mic', 'latin3_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION latin3_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN3 to MULE_INTERNAL';
DROP CONVERSION pg_catalog.iso_8859_3_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_3_to_mic FOR 'LATIN3' TO 'MULE_INTERNAL' FROM latin3_to_mic;
COMMENT ON CONVERSION pg_catalog.iso_8859_3_to_mic IS 'conversion for LATIN3 to MULE_INTERNAL';
-- MULE_INTERNAL --> LATIN3
CREATE OR REPLACE FUNCTION mic_to_latin3 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin_and_mic', 'mic_to_latin3' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_latin3(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to LATIN3';
DROP CONVERSION pg_catalog.mic_to_iso_8859_3;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_iso_8859_3 FOR 'MULE_INTERNAL' TO 'LATIN3' FROM mic_to_latin3;
COMMENT ON CONVERSION pg_catalog.mic_to_iso_8859_3 IS 'conversion for MULE_INTERNAL to LATIN3';
-- LATIN4 --> MULE_INTERNAL
CREATE OR REPLACE FUNCTION latin4_to_mic (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin_and_mic', 'latin4_to_mic' LANGUAGE C STRICT;
COMMENT ON FUNCTION latin4_to_mic(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN4 to MULE_INTERNAL';
DROP CONVERSION pg_catalog.iso_8859_4_to_mic;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_4_to_mic FOR 'LATIN4' TO 'MULE_INTERNAL' FROM latin4_to_mic;
COMMENT ON CONVERSION pg_catalog.iso_8859_4_to_mic IS 'conversion for LATIN4 to MULE_INTERNAL';
-- MULE_INTERNAL --> LATIN4
CREATE OR REPLACE FUNCTION mic_to_latin4 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/latin_and_mic', 'mic_to_latin4' LANGUAGE C STRICT;
COMMENT ON FUNCTION mic_to_latin4(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for MULE_INTERNAL to LATIN4';
DROP CONVERSION pg_catalog.mic_to_iso_8859_4;
CREATE DEFAULT CONVERSION pg_catalog.mic_to_iso_8859_4 FOR 'MULE_INTERNAL' TO 'LATIN4' FROM mic_to_latin4;
COMMENT ON CONVERSION pg_catalog.mic_to_iso_8859_4 IS 'conversion for MULE_INTERNAL to LATIN4';
-- SQL_ASCII --> UTF8
CREATE OR REPLACE FUNCTION ascii_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_ascii', 'ascii_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION ascii_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for SQL_ASCII to UTF8';
DROP CONVERSION pg_catalog.ascii_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.ascii_to_utf8 FOR 'SQL_ASCII' TO 'UTF8' FROM ascii_to_utf8;
COMMENT ON CONVERSION pg_catalog.ascii_to_utf8 IS 'conversion for SQL_ASCII to UTF8';
-- UTF8 --> SQL_ASCII
CREATE OR REPLACE FUNCTION utf8_to_ascii (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_ascii', 'utf8_to_ascii' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_ascii(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to SQL_ASCII';
DROP CONVERSION pg_catalog.utf8_to_ascii;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_ascii FOR 'UTF8' TO 'SQL_ASCII' FROM utf8_to_ascii;
COMMENT ON CONVERSION pg_catalog.utf8_to_ascii IS 'conversion for UTF8 to SQL_ASCII';
-- BIG5 --> UTF8
CREATE OR REPLACE FUNCTION big5_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_big5', 'big5_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION big5_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for BIG5 to UTF8';
DROP CONVERSION pg_catalog.big5_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.big5_to_utf8 FOR 'BIG5' TO 'UTF8' FROM big5_to_utf8;
COMMENT ON CONVERSION pg_catalog.big5_to_utf8 IS 'conversion for BIG5 to UTF8';
-- UTF8 --> BIG5
CREATE OR REPLACE FUNCTION utf8_to_big5 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_big5', 'utf8_to_big5' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_big5(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to BIG5';
DROP CONVERSION pg_catalog.utf8_to_big5;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_big5 FOR 'UTF8' TO 'BIG5' FROM utf8_to_big5;
COMMENT ON CONVERSION pg_catalog.utf8_to_big5 IS 'conversion for UTF8 to BIG5';
-- UTF8 --> KOI8R
CREATE OR REPLACE FUNCTION utf8_to_koi8r (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_cyrillic', 'utf8_to_koi8r' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_koi8r(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to KOI8R';
DROP CONVERSION pg_catalog.utf8_to_koi8_r;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_koi8_r FOR 'UTF8' TO 'KOI8R' FROM utf8_to_koi8r;
COMMENT ON CONVERSION pg_catalog.utf8_to_koi8_r IS 'conversion for UTF8 to KOI8R';
-- KOI8R --> UTF8
CREATE OR REPLACE FUNCTION koi8r_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_cyrillic', 'koi8r_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION koi8r_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for KOI8R to UTF8';
DROP CONVERSION pg_catalog.koi8_r_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.koi8_r_to_utf8 FOR 'KOI8R' TO 'UTF8' FROM koi8r_to_utf8;
COMMENT ON CONVERSION pg_catalog.koi8_r_to_utf8 IS 'conversion for KOI8R to UTF8';
-- UTF8 --> KOI8U
CREATE OR REPLACE FUNCTION utf8_to_koi8u (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_cyrillic', 'utf8_to_koi8u' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_koi8u(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to KOI8U';
DROP CONVERSION pg_catalog.utf8_to_koi8_u;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_koi8_u FOR 'UTF8' TO 'KOI8U' FROM utf8_to_koi8u;
COMMENT ON CONVERSION pg_catalog.utf8_to_koi8_u IS 'conversion for UTF8 to KOI8U';
-- KOI8U --> UTF8
CREATE OR REPLACE FUNCTION koi8u_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_cyrillic', 'koi8u_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION koi8u_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for KOI8U to UTF8';
DROP CONVERSION pg_catalog.koi8_u_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.koi8_u_to_utf8 FOR 'KOI8U' TO 'UTF8' FROM koi8u_to_utf8;
COMMENT ON CONVERSION pg_catalog.koi8_u_to_utf8 IS 'conversion for KOI8U to UTF8';
-- UTF8 --> WIN866
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN866';
DROP CONVERSION pg_catalog.utf8_to_windows_866;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_866 FOR 'UTF8' TO 'WIN866' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_866 IS 'conversion for UTF8 to WIN866';
-- WIN866 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN866 to UTF8';
DROP CONVERSION pg_catalog.windows_866_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_866_to_utf8 FOR 'WIN866' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_866_to_utf8 IS 'conversion for WIN866 to UTF8';
-- UTF8 --> WIN874
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN874';
DROP CONVERSION pg_catalog.utf8_to_windows_874;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_874 FOR 'UTF8' TO 'WIN874' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_874 IS 'conversion for UTF8 to WIN874';
-- WIN874 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN874 to UTF8';
DROP CONVERSION pg_catalog.windows_874_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_874_to_utf8 FOR 'WIN874' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_874_to_utf8 IS 'conversion for WIN874 to UTF8';
-- UTF8 --> WIN1250
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN1250';
DROP CONVERSION pg_catalog.utf8_to_windows_1250;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_1250 FOR 'UTF8' TO 'WIN1250' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_1250 IS 'conversion for UTF8 to WIN1250';
-- WIN1250 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1250 to UTF8';
DROP CONVERSION pg_catalog.windows_1250_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_1250_to_utf8 FOR 'WIN1250' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_1250_to_utf8 IS 'conversion for WIN1250 to UTF8';
-- UTF8 --> WIN1251
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN1251';
DROP CONVERSION pg_catalog.utf8_to_windows_1251;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_1251 FOR 'UTF8' TO 'WIN1251' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_1251 IS 'conversion for UTF8 to WIN1251';
-- WIN1251 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1251 to UTF8';
DROP CONVERSION pg_catalog.windows_1251_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_1251_to_utf8 FOR 'WIN1251' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_1251_to_utf8 IS 'conversion for WIN1251 to UTF8';
-- UTF8 --> WIN1252
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN1252';
DROP CONVERSION pg_catalog.utf8_to_windows_1252;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_1252 FOR 'UTF8' TO 'WIN1252' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_1252 IS 'conversion for UTF8 to WIN1252';
-- WIN1252 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1252 to UTF8';
DROP CONVERSION pg_catalog.windows_1252_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_1252_to_utf8 FOR 'WIN1252' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_1252_to_utf8 IS 'conversion for WIN1252 to UTF8';
-- UTF8 --> WIN1253
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN1253';
DROP CONVERSION pg_catalog.utf8_to_windows_1253;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_1253 FOR 'UTF8' TO 'WIN1253' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_1253 IS 'conversion for UTF8 to WIN1253';
-- WIN1253 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1253 to UTF8';
DROP CONVERSION pg_catalog.windows_1253_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_1253_to_utf8 FOR 'WIN1253' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_1253_to_utf8 IS 'conversion for WIN1253 to UTF8';
-- UTF8 --> WIN1254
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN1254';
DROP CONVERSION pg_catalog.utf8_to_windows_1254;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_1254 FOR 'UTF8' TO 'WIN1254' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_1254 IS 'conversion for UTF8 to WIN1254';
-- WIN1254 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1254 to UTF8';
DROP CONVERSION pg_catalog.windows_1254_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_1254_to_utf8 FOR 'WIN1254' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_1254_to_utf8 IS 'conversion for WIN1254 to UTF8';
-- UTF8 --> WIN1255
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN1255';
DROP CONVERSION pg_catalog.utf8_to_windows_1255;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_1255 FOR 'UTF8' TO 'WIN1255' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_1255 IS 'conversion for UTF8 to WIN1255';
-- WIN1255 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1255 to UTF8';
DROP CONVERSION pg_catalog.windows_1255_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_1255_to_utf8 FOR 'WIN1255' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_1255_to_utf8 IS 'conversion for WIN1255 to UTF8';
-- UTF8 --> WIN1256
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN1256';
DROP CONVERSION pg_catalog.utf8_to_windows_1256;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_1256 FOR 'UTF8' TO 'WIN1256' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_1256 IS 'conversion for UTF8 to WIN1256';
-- WIN1256 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1256 to UTF8';
DROP CONVERSION pg_catalog.windows_1256_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_1256_to_utf8 FOR 'WIN1256' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_1256_to_utf8 IS 'conversion for WIN1256 to UTF8';
-- UTF8 --> WIN1257
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN1257';
DROP CONVERSION pg_catalog.utf8_to_windows_1257;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_1257 FOR 'UTF8' TO 'WIN1257' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_1257 IS 'conversion for UTF8 to WIN1257';
-- WIN1257 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1257 to UTF8';
DROP CONVERSION pg_catalog.windows_1257_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_1257_to_utf8 FOR 'WIN1257' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_1257_to_utf8 IS 'conversion for WIN1257 to UTF8';
-- UTF8 --> WIN1258
CREATE OR REPLACE FUNCTION utf8_to_win (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'utf8_to_win' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_win(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to WIN1258';
DROP CONVERSION pg_catalog.utf8_to_windows_1258;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_windows_1258 FOR 'UTF8' TO 'WIN1258' FROM utf8_to_win;
COMMENT ON CONVERSION pg_catalog.utf8_to_windows_1258 IS 'conversion for UTF8 to WIN1258';
-- WIN1258 --> UTF8
CREATE OR REPLACE FUNCTION win_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_win', 'win_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION win_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for WIN1258 to UTF8';
DROP CONVERSION pg_catalog.windows_1258_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.windows_1258_to_utf8 FOR 'WIN1258' TO 'UTF8' FROM win_to_utf8;
COMMENT ON CONVERSION pg_catalog.windows_1258_to_utf8 IS 'conversion for WIN1258 to UTF8';
-- EUC_CN --> UTF8
CREATE OR REPLACE FUNCTION euc_cn_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_euc_cn', 'euc_cn_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_cn_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_CN to UTF8';
DROP CONVERSION pg_catalog.euc_cn_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.euc_cn_to_utf8 FOR 'EUC_CN' TO 'UTF8' FROM euc_cn_to_utf8;
COMMENT ON CONVERSION pg_catalog.euc_cn_to_utf8 IS 'conversion for EUC_CN to UTF8';
-- UTF8 --> EUC_CN
CREATE OR REPLACE FUNCTION utf8_to_euc_cn (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_euc_cn', 'utf8_to_euc_cn' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_euc_cn(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to EUC_CN';
DROP CONVERSION pg_catalog.utf8_to_euc_cn;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_euc_cn FOR 'UTF8' TO 'EUC_CN' FROM utf8_to_euc_cn;
COMMENT ON CONVERSION pg_catalog.utf8_to_euc_cn IS 'conversion for UTF8 to EUC_CN';
-- EUC_JP --> UTF8
CREATE OR REPLACE FUNCTION euc_jp_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_euc_jp', 'euc_jp_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_jp_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_JP to UTF8';
DROP CONVERSION pg_catalog.euc_jp_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.euc_jp_to_utf8 FOR 'EUC_JP' TO 'UTF8' FROM euc_jp_to_utf8;
COMMENT ON CONVERSION pg_catalog.euc_jp_to_utf8 IS 'conversion for EUC_JP to UTF8';
-- UTF8 --> EUC_JP
CREATE OR REPLACE FUNCTION utf8_to_euc_jp (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_euc_jp', 'utf8_to_euc_jp' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_euc_jp(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to EUC_JP';
DROP CONVERSION pg_catalog.utf8_to_euc_jp;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_euc_jp FOR 'UTF8' TO 'EUC_JP' FROM utf8_to_euc_jp;
COMMENT ON CONVERSION pg_catalog.utf8_to_euc_jp IS 'conversion for UTF8 to EUC_JP';
-- EUC_KR --> UTF8
CREATE OR REPLACE FUNCTION euc_kr_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_euc_kr', 'euc_kr_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_kr_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_KR to UTF8';
DROP CONVERSION pg_catalog.euc_kr_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.euc_kr_to_utf8 FOR 'EUC_KR' TO 'UTF8' FROM euc_kr_to_utf8;
COMMENT ON CONVERSION pg_catalog.euc_kr_to_utf8 IS 'conversion for EUC_KR to UTF8';
-- UTF8 --> EUC_KR
CREATE OR REPLACE FUNCTION utf8_to_euc_kr (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_euc_kr', 'utf8_to_euc_kr' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_euc_kr(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to EUC_KR';
DROP CONVERSION pg_catalog.utf8_to_euc_kr;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_euc_kr FOR 'UTF8' TO 'EUC_KR' FROM utf8_to_euc_kr;
COMMENT ON CONVERSION pg_catalog.utf8_to_euc_kr IS 'conversion for UTF8 to EUC_KR';
-- EUC_TW --> UTF8
CREATE OR REPLACE FUNCTION euc_tw_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_euc_tw', 'euc_tw_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_tw_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_TW to UTF8';
DROP CONVERSION pg_catalog.euc_tw_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.euc_tw_to_utf8 FOR 'EUC_TW' TO 'UTF8' FROM euc_tw_to_utf8;
COMMENT ON CONVERSION pg_catalog.euc_tw_to_utf8 IS 'conversion for EUC_TW to UTF8';
-- UTF8 --> EUC_TW
CREATE OR REPLACE FUNCTION utf8_to_euc_tw (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_euc_tw', 'utf8_to_euc_tw' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_euc_tw(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to EUC_TW';
DROP CONVERSION pg_catalog.utf8_to_euc_tw;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_euc_tw FOR 'UTF8' TO 'EUC_TW' FROM utf8_to_euc_tw;
COMMENT ON CONVERSION pg_catalog.utf8_to_euc_tw IS 'conversion for UTF8 to EUC_TW';
-- GB18030 --> UTF8
CREATE OR REPLACE FUNCTION gb18030_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_gb18030', 'gb18030_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION gb18030_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for GB18030 to UTF8';
DROP CONVERSION pg_catalog.gb18030_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.gb18030_to_utf8 FOR 'GB18030' TO 'UTF8' FROM gb18030_to_utf8;
COMMENT ON CONVERSION pg_catalog.gb18030_to_utf8 IS 'conversion for GB18030 to UTF8';
-- UTF8 --> GB18030
CREATE OR REPLACE FUNCTION utf8_to_gb18030 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_gb18030', 'utf8_to_gb18030' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_gb18030(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to GB18030';
DROP CONVERSION pg_catalog.utf8_to_gb18030;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_gb18030 FOR 'UTF8' TO 'GB18030' FROM utf8_to_gb18030;
COMMENT ON CONVERSION pg_catalog.utf8_to_gb18030 IS 'conversion for UTF8 to GB18030';
-- GBK --> UTF8
CREATE OR REPLACE FUNCTION gbk_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_gbk', 'gbk_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION gbk_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for GBK to UTF8';
DROP CONVERSION pg_catalog.gbk_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.gbk_to_utf8 FOR 'GBK' TO 'UTF8' FROM gbk_to_utf8;
COMMENT ON CONVERSION pg_catalog.gbk_to_utf8 IS 'conversion for GBK to UTF8';
-- UTF8 --> GBK
CREATE OR REPLACE FUNCTION utf8_to_gbk (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_gbk', 'utf8_to_gbk' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_gbk(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to GBK';
DROP CONVERSION pg_catalog.utf8_to_gbk;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_gbk FOR 'UTF8' TO 'GBK' FROM utf8_to_gbk;
COMMENT ON CONVERSION pg_catalog.utf8_to_gbk IS 'conversion for UTF8 to GBK';
-- UTF8 --> LATIN2
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to LATIN2';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_2;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_2 FOR 'UTF8' TO 'LATIN2' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_2 IS 'conversion for UTF8 to LATIN2';
-- LATIN2 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN2 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_2_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_2_to_utf8 FOR 'LATIN2' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_2_to_utf8 IS 'conversion for LATIN2 to UTF8';
-- UTF8 --> LATIN3
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to LATIN3';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_3;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_3 FOR 'UTF8' TO 'LATIN3' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_3 IS 'conversion for UTF8 to LATIN3';
-- LATIN3 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN3 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_3_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_3_to_utf8 FOR 'LATIN3' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_3_to_utf8 IS 'conversion for LATIN3 to UTF8';
-- UTF8 --> LATIN4
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to LATIN4';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_4;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_4 FOR 'UTF8' TO 'LATIN4' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_4 IS 'conversion for UTF8 to LATIN4';
-- LATIN4 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN4 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_4_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_4_to_utf8 FOR 'LATIN4' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_4_to_utf8 IS 'conversion for LATIN4 to UTF8';
-- UTF8 --> LATIN5
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to LATIN5';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_9;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_9 FOR 'UTF8' TO 'LATIN5' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_9 IS 'conversion for UTF8 to LATIN5';
-- LATIN5 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN5 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_9_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_9_to_utf8 FOR 'LATIN5' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_9_to_utf8 IS 'conversion for LATIN5 to UTF8';
-- UTF8 --> LATIN6
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to LATIN6';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_10;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_10 FOR 'UTF8' TO 'LATIN6' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_10 IS 'conversion for UTF8 to LATIN6';
-- LATIN6 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN6 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_10_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_10_to_utf8 FOR 'LATIN6' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_10_to_utf8 IS 'conversion for LATIN6 to UTF8';
-- UTF8 --> LATIN7
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to LATIN7';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_13;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_13 FOR 'UTF8' TO 'LATIN7' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_13 IS 'conversion for UTF8 to LATIN7';
-- LATIN7 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN7 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_13_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_13_to_utf8 FOR 'LATIN7' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_13_to_utf8 IS 'conversion for LATIN7 to UTF8';
-- UTF8 --> LATIN8
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to LATIN8';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_14;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_14 FOR 'UTF8' TO 'LATIN8' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_14 IS 'conversion for UTF8 to LATIN8';
-- LATIN8 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN8 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_14_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_14_to_utf8 FOR 'LATIN8' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_14_to_utf8 IS 'conversion for LATIN8 to UTF8';
-- UTF8 --> LATIN9
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to LATIN9';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_15;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_15 FOR 'UTF8' TO 'LATIN9' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_15 IS 'conversion for UTF8 to LATIN9';
-- LATIN9 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN9 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_15_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_15_to_utf8 FOR 'LATIN9' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_15_to_utf8 IS 'conversion for LATIN9 to UTF8';
-- UTF8 --> LATIN10
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to LATIN10';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_16;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_16 FOR 'UTF8' TO 'LATIN10' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_16 IS 'conversion for UTF8 to LATIN10';
-- LATIN10 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN10 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_16_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_16_to_utf8 FOR 'LATIN10' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_16_to_utf8 IS 'conversion for LATIN10 to UTF8';
-- UTF8 --> ISO-8859-5
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to ISO-8859-5';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_5;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_5 FOR 'UTF8' TO 'ISO-8859-5' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_5 IS 'conversion for UTF8 to ISO-8859-5';
-- ISO-8859-5 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for ISO-8859-5 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_5_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_5_to_utf8 FOR 'ISO-8859-5' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_5_to_utf8 IS 'conversion for ISO-8859-5 to UTF8';
-- UTF8 --> ISO-8859-6
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to ISO-8859-6';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_6;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_6 FOR 'UTF8' TO 'ISO-8859-6' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_6 IS 'conversion for UTF8 to ISO-8859-6';
-- ISO-8859-6 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for ISO-8859-6 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_6_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_6_to_utf8 FOR 'ISO-8859-6' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_6_to_utf8 IS 'conversion for ISO-8859-6 to UTF8';
-- UTF8 --> ISO-8859-7
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to ISO-8859-7';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_7;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_7 FOR 'UTF8' TO 'ISO-8859-7' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_7 IS 'conversion for UTF8 to ISO-8859-7';
-- ISO-8859-7 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for ISO-8859-7 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_7_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_7_to_utf8 FOR 'ISO-8859-7' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_7_to_utf8 IS 'conversion for ISO-8859-7 to UTF8';
-- UTF8 --> ISO-8859-8
CREATE OR REPLACE FUNCTION utf8_to_iso8859 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'utf8_to_iso8859' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to ISO-8859-8';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_8;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_8 FOR 'UTF8' TO 'ISO-8859-8' FROM utf8_to_iso8859;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_8 IS 'conversion for UTF8 to ISO-8859-8';
-- ISO-8859-8 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859', 'iso8859_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for ISO-8859-8 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_8_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_8_to_utf8 FOR 'ISO-8859-8' TO 'UTF8' FROM iso8859_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_8_to_utf8 IS 'conversion for ISO-8859-8 to UTF8';
-- LATIN1 --> UTF8
CREATE OR REPLACE FUNCTION iso8859_1_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859_1', 'iso8859_1_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION iso8859_1_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for LATIN1 to UTF8';
DROP CONVERSION pg_catalog.iso_8859_1_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.iso_8859_1_to_utf8 FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
COMMENT ON CONVERSION pg_catalog.iso_8859_1_to_utf8 IS 'conversion for LATIN1 to UTF8';
-- UTF8 --> LATIN1
CREATE OR REPLACE FUNCTION utf8_to_iso8859_1 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_iso8859_1', 'utf8_to_iso8859_1' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_iso8859_1(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to LATIN1';
DROP CONVERSION pg_catalog.utf8_to_iso_8859_1;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_iso_8859_1 FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1;
COMMENT ON CONVERSION pg_catalog.utf8_to_iso_8859_1 IS 'conversion for UTF8 to LATIN1';
-- JOHAB --> UTF8
CREATE OR REPLACE FUNCTION johab_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_johab', 'johab_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION johab_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for JOHAB to UTF8';
DROP CONVERSION pg_catalog.johab_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.johab_to_utf8 FOR 'JOHAB' TO 'UTF8' FROM johab_to_utf8;
COMMENT ON CONVERSION pg_catalog.johab_to_utf8 IS 'conversion for JOHAB to UTF8';
-- UTF8 --> JOHAB
CREATE OR REPLACE FUNCTION utf8_to_johab (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_johab', 'utf8_to_johab' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_johab(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to JOHAB';
DROP CONVERSION pg_catalog.utf8_to_johab;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_johab FOR 'UTF8' TO 'JOHAB' FROM utf8_to_johab;
COMMENT ON CONVERSION pg_catalog.utf8_to_johab IS 'conversion for UTF8 to JOHAB';
-- SJIS --> UTF8
CREATE OR REPLACE FUNCTION sjis_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_sjis', 'sjis_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION sjis_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for SJIS to UTF8';
DROP CONVERSION pg_catalog.sjis_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.sjis_to_utf8 FOR 'SJIS' TO 'UTF8' FROM sjis_to_utf8;
COMMENT ON CONVERSION pg_catalog.sjis_to_utf8 IS 'conversion for SJIS to UTF8';
-- UTF8 --> SJIS
CREATE OR REPLACE FUNCTION utf8_to_sjis (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_sjis', 'utf8_to_sjis' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_sjis(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to SJIS';
DROP CONVERSION pg_catalog.utf8_to_sjis;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_sjis FOR 'UTF8' TO 'SJIS' FROM utf8_to_sjis;
COMMENT ON CONVERSION pg_catalog.utf8_to_sjis IS 'conversion for UTF8 to SJIS';
-- UHC --> UTF8
CREATE OR REPLACE FUNCTION uhc_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_uhc', 'uhc_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION uhc_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UHC to UTF8';
DROP CONVERSION pg_catalog.uhc_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.uhc_to_utf8 FOR 'UHC' TO 'UTF8' FROM uhc_to_utf8;
COMMENT ON CONVERSION pg_catalog.uhc_to_utf8 IS 'conversion for UHC to UTF8';
-- UTF8 --> UHC
CREATE OR REPLACE FUNCTION utf8_to_uhc (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_uhc', 'utf8_to_uhc' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_uhc(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to UHC';
DROP CONVERSION pg_catalog.utf8_to_uhc;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_uhc FOR 'UTF8' TO 'UHC' FROM utf8_to_uhc;
COMMENT ON CONVERSION pg_catalog.utf8_to_uhc IS 'conversion for UTF8 to UHC';
-- EUC_JIS_2004 --> UTF8
CREATE OR REPLACE FUNCTION euc_jis_2004_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_euc2004', 'euc_jis_2004_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_jis_2004_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_JIS_2004 to UTF8';
DROP CONVERSION pg_catalog.euc_jis_2004_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.euc_jis_2004_to_utf8 FOR 'EUC_JIS_2004' TO 'UTF8' FROM euc_jis_2004_to_utf8;
COMMENT ON CONVERSION pg_catalog.euc_jis_2004_to_utf8 IS 'conversion for EUC_JIS_2004 to UTF8';
-- UTF8 --> EUC_JIS_2004
CREATE OR REPLACE FUNCTION utf8_to_euc_jis_2004 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_euc2004', 'utf8_to_euc_jis_2004' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_euc_jis_2004(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to EUC_JIS_2004';
DROP CONVERSION pg_catalog.utf8_to_euc_jis_2004;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_euc_jis_2004 FOR 'UTF8' TO 'EUC_JIS_2004' FROM utf8_to_euc_jis_2004;
COMMENT ON CONVERSION pg_catalog.utf8_to_euc_jis_2004 IS 'conversion for UTF8 to EUC_JIS_2004';
-- SHIFT_JIS_2004 --> UTF8
CREATE OR REPLACE FUNCTION shift_jis_2004_to_utf8 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_sjis2004', 'shift_jis_2004_to_utf8' LANGUAGE C STRICT;
COMMENT ON FUNCTION shift_jis_2004_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for SHIFT_JIS_2004 to UTF8';
DROP CONVERSION pg_catalog.shift_jis_2004_to_utf8;
CREATE DEFAULT CONVERSION pg_catalog.shift_jis_2004_to_utf8 FOR 'SHIFT_JIS_2004' TO 'UTF8' FROM shift_jis_2004_to_utf8;
COMMENT ON CONVERSION pg_catalog.shift_jis_2004_to_utf8 IS 'conversion for SHIFT_JIS_2004 to UTF8';
-- UTF8 --> SHIFT_JIS_2004
CREATE OR REPLACE FUNCTION utf8_to_shift_jis_2004 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/utf8_and_sjis2004', 'utf8_to_shift_jis_2004' LANGUAGE C STRICT;
COMMENT ON FUNCTION utf8_to_shift_jis_2004(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for UTF8 to SHIFT_JIS_2004';
DROP CONVERSION pg_catalog.utf8_to_shift_jis_2004;
CREATE DEFAULT CONVERSION pg_catalog.utf8_to_shift_jis_2004 FOR 'UTF8' TO 'SHIFT_JIS_2004' FROM utf8_to_shift_jis_2004;
COMMENT ON CONVERSION pg_catalog.utf8_to_shift_jis_2004 IS 'conversion for UTF8 to SHIFT_JIS_2004';
-- EUC_JIS_2004 --> SHIFT_JIS_2004
CREATE OR REPLACE FUNCTION euc_jis_2004_to_shift_jis_2004 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc2004_sjis2004', 'euc_jis_2004_to_shift_jis_2004' LANGUAGE C STRICT;
COMMENT ON FUNCTION euc_jis_2004_to_shift_jis_2004(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for EUC_JIS_2004 to SHIFT_JIS_2004';
DROP CONVERSION pg_catalog.euc_jis_2004_to_shift_jis_2004;
CREATE DEFAULT CONVERSION pg_catalog.euc_jis_2004_to_shift_jis_2004 FOR 'EUC_JIS_2004' TO 'SHIFT_JIS_2004' FROM euc_jis_2004_to_shift_jis_2004;
COMMENT ON CONVERSION pg_catalog.euc_jis_2004_to_shift_jis_2004 IS 'conversion for EUC_JIS_2004 to SHIFT_JIS_2004';
-- SHIFT_JIS_2004 --> EUC_JIS_2004
CREATE OR REPLACE FUNCTION shift_jis_2004_to_euc_jis_2004 (INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) RETURNS VOID AS '$libdir/euc2004_sjis2004', 'shift_jis_2004_to_euc_jis_2004' LANGUAGE C STRICT;
COMMENT ON FUNCTION shift_jis_2004_to_euc_jis_2004(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER) IS 'internal conversion function for SHIFT_JIS_2004 to EUC_JIS_2004';
DROP CONVERSION pg_catalog.shift_jis_2004_to_euc_jis_2004;
CREATE DEFAULT CONVERSION pg_catalog.shift_jis_2004_to_euc_jis_2004 FOR 'SHIFT_JIS_2004' TO 'EUC_JIS_2004' FROM shift_jis_2004_to_euc_jis_2004;
COMMENT ON CONVERSION pg_catalog.shift_jis_2004_to_euc_jis_2004 IS 'conversion for SHIFT_JIS_2004 to EUC_JIS_2004';
