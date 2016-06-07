/**
 * SQL Scanner
 * sql_scanner.l
 *
 * Based on : https://github.com/hyrise/sql-parser (Feb 2015)
 */


/***************************
 ** Section 1: Definitions
 ***************************/
%{

#include <stdio.h>
#include <stdlib.h>

#include "parser/statements.h"
#include "parser/sql_parser.h"

char* substr(const char* source, int from, int to) {
	int len = to-from;
	char* copy = (char *) malloc(sizeof(char)* (len+1));
	strncpy(copy, source+from, len);
	copy[len] = '\0';
	return copy;
}

#define TOKEN(name) { return SQL_##name; }
#define yylex           parser_lex
#define YY_EXIT_FAILURE ( (void)yyscanner, 2 ) // unused function warning

%}
/***************************
 ** Section 2: Rules
 ***************************/

/* Define the output files */
/* Output location is defined by the build system */
/* %option outfile="parser/sql_scanner.cpp" */
%option header-file="include/parser/sql_scanner.h"

/* Make reentrant */
%option reentrant
%option bison-bridge

/* performance tweeks */
%option never-interactive
%option batch

/* other flags */
%option noyywrap
%option nounput
%option warn
%option case-insensitive
%option bison-locations 
/* %option nodefault */

/* Peloton warning issue */
%option noyyalloc
%option noyyfree
%option noyyrealloc


%s COMMENT

/***************************
 ** Section 3: Rules
 ***************************/
%%

--				BEGIN(COMMENT);
<COMMENT>[^\n]*	/* skipping comment content until a end of line is read */;
<COMMENT>\n 	BEGIN(INITIAL);

[ \t\n]+	/* skip whitespace */;

TRANSACTION TOKEN(TRANSACTION)
DEALLOCATE	TOKEN(DEALLOCATE)
PARAMETERS	TOKEN(PARAMETERS)
REFERENCES  TOKEN(REFERENCES)
INTERSECT	TOKEN(INTERSECT)
VARBINARY   TOKEN(VARBINARY)
TEMPORARY	TOKEN(TEMPORARY)
TIMESTAMP	TOKEN(TIMESTAMP)
DISTINCT	TOKEN(DISTINCT)
NVARCHAR	TOKEN(NVARCHAR)
RESTRICT	TOKEN(RESTRICT)
TRUNCATE	TOKEN(TRUNCATE)
ROLLBACK    TOKEN(ROLLBACK)
SMALLINT    TOKEN(SMALLINT)
DATABASE    TOKEN(DATABASE)
BOOLEAN     TOKEN(BOOLEAN)
VARCHAR     TOKEN(VARCHAR)
FOREIGN     TOKEN(FOREIGN)
TINYINT     TOKEN(TINYINT)
ANALYZE		TOKEN(ANALYZE)
BETWEEN		TOKEN(BETWEEN)
CASCADE		TOKEN(CASCADE)
COLUMNS		TOKEN(COLUMNS)
CONTROL		TOKEN(CONTROL)
DEFAULT		TOKEN(DEFAULT)
EXECUTE		TOKEN(EXECUTE)
EXPLAIN		TOKEN(EXPLAIN)
HISTORY		TOKEN(HISTORY)
INTEGER		TOKEN(INTEGER)
NATURAL		TOKEN(NATURAL)
PREPARE		TOKEN(PREPARE)
PRIMARY		TOKEN(PRIMARY)
SCHEMAS		TOKEN(SCHEMAS)
SPATIAL		TOKEN(SPATIAL)
VIRTUAL		TOKEN(VIRTUAL)
DECIMAL     TOKEN(DECIMAL)
BEFORE		TOKEN(BEFORE)
COLUMN		TOKEN(COLUMN)
CREATE		TOKEN(CREATE)
DELETE		TOKEN(DELETE)
DIRECT		TOKEN(DIRECT)
DOUBLE		TOKEN(DOUBLE)
ESCAPE		TOKEN(ESCAPE)
EXCEPT		TOKEN(EXCEPT)
EXISTS		TOKEN(EXISTS)
GLOBAL		TOKEN(GLOBAL)
HAVING		TOKEN(HAVING)
BIGINT      TOKEN(BIGINT)
INSERT		TOKEN(INSERT)
ISNULL		TOKEN(ISNULL)
OFFSET		TOKEN(OFFSET)
RENAME		TOKEN(RENAME)
SCHEMA		TOKEN(SCHEMA)
SELECT		TOKEN(SELECT)
SORTED		TOKEN(SORTED)
TABLES		TOKEN(TABLES)
UNIQUE		TOKEN(UNIQUE)
UNLOAD		TOKEN(UNLOAD)
UPDATE		TOKEN(UPDATE)
VALUES		TOKEN(VALUES)
COMMIT      TOKEN(COMMIT)
AFTER		TOKEN(AFTER)
ALTER		TOKEN(ALTER)
CROSS		TOKEN(CROSS)
DELTA		TOKEN(DELTA)
GROUP		TOKEN(GROUP)
INDEX		TOKEN(INDEX)
INNER		TOKEN(INNER)
LIMIT		TOKEN(LIMIT)
LOCAL		TOKEN(LOCAL)
MERGE		TOKEN(MERGE)
MINUS		TOKEN(MINUS)
ORDER		TOKEN(ORDER)
OUTER		TOKEN(OUTER)
RIGHT		TOKEN(RIGHT)
TABLE		TOKEN(TABLE)
UNION		TOKEN(UNION)
USING		TOKEN(USING)
WHERE		TOKEN(WHERE)
BEGIN       TOKEN(BEGIN)
FLOAT       TOKEN(FLOAT)
CHAR        TOKEN(CHAR)
CALL		TOKEN(CALL)
DATE		TOKEN(DATE)
DESC		TOKEN(DESC)
DROP		TOKEN(DROP)
FILE		TOKEN(FILE)
FROM		TOKEN(FROM)
FULL		TOKEN(FULL)
HASH		TOKEN(HASH)
HINT		TOKEN(HINT)
INTO		TOKEN(INTO)
JOIN		TOKEN(JOIN)
LEFT		TOKEN(LEFT)
LIKE		TOKEN(LIKE)
LOAD		TOKEN(LOAD)
NULL		TOKEN(NULL)
PART		TOKEN(PART)
PLAN		TOKEN(PLAN)
SHOW		TOKEN(SHOW)
TEXT		TOKEN(TEXT)
TIME		TOKEN(TIME)
VIEW		TOKEN(VIEW)
WITH		TOKEN(WITH)
ADD			TOKEN(ADD)
ALL			TOKEN(ALL)
AND			TOKEN(AND)
ASC			TOKEN(ASC)
CSV			TOKEN(CSV)
FOR			TOKEN(FOR)
INT			TOKEN(INT)
KEY			TOKEN(KEY)
NOT			TOKEN(NOT)
OFF			TOKEN(OFF)
SET			TOKEN(SET)
TOP			TOKEN(TOP)
AS			TOKEN(AS)
BY			TOKEN(BY)
IF			TOKEN(IF)
IN			TOKEN(IN)
IS			TOKEN(IS)
OF			TOKEN(OF)
ON			TOKEN(ON)
OR			TOKEN(OR)
TO			TOKEN(TO)


"<>" 		TOKEN(NOTEQUALS)
"<="		TOKEN(LESSEQ)
">="		TOKEN(GREATEREQ)


[-+*/(){},.;<>=^%:?]	{ return yytext[0]; }


[0-9]+"."[0-9]* |
"."[0-9]*	{
	yylval->fval = atof(yytext);
	return SQL_FLOATVAL;
}

[0-9]+	{
	yylval->ival = atol(yytext);
	return SQL_INTVAL;
}

\"[^\"\n]+\" {
	// Crop the leading and trailing quote char
	yylval->sval = substr(yytext, 1, strlen(yytext)-1);
	return SQL_IDENTIFIER;
}

[A-Za-z][A-Za-z0-9_]* {
	// To lower case
	for (int i = 0; yytext[i]; i++)
	    yytext[i] = tolower(yytext[i]);
	yylval->sval = strdup(yytext);
	return SQL_IDENTIFIER;
}


'[^'\n]*' {
	// Crop the leading and trailing quote char
	yylval->sval = substr(yytext, 1, strlen(yytext)-1);
	return SQL_STRING;
}

. { fprintf(stderr, "[SQL-Scanner-Error] Unknown Character: %c\n", yytext[0]); return 0; }


%%
/***************************
 ** Section 3: User code
 ***************************/

int yyerror(const char *msg) {
    fprintf(stderr, "[SQL-Scanner-Error] %s\n",msg); return 0;
}

/* redefine these functions to get rid of unused variable warnings */
void yyfree (void * ptr, yyscan_t yyscanner) {
    (void)yyscanner;
    free(ptr);
}

void *yyalloc (yy_size_t size, yyscan_t yyscanner) {
    (void)yyscanner;
    return malloc(size);
}

void *yyrealloc (void * ptr, yy_size_t size, yyscan_t yyscanner) {
    (void)yyscanner;
    return realloc(ptr, size );
}
