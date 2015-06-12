/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_PARSER_PARSER_BISON_H_INCLUDED
# define YY_PARSER_PARSER_BISON_H_INCLUDED
/* Debug traces.  */
#ifndef PARSER_DEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define PARSER_DEBUG 1
#  else
#   define PARSER_DEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define PARSER_DEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined PARSER_DEBUG */
#if PARSER_DEBUG
extern int parser_debug;
#endif
/* "%code requires" blocks.  */
#line 48 "../../src/parser_bison.ypp" /* yacc.c:1915  */

// %code requires block	
#ifndef PARSER_TYPEDEF_
#define PARSER_TYPEDEF_
#include <vector>

#ifndef YYtypeDEF_YY_SCANNER_T
#define YYtypeDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

#define YYSTYPE PARSER_STYPE
#define YYLTYPE PARSER_LTYPE

struct PARSER_CUST_LTYPE {
  int first_line;
  int first_column;
  int last_line;
  int last_column;

  int total_column;

  // Placeholder
  int placeholder_id;
  std::vector<void*> placeholder_list;
};

#define PARSER_LTYPE PARSER_CUST_LTYPE
#define PARSER_LTYPE_IS_DECLARED 1

#endif

// Auto update column and line number
#define YY_USER_ACTION \
    yylloc->first_line = yylloc->last_line; \
    yylloc->first_column = yylloc->last_column; \
    for(int i = 0; yytext[i] != '\0'; i++) { \
    	yylloc->total_column++; \
        if(yytext[i] == '\n') { \
            yylloc->last_line++; \
            yylloc->last_column = 0; \
        } \
        else { \
            yylloc->last_column++; \
        } \
    }

#line 100 "parser_bison.h" /* yacc.c:1915  */

/* Token type.  */
#ifndef PARSER_TOKENTYPE
# define PARSER_TOKENTYPE
  enum parser_tokentype
  {
    SQL_IDENTIFIER = 258,
    SQL_STRING = 259,
    SQL_FLOATVAL = 260,
    SQL_INTVAL = 261,
    SQL_NOTEQUALS = 262,
    SQL_LESSEQ = 263,
    SQL_GREATEREQ = 264,
    SQL_TRANSACTION = 265,
    SQL_REFERENCES = 266,
    SQL_DEALLOCATE = 267,
    SQL_PARAMETERS = 268,
    SQL_INTERSECT = 269,
    SQL_TEMPORARY = 270,
    SQL_TIMESTAMP = 271,
    SQL_VARBINARY = 272,
    SQL_ROLLBACK = 273,
    SQL_DISTINCT = 274,
    SQL_NVARCHAR = 275,
    SQL_RESTRICT = 276,
    SQL_TRUNCATE = 277,
    SQL_ANALYZE = 278,
    SQL_BETWEEN = 279,
    SQL_BOOLEAN = 280,
    SQL_ADDRESS = 281,
    SQL_DATABASE = 282,
    SQL_SMALLINT = 283,
    SQL_VARCHAR = 284,
    SQL_FOREIGN = 285,
    SQL_TINYINT = 286,
    SQL_CASCADE = 287,
    SQL_COLUMNS = 288,
    SQL_CONTROL = 289,
    SQL_DEFAULT = 290,
    SQL_EXECUTE = 291,
    SQL_EXPLAIN = 292,
    SQL_HISTORY = 293,
    SQL_INTEGER = 294,
    SQL_NATURAL = 295,
    SQL_PREPARE = 296,
    SQL_PRIMARY = 297,
    SQL_SCHEMAS = 298,
    SQL_DECIMAL = 299,
    SQL_SPATIAL = 300,
    SQL_VIRTUAL = 301,
    SQL_BEFORE = 302,
    SQL_COLUMN = 303,
    SQL_CREATE = 304,
    SQL_DELETE = 305,
    SQL_DIRECT = 306,
    SQL_BIGINT = 307,
    SQL_DOUBLE = 308,
    SQL_ESCAPE = 309,
    SQL_EXCEPT = 310,
    SQL_EXISTS = 311,
    SQL_GLOBAL = 312,
    SQL_HAVING = 313,
    SQL_INSERT = 314,
    SQL_ISNULL = 315,
    SQL_OFFSET = 316,
    SQL_RENAME = 317,
    SQL_SCHEMA = 318,
    SQL_SELECT = 319,
    SQL_SORTED = 320,
    SQL_COMMIT = 321,
    SQL_TABLES = 322,
    SQL_UNIQUE = 323,
    SQL_UNLOAD = 324,
    SQL_UPDATE = 325,
    SQL_VALUES = 326,
    SQL_AFTER = 327,
    SQL_ALTER = 328,
    SQL_CROSS = 329,
    SQL_FLOAT = 330,
    SQL_BEGIN = 331,
    SQL_DELTA = 332,
    SQL_GROUP = 333,
    SQL_INDEX = 334,
    SQL_INNER = 335,
    SQL_LIMIT = 336,
    SQL_LOCAL = 337,
    SQL_MERGE = 338,
    SQL_MINUS = 339,
    SQL_ORDER = 340,
    SQL_OUTER = 341,
    SQL_RIGHT = 342,
    SQL_TABLE = 343,
    SQL_UNION = 344,
    SQL_USING = 345,
    SQL_WHERE = 346,
    SQL_CHAR = 347,
    SQL_CALL = 348,
    SQL_DATE = 349,
    SQL_DESC = 350,
    SQL_DROP = 351,
    SQL_FILE = 352,
    SQL_FROM = 353,
    SQL_FULL = 354,
    SQL_HASH = 355,
    SQL_HINT = 356,
    SQL_INTO = 357,
    SQL_JOIN = 358,
    SQL_LEFT = 359,
    SQL_LIKE = 360,
    SQL_LOAD = 361,
    SQL_NULL = 362,
    SQL_PART = 363,
    SQL_PLAN = 364,
    SQL_SHOW = 365,
    SQL_TEXT = 366,
    SQL_TIME = 367,
    SQL_VIEW = 368,
    SQL_WITH = 369,
    SQL_ADD = 370,
    SQL_ALL = 371,
    SQL_AND = 372,
    SQL_ASC = 373,
    SQL_CSV = 374,
    SQL_FOR = 375,
    SQL_INT = 376,
    SQL_KEY = 377,
    SQL_NOT = 378,
    SQL_OFF = 379,
    SQL_SET = 380,
    SQL_TOP = 381,
    SQL_AS = 382,
    SQL_BY = 383,
    SQL_IF = 384,
    SQL_IN = 385,
    SQL_IS = 386,
    SQL_OF = 387,
    SQL_ON = 388,
    SQL_OR = 389,
    SQL_TO = 390,
    SQL_EQUALS = 391,
    SQL_LESS = 392,
    SQL_GREATER = 393,
    SQL_NOTNULL = 394,
    SQL_UMINUS = 395
  };
#endif
/* Tokens.  */
#define SQL_IDENTIFIER 258
#define SQL_STRING 259
#define SQL_FLOATVAL 260
#define SQL_INTVAL 261
#define SQL_NOTEQUALS 262
#define SQL_LESSEQ 263
#define SQL_GREATEREQ 264
#define SQL_TRANSACTION 265
#define SQL_REFERENCES 266
#define SQL_DEALLOCATE 267
#define SQL_PARAMETERS 268
#define SQL_INTERSECT 269
#define SQL_TEMPORARY 270
#define SQL_TIMESTAMP 271
#define SQL_VARBINARY 272
#define SQL_ROLLBACK 273
#define SQL_DISTINCT 274
#define SQL_NVARCHAR 275
#define SQL_RESTRICT 276
#define SQL_TRUNCATE 277
#define SQL_ANALYZE 278
#define SQL_BETWEEN 279
#define SQL_BOOLEAN 280
#define SQL_ADDRESS 281
#define SQL_DATABASE 282
#define SQL_SMALLINT 283
#define SQL_VARCHAR 284
#define SQL_FOREIGN 285
#define SQL_TINYINT 286
#define SQL_CASCADE 287
#define SQL_COLUMNS 288
#define SQL_CONTROL 289
#define SQL_DEFAULT 290
#define SQL_EXECUTE 291
#define SQL_EXPLAIN 292
#define SQL_HISTORY 293
#define SQL_INTEGER 294
#define SQL_NATURAL 295
#define SQL_PREPARE 296
#define SQL_PRIMARY 297
#define SQL_SCHEMAS 298
#define SQL_DECIMAL 299
#define SQL_SPATIAL 300
#define SQL_VIRTUAL 301
#define SQL_BEFORE 302
#define SQL_COLUMN 303
#define SQL_CREATE 304
#define SQL_DELETE 305
#define SQL_DIRECT 306
#define SQL_BIGINT 307
#define SQL_DOUBLE 308
#define SQL_ESCAPE 309
#define SQL_EXCEPT 310
#define SQL_EXISTS 311
#define SQL_GLOBAL 312
#define SQL_HAVING 313
#define SQL_INSERT 314
#define SQL_ISNULL 315
#define SQL_OFFSET 316
#define SQL_RENAME 317
#define SQL_SCHEMA 318
#define SQL_SELECT 319
#define SQL_SORTED 320
#define SQL_COMMIT 321
#define SQL_TABLES 322
#define SQL_UNIQUE 323
#define SQL_UNLOAD 324
#define SQL_UPDATE 325
#define SQL_VALUES 326
#define SQL_AFTER 327
#define SQL_ALTER 328
#define SQL_CROSS 329
#define SQL_FLOAT 330
#define SQL_BEGIN 331
#define SQL_DELTA 332
#define SQL_GROUP 333
#define SQL_INDEX 334
#define SQL_INNER 335
#define SQL_LIMIT 336
#define SQL_LOCAL 337
#define SQL_MERGE 338
#define SQL_MINUS 339
#define SQL_ORDER 340
#define SQL_OUTER 341
#define SQL_RIGHT 342
#define SQL_TABLE 343
#define SQL_UNION 344
#define SQL_USING 345
#define SQL_WHERE 346
#define SQL_CHAR 347
#define SQL_CALL 348
#define SQL_DATE 349
#define SQL_DESC 350
#define SQL_DROP 351
#define SQL_FILE 352
#define SQL_FROM 353
#define SQL_FULL 354
#define SQL_HASH 355
#define SQL_HINT 356
#define SQL_INTO 357
#define SQL_JOIN 358
#define SQL_LEFT 359
#define SQL_LIKE 360
#define SQL_LOAD 361
#define SQL_NULL 362
#define SQL_PART 363
#define SQL_PLAN 364
#define SQL_SHOW 365
#define SQL_TEXT 366
#define SQL_TIME 367
#define SQL_VIEW 368
#define SQL_WITH 369
#define SQL_ADD 370
#define SQL_ALL 371
#define SQL_AND 372
#define SQL_ASC 373
#define SQL_CSV 374
#define SQL_FOR 375
#define SQL_INT 376
#define SQL_KEY 377
#define SQL_NOT 378
#define SQL_OFF 379
#define SQL_SET 380
#define SQL_TOP 381
#define SQL_AS 382
#define SQL_BY 383
#define SQL_IF 384
#define SQL_IN 385
#define SQL_IS 386
#define SQL_OF 387
#define SQL_ON 388
#define SQL_OR 389
#define SQL_TO 390
#define SQL_EQUALS 391
#define SQL_LESS 392
#define SQL_GREATER 393
#define SQL_NOTNULL 394
#define SQL_UMINUS 395

/* Value type.  */
#if ! defined PARSER_STYPE && ! defined PARSER_STYPE_IS_DECLARED

union PARSER_STYPE
{
#line 132 "../../src/parser_bison.ypp" /* yacc.c:1915  */

	double fval;
	int64_t ival;
	char* sval;
	uint uval;
	bool bval;

	nstore::parser::SQLStatement* statement;
	nstore::parser::SelectStatement*  	  select_stmt;
	nstore::parser::CreateStatement* 	  create_stmt;
	nstore::parser::InsertStatement* 	  insert_stmt;
	nstore::parser::DeleteStatement* 	  delete_stmt;
	nstore::parser::UpdateStatement* 	  update_stmt;
	nstore::parser::DropStatement*   	  drop_stmt;
	nstore::parser::PrepareStatement*     prep_stmt;
	nstore::parser::ExecuteStatement*     exec_stmt;
	nstore::parser::TransactionStatement* txn_stmt;

	nstore::parser::TableRef* table;
	nstore::expression::AbstractExpression* expr;
	nstore::parser::OrderDescription* order;
	nstore::parser::OrderType order_type;
	nstore::parser::LimitDescription* limit;
	nstore::parser::ColumnDefinition* column_t;
	nstore::parser::GroupByDescription* group_t;
	nstore::parser::UpdateClause* update_t;

	nstore::parser::SQLStatementList* stmt_list;

	std::vector<char*>* str_vec;
	std::vector<nstore::parser::TableRef*>* table_vec;
	std::vector<nstore::parser::ColumnDefinition*>* column_vec;
	std::vector<nstore::parser::UpdateClause*>* update_vec;
	std::vector<nstore::expression::AbstractExpression*>* expr_vec;

#line 428 "parser_bison.h" /* yacc.c:1915  */
};

typedef union PARSER_STYPE PARSER_STYPE;
# define PARSER_STYPE_IS_TRIVIAL 1
# define PARSER_STYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined PARSER_LTYPE && ! defined PARSER_LTYPE_IS_DECLARED
typedef struct PARSER_LTYPE PARSER_LTYPE;
struct PARSER_LTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define PARSER_LTYPE_IS_DECLARED 1
# define PARSER_LTYPE_IS_TRIVIAL 1
#endif



int parser_parse (nstore::parser::SQLStatementList** result, yyscan_t scanner);

#endif /* !YY_PARSER_PARSER_BISON_H_INCLUDED  */
