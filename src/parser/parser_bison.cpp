/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison implementation for Yacc-like parsers in C

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

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "3.0.4"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Substitute the type names.  */
#define YYSTYPE         PARSER_STYPE
#define YYLTYPE         PARSER_LTYPE
/* Substitute the variable and function names.  */
#define yyparse         parser_parse
#define yylex           parser_lex
#define yyerror         parser_error
#define yydebug         parser_debug
#define yynerrs         parser_nerrs


/* Copy the first part of user declarations.  */
#line 1 "../../src/parser_bison.ypp" /* yacc.c:339  */

/**
 * parser_bison.y
 * defines parser_bison.h
 * outputs parser_bison.c
 * 
 * Grammar File Spec: http://dinosaur.compilertools.net/bison/bison_6.html
 * Based on : https://github.com/hyrise/sql-parser (Feb 2015)
 *
 */
/*********************************
 ** Section 1: C Declarations
 *********************************/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "parser/statements.h"
#include "parser_bison.h"
#include "common/value_factory.h"

using namespace std;
using namespace nstore::parser;

extern int parser_lex (YYSTYPE *lvalp, YYLTYPE *llocp, yyscan_t scanner);

int yyerror(YYLTYPE* llocp, nstore::parser::SQLStatementList** result, __attribute__((unused)) yyscan_t scanner, const char *msg) {

	nstore::parser::SQLStatementList* list = new nstore::parser::SQLStatementList();
	list->is_valid = false;
	list->parser_msg = strdup(msg);
	list->error_line = llocp->first_line;
	list->error_col = llocp->first_column;

	*result = list;
	return 0;
}



#line 116 "parser_bison.cpp" /* yacc.c:339  */

# ifndef YY_NULLPTR
#  if defined __cplusplus && 201103L <= __cplusplus
#   define YY_NULLPTR nullptr
#  else
#   define YY_NULLPTR 0
#  endif
# endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 1
#endif

/* In a future release of Bison, this section will be replaced
   by #include "parser_bison.h".  */
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
#line 48 "../../src/parser_bison.ypp" /* yacc.c:355  */

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

#line 202 "parser_bison.cpp" /* yacc.c:355  */

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
#line 132 "../../src/parser_bison.ypp" /* yacc.c:355  */

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

#line 530 "parser_bison.cpp" /* yacc.c:355  */
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

/* Copy the second part of user declarations.  */

#line 560 "parser_bison.cpp" /* yacc.c:358  */

#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif

#ifndef YY_ATTRIBUTE
# if (defined __GNUC__                                               \
      && (2 < __GNUC__ || (__GNUC__ == 2 && 96 <= __GNUC_MINOR__)))  \
     || defined __SUNPRO_C && 0x5110 <= __SUNPRO_C
#  define YY_ATTRIBUTE(Spec) __attribute__(Spec)
# else
#  define YY_ATTRIBUTE(Spec) /* empty */
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# define YY_ATTRIBUTE_PURE   YY_ATTRIBUTE ((__pure__))
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# define YY_ATTRIBUTE_UNUSED YY_ATTRIBUTE ((__unused__))
#endif

#if !defined _Noreturn \
     && (!defined __STDC_VERSION__ || __STDC_VERSION__ < 201112)
# if defined _MSC_VER && 1200 <= _MSC_VER
#  define _Noreturn __declspec (noreturn)
# else
#  define _Noreturn YY_ATTRIBUTE ((__noreturn__))
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN \
    _Pragma ("GCC diagnostic push") \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")\
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif


#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined PARSER_LTYPE_IS_TRIVIAL && PARSER_LTYPE_IS_TRIVIAL \
             && defined PARSER_STYPE_IS_TRIVIAL && PARSER_STYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE) + sizeof (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYSIZE_T yynewbytes;                                            \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / sizeof (*yyptr);                          \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, (Count) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYSIZE_T yyi;                         \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  56
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   424

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  161
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  70
/* YYNRULES -- Number of rules.  */
#define YYNRULES  168
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  301

/* YYTRANSLATE[YYX] -- Symbol number corresponding to YYX as returned
   by yylex, with out-of-bounds checking.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   395

#define YYTRANSLATE(YYX)                                                \
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, without out-of-bounds checking.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,   147,     2,     2,
     152,   153,   145,   143,   159,   144,   154,   146,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,   156,   155,
     138,   136,   139,   160,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,   150,     2,   151,   148,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   157,     2,   158,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,   133,   134,
     135,   137,   140,   141,   142,   149
};

#if PARSER_DEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   261,   261,   268,   269,   273,   278,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   299,   304,   312,   316,
     331,   337,   342,   352,   353,   357,   358,   362,   370,   375,
     384,   385,   389,   390,   394,   395,   399,   400,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   429,   434,   439,   445,   459,   462,   465,   470,
     471,   480,   488,   500,   506,   516,   517,   527,   536,   537,
     541,   553,   554,   558,   559,   563,   568,   580,   581,   582,
     586,   597,   598,   602,   607,   612,   613,   617,   622,   626,
     627,   630,   631,   635,   636,   637,   642,   643,   644,   651,
     652,   656,   657,   661,   668,   669,   670,   671,   672,   676,
     677,   678,   682,   683,   687,   688,   689,   690,   691,   692,
     693,   694,   695,   700,   701,   702,   703,   704,   705,   709,
     713,   714,   718,   719,   720,   724,   729,   730,   734,   738,
     743,   754,   755,   765,   766,   772,   776,   777,   782,   792,
     800,   801,   806,   807,   811,   812,   820,   832,   833,   834,
     835,   836,   842,   848,   852,   861,   862,   867,   868
};
#endif

#if PARSER_DEBUG || YYERROR_VERBOSE || 1
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "IDENTIFIER", "STRING", "FLOATVAL",
  "INTVAL", "NOTEQUALS", "LESSEQ", "GREATEREQ", "TRANSACTION",
  "REFERENCES", "DEALLOCATE", "PARAMETERS", "INTERSECT", "TEMPORARY",
  "TIMESTAMP", "VARBINARY", "ROLLBACK", "DISTINCT", "NVARCHAR", "RESTRICT",
  "TRUNCATE", "ANALYZE", "BETWEEN", "BOOLEAN", "ADDRESS", "DATABASE",
  "SMALLINT", "VARCHAR", "FOREIGN", "TINYINT", "CASCADE", "COLUMNS",
  "CONTROL", "DEFAULT", "EXECUTE", "EXPLAIN", "HISTORY", "INTEGER",
  "NATURAL", "PREPARE", "PRIMARY", "SCHEMAS", "DECIMAL", "SPATIAL",
  "VIRTUAL", "BEFORE", "COLUMN", "CREATE", "DELETE", "DIRECT", "BIGINT",
  "DOUBLE", "ESCAPE", "EXCEPT", "EXISTS", "GLOBAL", "HAVING", "INSERT",
  "ISNULL", "OFFSET", "RENAME", "SCHEMA", "SELECT", "SORTED", "COMMIT",
  "TABLES", "UNIQUE", "UNLOAD", "UPDATE", "VALUES", "AFTER", "ALTER",
  "CROSS", "FLOAT", "BEGIN", "DELTA", "GROUP", "INDEX", "INNER", "LIMIT",
  "LOCAL", "MERGE", "MINUS", "ORDER", "OUTER", "RIGHT", "TABLE", "UNION",
  "USING", "WHERE", "CHAR", "CALL", "DATE", "DESC", "DROP", "FILE", "FROM",
  "FULL", "HASH", "HINT", "INTO", "JOIN", "LEFT", "LIKE", "LOAD", "NULL",
  "PART", "PLAN", "SHOW", "TEXT", "TIME", "VIEW", "WITH", "ADD", "ALL",
  "AND", "ASC", "CSV", "FOR", "INT", "KEY", "NOT", "OFF", "SET", "TOP",
  "AS", "BY", "IF", "IN", "IS", "OF", "ON", "OR", "TO", "'='", "EQUALS",
  "'<'", "'>'", "LESS", "GREATER", "NOTNULL", "'+'", "'-'", "'*'", "'/'",
  "'%'", "'^'", "UMINUS", "'['", "']'", "'('", "')'", "'.'", "';'", "':'",
  "'{'", "'}'", "','", "'?'", "$accept", "input", "statement_list",
  "statement", "preparable_statement", "prepare_statement",
  "execute_statement", "create_statement", "opt_not_exists",
  "column_def_commalist", "column_def", "opt_column_width", "opt_notnull",
  "opt_primary", "opt_unique", "column_type", "drop_statement",
  "transaction_statement", "opt_transaction", "delete_statement",
  "truncate_statement", "insert_statement", "opt_column_list",
  "update_statement", "update_clause_commalist", "update_clause",
  "select_statement", "select_with_paren", "select_no_paren",
  "set_operator", "select_clause", "opt_distinct", "select_list",
  "from_clause", "opt_where", "opt_group", "opt_having", "opt_order",
  "opt_order_type", "opt_limit", "expr_list", "literal_list", "expr_alias",
  "expr", "scalar_expr", "unary_expr", "binary_expr", "comp_expr",
  "function_expr", "column_name", "literal", "string_literal",
  "num_literal", "int_literal", "star_expr", "placeholder_expr",
  "table_ref", "table_ref_atomic", "table_ref_commalist", "table_ref_name",
  "table_ref_name_no_alias", "table_name", "alias", "opt_alias",
  "join_clause", "opt_join_type", "join_table", "join_condition",
  "opt_semicolon", "ident_commalist", YY_NULLPTR
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,   316,   317,   318,   319,   320,   321,   322,   323,   324,
     325,   326,   327,   328,   329,   330,   331,   332,   333,   334,
     335,   336,   337,   338,   339,   340,   341,   342,   343,   344,
     345,   346,   347,   348,   349,   350,   351,   352,   353,   354,
     355,   356,   357,   358,   359,   360,   361,   362,   363,   364,
     365,   366,   367,   368,   369,   370,   371,   372,   373,   374,
     375,   376,   377,   378,   379,   380,   381,   382,   383,   384,
     385,   386,   387,   388,   389,   390,    61,   391,    60,    62,
     392,   393,   394,    43,    45,    42,    47,    37,    94,   395,
      91,    93,    40,    41,    46,    59,    58,   123,   125,    44,
      63
};
# endif

#define YYPACT_NINF -227

#define yypact_value_is_default(Yystate) \
  (!!((Yystate) == (-227)))

#define YYTABLE_NINF -164

#define yytable_value_is_error(Yytable_value) \
  (!!((Yytable_value) == (-164)))

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     208,   -15,    24,    28,    46,    50,     2,   -17,     0,    63,
      24,    28,    24,     5,   -37,   126,   -18,  -227,  -227,  -227,
    -227,  -227,  -227,  -227,  -227,  -227,  -227,  -227,  -227,  -227,
    -227,     7,   152,  -227,  -227,   -14,  -227,    -5,  -134,    33,
    -227,    33,    89,    28,    28,  -227,    -2,  -227,    44,  -227,
    -227,   168,   170,    28,    23,    25,  -227,   208,  -227,  -227,
    -227,    49,  -227,   120,   106,  -227,   185,     1,   241,   208,
      71,   196,    28,   197,   111,    51,   -30,  -227,  -227,  -227,
      -2,    -2,  -227,    -2,  -227,   107,    54,  -227,   102,  -227,
    -227,  -227,  -227,  -227,  -227,  -227,  -227,  -227,  -227,  -227,
    -227,   201,  -227,    73,  -227,  -227,  -227,  -227,    -2,   123,
     203,  -227,  -227,  -105,  -227,  -227,   -18,   159,  -227,    66,
      94,    -2,  -227,   225,   -29,    63,   234,   243,    21,    36,
      12,   111,    -2,  -227,    -2,    -2,    -2,    -2,    -2,   134,
     240,    -2,    -2,    -2,    -2,    -2,    -2,    -2,    -2,  -227,
    -227,   125,   -71,  -227,    28,   137,   106,   195,  -227,     1,
     104,  -227,    88,    28,   189,  -227,   -64,   112,  -227,    -2,
    -227,  -227,   -37,  -227,   109,    45,     9,  -227,    48,   187,
    -227,   278,    47,    47,   278,   243,    -2,  -227,   226,   278,
      47,    47,    21,    21,  -227,  -227,     1,   201,  -227,  -227,
    -227,  -227,  -227,  -227,   203,  -227,  -227,     8,   147,   148,
     -56,  -227,   114,  -227,   276,     1,    78,   135,    12,  -227,
    -227,  -227,  -227,  -227,   186,   165,  -227,   278,  -227,  -227,
    -227,  -227,  -227,  -227,  -227,  -227,  -227,  -227,  -227,  -227,
    -227,  -227,  -227,  -227,  -227,   145,   146,   151,  -227,    88,
     225,  -227,   -55,  -227,     9,  -227,   140,    13,    -2,   203,
     173,   225,   225,  -227,   -47,  -227,   215,    12,   -37,  -227,
     175,   -39,   156,   206,   268,   -46,   -36,  -227,  -227,   161,
      -2,    -2,  -227,  -227,  -227,   193,   248,   309,  -227,     9,
     189,  -227,   189,  -227,  -227,    28,  -227,   169,   225,   -20,
    -227
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,    59,     0,     0,     0,    37,     0,     0,    82,
      59,     0,    59,     0,     0,     0,   166,     3,     6,     5,
      14,     8,    13,    15,    10,    11,     9,    12,     7,    71,
      72,    92,     0,    60,    58,   150,    62,    18,     0,    24,
      36,    24,     0,     0,     0,    81,     0,    57,     0,   149,
      56,     0,     0,     0,     0,     0,     1,   165,     2,    78,
      79,     0,    77,     0,    98,    55,     0,     0,     0,     0,
       0,     0,     0,     0,    86,    66,   130,   135,   136,   138,
       0,     0,   139,     0,   140,     0,    83,    99,   155,   105,
     106,   107,   114,   108,   109,   111,   132,   133,   137,   110,
     134,     0,    53,     0,    52,    74,    73,     4,     0,    92,
       0,    75,   151,     0,   101,    16,   166,     0,    21,     0,
       0,     0,    61,     0,     0,    82,     0,   113,   112,     0,
       0,    86,     0,   153,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   154,
     103,     0,    86,    68,     0,    95,    98,    96,    19,     0,
       0,    23,     0,     0,    85,   167,     0,     0,    64,     0,
     131,   104,     0,    84,   141,   143,   155,   145,   161,    88,
     100,   124,   127,   128,   121,   119,     0,   152,   120,   123,
     125,   126,   116,   115,   118,   117,     0,     0,    67,    54,
      94,    93,    91,    76,     0,   102,    17,     0,     0,     0,
       0,    25,     0,    65,     0,     0,     0,     0,     0,   148,
     157,   158,   160,   159,     0,     0,    80,   122,    70,    69,
      97,    49,    51,    48,    44,    50,    43,    39,    47,    45,
      40,    46,    42,    41,    38,    31,     0,     0,    20,     0,
       0,   168,     0,   129,     0,   146,   142,     0,     0,     0,
      33,     0,     0,    26,     0,    63,   144,     0,     0,   163,
       0,    90,     0,     0,    35,     0,     0,    22,   147,     0,
       0,     0,    87,    30,    32,     0,    37,     0,    28,     0,
     164,   156,    89,    34,    27,     0,   162,     0,     0,     0,
      29
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -227,  -227,   253,   267,   258,  -227,  -227,  -227,   288,  -227,
      81,  -227,  -227,  -227,    52,  -227,  -227,  -227,   144,  -227,
    -227,  -227,  -227,  -227,  -227,   139,  -154,   325,     3,  -227,
     277,   216,  -227,  -227,   -72,  -227,  -227,   233,  -227,   188,
      87,   131,   218,   -70,  -227,  -227,  -227,  -227,  -227,  -227,
     -58,  -227,  -227,   -96,  -227,  -227,  -227,  -188,  -227,    90,
    -227,    -3,  -226,   176,  -227,  -227,    96,  -227,   235,  -204
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    15,    16,    17,    18,    19,    20,    21,    71,   210,
     211,   260,   274,   286,    42,   245,    22,    23,    34,    24,
      25,    26,   124,    27,   152,   153,    28,    29,    30,    63,
      31,    46,    85,   131,   122,   226,   282,    64,   202,   111,
      86,   113,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   173,   174,   256,   175,
      48,   176,   149,   150,   177,   224,   178,   291,    58,   166
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      36,    76,    77,    78,    79,    77,    78,    79,    49,   114,
     127,   128,   133,   129,   157,    35,    35,    55,   217,   281,
     121,    59,    68,    69,   231,   232,    32,     9,   266,    39,
     255,    35,    51,   233,    33,     9,   234,   235,   155,   236,
      74,    75,   167,   134,   135,   136,   264,   237,   158,    37,
     104,   164,   238,    38,   159,  -164,  -164,   275,   276,   179,
     239,   240,    60,   296,   181,   182,   183,   184,   185,   119,
      40,   188,   189,   190,   191,   192,   193,   194,   195,   278,
     198,    43,    45,   241,    52,   134,   135,   136,   197,   213,
      41,   207,    61,    53,   299,   214,    62,   248,   265,   216,
     242,   205,    44,   249,   159,   133,   277,   287,   230,   134,
     135,   136,   214,   214,   279,    14,   227,   288,   208,   243,
     132,    80,   125,   214,   126,  -163,    56,   168,   220,   244,
     209,  -163,  -163,   300,   221,   222,   140,    57,   228,   214,
      66,   137,    81,    82,   134,   135,   136,    67,  -163,  -163,
      83,   199,   223,   138,    47,    65,    50,   114,    84,   139,
     212,    84,    70,   272,   172,   268,   147,   148,    73,   101,
     141,   102,   142,   103,   143,   144,   105,   108,   106,   145,
     146,   147,   148,   137,     9,  -164,  -164,   110,   112,   171,
     145,   146,   147,   148,   117,   138,   134,   135,   136,   118,
     120,   139,   121,   123,   151,   130,   154,   137,    61,    79,
     290,   292,   141,   132,   142,   161,   143,   144,   162,   138,
       1,   145,   146,   147,   148,   139,     2,   163,   165,   140,
       3,   253,   200,   134,   135,   136,   141,   170,   142,   186,
     143,   144,   137,   187,     4,   145,   146,   147,   148,     5,
     134,   135,   136,     1,   138,   201,   204,     6,     7,     2,
     139,   196,   206,     3,   215,   225,   250,     8,   218,   246,
     247,   141,     9,   142,    10,   143,   144,     4,    11,   251,
     145,   146,   147,   148,    12,   134,   135,   136,   254,   257,
       6,     7,   297,   258,   137,  -162,   273,   259,   261,   267,
       8,  -162,  -162,   262,    13,     9,   138,    10,   280,   283,
     285,    11,   139,   284,   289,   293,    40,    12,  -162,  -162,
     295,   298,   116,   141,   107,   142,   115,   143,   144,    72,
     263,   137,   145,   146,   147,   148,   229,    13,   294,    54,
     109,   169,   156,   138,   203,   271,   252,   269,   137,   139,
     180,   160,   219,   270,     0,     0,     0,     0,     0,     0,
      14,     0,   142,     0,   143,   144,   139,     0,     0,   145,
     146,   147,   148,     0,     0,     0,     0,     0,     0,   142,
       0,   143,   144,   137,     0,     0,   145,   146,   147,   148,
       0,     0,     0,    14,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   142,     0,   143,   144,     0,     0,
       0,   145,   146,   147,   148
};

static const yytype_int16 yycheck[] =
{
       3,     3,     4,     5,     6,     4,     5,     6,    11,    67,
      80,    81,     3,    83,   110,     3,     3,    14,   172,    58,
      91,    14,   156,   157,    16,    17,    41,    64,   254,    27,
     218,     3,    27,    25,    10,    64,    28,    29,   108,    31,
      43,    44,    71,     7,     8,     9,   250,    39,   153,     3,
      53,   121,    44,     3,   159,     8,     9,   261,   262,   131,
      52,    53,    55,   289,   134,   135,   136,   137,   138,    72,
      68,   141,   142,   143,   144,   145,   146,   147,   148,   267,
     152,    98,    19,    75,    79,     7,     8,     9,   159,   153,
      88,     3,    85,    88,   298,   159,    89,   153,   153,   169,
      92,   159,   102,   159,   159,     3,   153,   153,   204,     7,
       8,     9,   159,   159,   268,   152,   186,   153,    30,   111,
     159,   123,   152,   159,   154,    80,     0,   124,    80,   121,
      42,    86,    87,   153,    86,    87,   127,   155,   196,   159,
     154,   105,   144,   145,     7,     8,     9,   152,   103,   104,
     152,   154,   104,   117,    10,     3,    12,   215,   160,   123,
     163,   160,   129,   259,   152,   152,   145,   146,    79,   125,
     134,     3,   136,     3,   138,   139,   153,   128,   153,   143,
     144,   145,   146,   105,    64,   138,   139,    81,     3,   153,
     143,   144,   145,   146,   123,   117,     7,     8,     9,     3,
       3,   123,    91,   152,     3,    98,   133,   105,    85,     6,
     280,   281,   134,   159,   136,    56,   138,   139,   152,   117,
      12,   143,   144,   145,   146,   123,    18,   133,     3,   127,
      22,   153,    95,     7,     8,     9,   134,     3,   136,   105,
     138,   139,   105,     3,    36,   143,   144,   145,   146,    41,
       7,     8,     9,    12,   117,   118,    61,    49,    50,    18,
     123,   136,   158,    22,   152,    78,   152,    59,   159,   122,
     122,   134,    64,   136,    66,   138,   139,    36,    70,     3,
     143,   144,   145,   146,    76,     7,     8,     9,   153,   103,
      49,    50,   295,   128,   105,    80,   123,   152,   152,   159,
      59,    86,    87,   152,    96,    64,   117,    66,   133,   153,
      42,    70,   123,   107,   153,   122,    68,    76,   103,   104,
      11,   152,    69,   134,    57,   136,    68,   138,   139,    41,
     249,   105,   143,   144,   145,   146,   197,    96,   286,    14,
      63,   125,   109,   117,   156,   258,   215,   257,   105,   123,
     132,   116,   176,   257,    -1,    -1,    -1,    -1,    -1,    -1,
     152,    -1,   136,    -1,   138,   139,   123,    -1,    -1,   143,
     144,   145,   146,    -1,    -1,    -1,    -1,    -1,    -1,   136,
      -1,   138,   139,   105,    -1,    -1,   143,   144,   145,   146,
      -1,    -1,    -1,   152,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   136,    -1,   138,   139,    -1,    -1,
      -1,   143,   144,   145,   146
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    12,    18,    22,    36,    41,    49,    50,    59,    64,
      66,    70,    76,    96,   152,   162,   163,   164,   165,   166,
     167,   168,   177,   178,   180,   181,   182,   184,   187,   188,
     189,   191,    41,    10,   179,     3,   222,     3,     3,    27,
      68,    88,   175,    98,   102,    19,   192,   179,   221,   222,
     179,    27,    79,    88,   188,   189,     0,   155,   229,    14,
      55,    85,    89,   190,   198,     3,   154,   152,   156,   157,
     129,   169,   169,    79,   222,   222,     3,     4,     5,     6,
     123,   144,   145,   152,   160,   193,   201,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   125,     3,     3,   222,   153,   153,   164,   128,   191,
      81,   200,     3,   202,   211,   165,   163,   123,     3,   222,
       3,    91,   195,   152,   183,   152,   154,   204,   204,   204,
      98,   194,   159,     3,     7,     8,     9,   105,   117,   123,
     127,   134,   136,   138,   139,   143,   144,   145,   146,   223,
     224,     3,   185,   186,   133,   204,   198,   214,   153,   159,
     229,    56,   152,   133,   204,     3,   230,    71,   189,   192,
       3,   153,   152,   217,   218,   220,   222,   225,   227,   195,
     203,   204,   204,   204,   204,   204,   105,     3,   204,   204,
     204,   204,   204,   204,   204,   204,   136,   159,   195,   222,
      95,   118,   199,   200,    61,   211,   158,     3,    30,    42,
     170,   171,   222,   153,   159,   152,   204,   187,   159,   224,
      80,    86,    87,   104,   226,    78,   196,   204,   211,   186,
     214,    16,    17,    25,    28,    29,    31,    39,    44,    52,
      53,    75,    92,   111,   121,   176,   122,   122,   153,   159,
     152,     3,   202,   153,   153,   218,   219,   103,   128,   152,
     172,   152,   152,   171,   230,   153,   223,   159,   152,   220,
     227,   201,   214,   123,   173,   230,   230,   153,   218,   187,
     133,    58,   197,   153,   107,    42,   174,   153,   153,   153,
     204,   228,   204,   122,   175,    11,   223,   222,   152,   230,
     153
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,   161,   162,   163,   163,   164,   164,   165,   165,   165,
     165,   165,   165,   165,   165,   165,   166,   166,   167,   167,
     168,   168,   168,   169,   169,   170,   170,   171,   171,   171,
     172,   172,   173,   173,   174,   174,   175,   175,   176,   176,
     176,   176,   176,   176,   176,   176,   176,   176,   176,   176,
     176,   176,   177,   177,   177,   177,   178,   178,   178,   179,
     179,   180,   181,   182,   182,   183,   183,   184,   185,   185,
     186,   187,   187,   188,   188,   189,   189,   190,   190,   190,
     191,   192,   192,   193,   194,   195,   195,   196,   196,   197,
     197,   198,   198,   199,   199,   199,   200,   200,   200,   201,
     201,   202,   202,   203,   204,   204,   204,   204,   204,   205,
     205,   205,   206,   206,   207,   207,   207,   207,   207,   207,
     207,   207,   207,   208,   208,   208,   208,   208,   208,   209,
     210,   210,   211,   211,   211,   212,   213,   213,   214,   215,
     216,   217,   217,   218,   218,   218,   219,   219,   220,   221,
     222,   222,   223,   223,   224,   224,   225,   226,   226,   226,
     226,   226,   227,   227,   228,   229,   229,   230,   230
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     1,     3,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     4,     6,     2,     5,
       7,     4,     9,     3,     0,     1,     3,     6,     5,    10,
       3,     0,     2,     0,     2,     0,     1,     0,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     3,     3,     5,     3,     2,     2,     2,     0,
       1,     4,     2,     8,     5,     3,     0,     5,     1,     3,
       3,     1,     1,     3,     3,     3,     5,     1,     1,     1,
       6,     1,     0,     1,     2,     2,     0,     4,     0,     2,
       0,     4,     0,     1,     1,     0,     2,     4,     0,     1,
       3,     1,     3,     2,     3,     1,     1,     1,     1,     1,
       1,     1,     2,     2,     1,     3,     3,     3,     3,     3,
       3,     3,     4,     3,     3,     3,     3,     3,     3,     5,
       1,     3,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     3,     1,     4,     1,     1,     3,     2,     1,
       1,     3,     2,     1,     1,     0,     6,     1,     1,     1,
       1,     0,     4,     1,     1,     1,     0,     1,     3
};


#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)
#define YYEMPTY         (-2)
#define YYEOF           0

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                  \
do                                                              \
  if (yychar == YYEMPTY)                                        \
    {                                                           \
      yychar = (Token);                                         \
      yylval = (Value);                                         \
      YYPOPSTACK (yylen);                                       \
      yystate = *yyssp;                                         \
      goto yybackup;                                            \
    }                                                           \
  else                                                          \
    {                                                           \
      yyerror (&yylloc, result, scanner, YY_("syntax error: cannot back up")); \
      YYERROR;                                                  \
    }                                                           \
while (0)

/* Error token number */
#define YYTERROR        1
#define YYERRCODE       256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)                                \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;        \
          (Current).first_column = YYRHSLOC (Rhs, 1).first_column;      \
          (Current).last_line    = YYRHSLOC (Rhs, N).last_line;         \
          (Current).last_column  = YYRHSLOC (Rhs, N).last_column;       \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).first_line   = (Current).last_line   =              \
            YYRHSLOC (Rhs, 0).last_line;                                \
          (Current).first_column = (Current).last_column =              \
            YYRHSLOC (Rhs, 0).last_column;                              \
        }                                                               \
    while (0)
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K])


/* Enable debugging if requested.  */
#if PARSER_DEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if defined PARSER_LTYPE_IS_TRIVIAL && PARSER_LTYPE_IS_TRIVIAL

/* Print *YYLOCP on YYO.  Private, do not rely on its existence. */

YY_ATTRIBUTE_UNUSED
static unsigned
yy_location_print_ (FILE *yyo, YYLTYPE const * const yylocp)
{
  unsigned res = 0;
  int end_col = 0 != yylocp->last_column ? yylocp->last_column - 1 : 0;
  if (0 <= yylocp->first_line)
    {
      res += YYFPRINTF (yyo, "%d", yylocp->first_line);
      if (0 <= yylocp->first_column)
        res += YYFPRINTF (yyo, ".%d", yylocp->first_column);
    }
  if (0 <= yylocp->last_line)
    {
      if (yylocp->first_line < yylocp->last_line)
        {
          res += YYFPRINTF (yyo, "-%d", yylocp->last_line);
          if (0 <= end_col)
            res += YYFPRINTF (yyo, ".%d", end_col);
        }
      else if (0 <= end_col && yylocp->first_column < end_col)
        res += YYFPRINTF (yyo, "-%d", end_col);
    }
  return res;
 }

#  define YY_LOCATION_PRINT(File, Loc)          \
  yy_location_print_ (File, &(Loc))

# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


# define YY_SYMBOL_PRINT(Title, Type, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Type, Value, Location, result, scanner); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*----------------------------------------.
| Print this symbol's value on YYOUTPUT.  |
`----------------------------------------*/

static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, nstore::parser::SQLStatementList** result, yyscan_t scanner)
{
  FILE *yyo = yyoutput;
  YYUSE (yyo);
  YYUSE (yylocationp);
  YYUSE (result);
  YYUSE (scanner);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# endif
  YYUSE (yytype);
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, nstore::parser::SQLStatementList** result, yyscan_t scanner)
{
  YYFPRINTF (yyoutput, "%s %s (",
             yytype < YYNTOKENS ? "token" : "nterm", yytname[yytype]);

  YY_LOCATION_PRINT (yyoutput, *yylocationp);
  YYFPRINTF (yyoutput, ": ");
  yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result, scanner);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yytype_int16 *yyssp, YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, nstore::parser::SQLStatementList** result, yyscan_t scanner)
{
  unsigned long int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       yystos[yyssp[yyi + 1 - yynrhs]],
                       &(yyvsp[(yyi + 1) - (yynrhs)])
                       , &(yylsp[(yyi + 1) - (yynrhs)])                       , result, scanner);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, yylsp, Rule, result, scanner); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !PARSER_DEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !PARSER_DEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
static YYSIZE_T
yystrlen (const char *yystr)
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            /* Fall through.  */
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYSIZE_T *yymsg_alloc, char **yymsg,
                yytype_int16 *yyssp, int yytoken)
{
  YYSIZE_T yysize0 = yytnamerr (YY_NULLPTR, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[*yyssp];
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                {
                  YYSIZE_T yysize1 = yysize + yytnamerr (YY_NULLPTR, yytname[yyx]);
                  if (! (yysize <= yysize1
                         && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
                    return 2;
                  yysize = yysize1;
                }
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  {
    YYSIZE_T yysize1 = yysize + yystrlen (yyformat);
    if (! (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
      return 2;
    yysize = yysize1;
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          yyp++;
          yyformat++;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, nstore::parser::SQLStatementList** result, yyscan_t scanner)
{
  YYUSE (yyvaluep);
  YYUSE (yylocationp);
  YYUSE (result);
  YYUSE (scanner);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}




/*----------.
| yyparse.  |
`----------*/

int
yyparse (nstore::parser::SQLStatementList** result, yyscan_t scanner)
{
/* The lookahead symbol.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

/* Location data for the lookahead symbol.  */
static YYLTYPE yyloc_default
# if defined PARSER_LTYPE_IS_TRIVIAL && PARSER_LTYPE_IS_TRIVIAL
  = { 1, 1, 1, 1 }
# endif
;
YYLTYPE yylloc = yyloc_default;

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       'yyss': related to states.
       'yyvs': related to semantic values.
       'yyls': related to locations.

       Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    /* The location stack.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls;
    YYLTYPE *yylsp;

    /* The locations where the error started and ended.  */
    YYLTYPE yyerror_range[3];

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken = 0;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yyssp = yyss = yyssa;
  yyvsp = yyvs = yyvsa;
  yylsp = yyls = yylsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */

/* User initialization code.  */
#line 110 "../../src/parser_bison.ypp" /* yacc.c:1436  */
{
	// Initialize
	yylloc.first_column = 0;
	yylloc.last_column = 0;
	yylloc.first_line = 0;
	yylloc.last_line = 0;
	yylloc.total_column = 0;
	yylloc.placeholder_id = 0;
}

#line 1857 "parser_bison.cpp" /* yacc.c:1436  */
  yylsp[0] = yylloc;
  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        YYSTYPE *yyvs1 = yyvs;
        yytype_int16 *yyss1 = yyss;
        YYLTYPE *yyls1 = yyls;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * sizeof (*yyssp),
                    &yyvs1, yysize * sizeof (*yyvsp),
                    &yyls1, yysize * sizeof (*yylsp),
                    &yystacksize);

        yyls = yyls1;
        yyss = yyss1;
        yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yytype_int16 *yyss1 = yyss;
        union yyalloc *yyptr =
          (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
        if (! yyptr)
          goto yyexhaustedlab;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
        YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
                  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = yylex (&yylval, &yylloc, scanner);
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END
  *++yylsp = yylloc;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location.  */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:
#line 261 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			*result = (yyvsp[-1].stmt_list);
		}
#line 2048 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 3:
#line 268 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.stmt_list) = new SQLStatementList((yyvsp[0].statement)); }
#line 2054 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 4:
#line 269 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyvsp[-2].stmt_list)->AddStatement((yyvsp[0].statement)); (yyval.stmt_list) = (yyvsp[-2].stmt_list); }
#line 2060 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 5:
#line 273 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyvsp[0].prep_stmt)->setPlaceholders(yyloc.placeholder_list);
			yyloc.placeholder_list.clear();
			(yyval.statement) = (yyvsp[0].prep_stmt);
		}
#line 2070 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 7:
#line 283 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.statement) = (yyvsp[0].select_stmt); }
#line 2076 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 8:
#line 284 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.statement) = (yyvsp[0].create_stmt); }
#line 2082 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 9:
#line 285 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.statement) = (yyvsp[0].insert_stmt); }
#line 2088 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 10:
#line 286 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.statement) = (yyvsp[0].delete_stmt); }
#line 2094 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 11:
#line 287 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.statement) = (yyvsp[0].delete_stmt); }
#line 2100 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 12:
#line 288 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.statement) = (yyvsp[0].update_stmt); }
#line 2106 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 13:
#line 289 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.statement) = (yyvsp[0].drop_stmt); }
#line 2112 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 14:
#line 290 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.statement) = (yyvsp[0].exec_stmt); }
#line 2118 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 15:
#line 291 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.statement) = (yyvsp[0].txn_stmt); }
#line 2124 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 16:
#line 299 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.prep_stmt) = new PrepareStatement();
			(yyval.prep_stmt)->name = (yyvsp[-2].sval);
			(yyval.prep_stmt)->query = new SQLStatementList((yyvsp[0].statement));
		}
#line 2134 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 17:
#line 304 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.prep_stmt) = new PrepareStatement();
			(yyval.prep_stmt)->name = (yyvsp[-4].sval);
			(yyval.prep_stmt)->query = (yyvsp[-2].stmt_list);
		}
#line 2144 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 18:
#line 312 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.exec_stmt) = new ExecuteStatement();
			(yyval.exec_stmt)->name = (yyvsp[0].sval);
		}
#line 2153 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 19:
#line 316 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.exec_stmt) = new ExecuteStatement();
			(yyval.exec_stmt)->name = (yyvsp[-3].sval);
			(yyval.exec_stmt)->parameters = (yyvsp[-1].expr_vec);
		}
#line 2163 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 20:
#line 331 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.create_stmt) = new CreateStatement(CreateStatement::kTable);
			(yyval.create_stmt)->if_not_exists = (yyvsp[-4].bval);
			(yyval.create_stmt)->name = (yyvsp[-3].sval);
			(yyval.create_stmt)->columns = (yyvsp[-1].column_vec);
		}
#line 2174 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 21:
#line 337 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.create_stmt) = new CreateStatement(CreateStatement::kDatabase);
			(yyval.create_stmt)->if_not_exists = (yyvsp[-1].bval);
			(yyval.create_stmt)->name = (yyvsp[0].sval);
		}
#line 2184 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 22:
#line 342 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.create_stmt) = new CreateStatement(CreateStatement::kIndex);
			(yyval.create_stmt)->unique = (yyvsp[-7].bval);
			(yyval.create_stmt)->name = (yyvsp[-5].sval);
			(yyval.create_stmt)->table_name = (yyvsp[-3].sval);
			(yyval.create_stmt)->index_attrs = (yyvsp[-1].str_vec);			
		}
#line 2196 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 23:
#line 352 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.bval) = true; }
#line 2202 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 24:
#line 353 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.bval) = false; }
#line 2208 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 25:
#line 357 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.column_vec) = new std::vector<ColumnDefinition*>(); (yyval.column_vec)->push_back((yyvsp[0].column_t)); }
#line 2214 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 26:
#line 358 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyvsp[-2].column_vec)->push_back((yyvsp[0].column_t)); (yyval.column_vec) = (yyvsp[-2].column_vec); }
#line 2220 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 27:
#line 362 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.column_t) = new ColumnDefinition((yyvsp[-5].sval), (ColumnDefinition::DataType) (yyvsp[-4].uval));
			(yyval.column_t)->varlen = (yyvsp[-3].uval);
			(yyval.column_t)->not_null = (yyvsp[-2].bval);
			(yyval.column_t)->primary = (yyvsp[-1].bval);
			(yyval.column_t)->unique = (yyvsp[0].bval);
		}
#line 2232 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 28:
#line 370 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.column_t) = new ColumnDefinition(ColumnDefinition::DataType::PRIMARY);
			(yyval.column_t)->primary_key = (yyvsp[-1].str_vec);
		}
#line 2241 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 29:
#line 375 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.column_t) = new ColumnDefinition(ColumnDefinition::DataType::FOREIGN);
			(yyval.column_t)->foreign_key_source = (yyvsp[-6].str_vec);
			(yyval.column_t)->name = (yyvsp[-3].sval);
			(yyval.column_t)->foreign_key_sink = (yyvsp[-1].str_vec);
		}
#line 2252 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 30:
#line 384 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = (yyvsp[-1].expr)->ival; delete (yyvsp[-1].expr); }
#line 2258 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 31:
#line 385 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = 0; }
#line 2264 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 32:
#line 389 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.bval) = true; }
#line 2270 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 33:
#line 390 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.bval) = false; }
#line 2276 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 34:
#line 394 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.bval) = true; }
#line 2282 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 35:
#line 395 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.bval) = false; }
#line 2288 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 36:
#line 399 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.bval) = true; }
#line 2294 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 37:
#line 400 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.bval) = false; }
#line 2300 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 38:
#line 404 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::INT; }
#line 2306 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 39:
#line 405 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::INT; }
#line 2312 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 40:
#line 406 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::DOUBLE; }
#line 2318 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 41:
#line 407 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::TEXT; }
#line 2324 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 42:
#line 408 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::CHAR; }
#line 2330 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 43:
#line 409 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::TINYINT; }
#line 2336 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 44:
#line 410 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::SMALLINT; }
#line 2342 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 45:
#line 411 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::BIGINT; }
#line 2348 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 46:
#line 412 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::FLOAT; }
#line 2354 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 47:
#line 413 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::DECIMAL; }
#line 2360 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 48:
#line 414 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::BOOLEAN; }
#line 2366 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 49:
#line 415 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::TIMESTAMP; }
#line 2372 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 50:
#line 416 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::VARCHAR; }
#line 2378 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 51:
#line 417 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = ColumnDefinition::VARBINARY; }
#line 2384 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 52:
#line 429 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.drop_stmt) = new DropStatement(DropStatement::kTable);
			(yyval.drop_stmt)->name = (yyvsp[0].sval);
		}
#line 2393 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 53:
#line 434 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.drop_stmt) = new DropStatement(DropStatement::kDatabase);
			(yyval.drop_stmt)->name = (yyvsp[0].sval);
		}
#line 2402 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 54:
#line 439 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.drop_stmt) = new DropStatement(DropStatement::kIndex);
			(yyval.drop_stmt)->name = (yyvsp[-2].sval);
			(yyval.drop_stmt)->table_name = (yyvsp[0].sval);
		}
#line 2412 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 55:
#line 445 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.drop_stmt) = new DropStatement(DropStatement::kPreparedStatement);
			(yyval.drop_stmt)->name = (yyvsp[0].sval);
		}
#line 2421 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 56:
#line 459 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { 
		(yyval.txn_stmt) = new TransactionStatement(TransactionStatement::kBegin);
	}
#line 2429 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 57:
#line 462 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
		(yyval.txn_stmt) = new TransactionStatement(TransactionStatement::kCommit);
	}
#line 2437 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 58:
#line 465 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
		(yyval.txn_stmt) = new TransactionStatement(TransactionStatement::kRollback);
	}
#line 2445 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 61:
#line 480 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.delete_stmt) = new DeleteStatement();
			(yyval.delete_stmt)->table_name = (yyvsp[-1].sval);
			(yyval.delete_stmt)->expr = (yyvsp[0].expr);
		}
#line 2455 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 62:
#line 488 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.delete_stmt) = new DeleteStatement();
			(yyval.delete_stmt)->table_name = (yyvsp[0].sval);
		}
#line 2464 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 63:
#line 500 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.insert_stmt) = new InsertStatement(nstore::INSERT_TYPE_VALUES);
			(yyval.insert_stmt)->table_name = (yyvsp[-5].sval);
			(yyval.insert_stmt)->columns = (yyvsp[-4].str_vec);
			(yyval.insert_stmt)->values = (yyvsp[-1].expr_vec);
		}
#line 2475 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 64:
#line 506 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.insert_stmt) = new InsertStatement(nstore::INSERT_TYPE_SELECT);
			(yyval.insert_stmt)->table_name = (yyvsp[-2].sval);
			(yyval.insert_stmt)->columns = (yyvsp[-1].str_vec);
			(yyval.insert_stmt)->select = (yyvsp[0].select_stmt);
		}
#line 2486 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 65:
#line 516 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.str_vec) = (yyvsp[-1].str_vec); }
#line 2492 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 66:
#line 517 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.str_vec) = NULL; }
#line 2498 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 67:
#line 527 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
		(yyval.update_stmt) = new UpdateStatement();
		(yyval.update_stmt)->table = (yyvsp[-3].table);
		(yyval.update_stmt)->updates = (yyvsp[-1].update_vec);
		(yyval.update_stmt)->where = (yyvsp[0].expr);
	}
#line 2509 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 68:
#line 536 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.update_vec) = new std::vector<UpdateClause*>(); (yyval.update_vec)->push_back((yyvsp[0].update_t)); }
#line 2515 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 69:
#line 537 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyvsp[-2].update_vec)->push_back((yyvsp[0].update_t)); (yyval.update_vec) = (yyvsp[-2].update_vec); }
#line 2521 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 70:
#line 541 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.update_t) = new UpdateClause();
			(yyval.update_t)->column = (yyvsp[-2].sval);
			(yyval.update_t)->value = (yyvsp[0].expr);
		}
#line 2531 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 73:
#line 558 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.select_stmt) = (yyvsp[-1].select_stmt); }
#line 2537 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 74:
#line 559 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.select_stmt) = (yyvsp[-1].select_stmt); }
#line 2543 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 75:
#line 563 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.select_stmt) = (yyvsp[-2].select_stmt);
			(yyval.select_stmt)->order = (yyvsp[-1].order);
			(yyval.select_stmt)->limit = (yyvsp[0].limit);
		}
#line 2553 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 76:
#line 568 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			// TODO: allow multiple unions (through linked list)
			// TODO: capture type of set_operator
			// TODO: might overwrite order and limit of first select here
			(yyval.select_stmt) = (yyvsp[-4].select_stmt);
			(yyval.select_stmt)->union_select = (yyvsp[-2].select_stmt);
			(yyval.select_stmt)->order = (yyvsp[-1].order);
			(yyval.select_stmt)->limit = (yyvsp[0].limit);
		}
#line 2567 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 80:
#line 586 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.select_stmt) = new SelectStatement();
			(yyval.select_stmt)->select_distinct = (yyvsp[-4].bval);
			(yyval.select_stmt)->select_list = (yyvsp[-3].expr_vec);
			(yyval.select_stmt)->from_table = (yyvsp[-2].table);
			(yyval.select_stmt)->where_clause = (yyvsp[-1].expr);
			(yyval.select_stmt)->group_by = (yyvsp[0].group_t);
		}
#line 2580 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 81:
#line 597 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.bval) = true; }
#line 2586 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 82:
#line 598 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.bval) = false; }
#line 2592 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 84:
#line 607 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.table) = (yyvsp[0].table); }
#line 2598 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 85:
#line 612 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = (yyvsp[0].expr); }
#line 2604 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 86:
#line 613 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = NULL; }
#line 2610 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 87:
#line 617 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.group_t) = new GroupByDescription();
			(yyval.group_t)->columns = (yyvsp[-1].expr_vec);
			(yyval.group_t)->having = (yyvsp[0].expr);
		}
#line 2620 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 88:
#line 622 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.group_t) = NULL; }
#line 2626 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 89:
#line 626 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = (yyvsp[0].expr); }
#line 2632 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 90:
#line 627 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = NULL; }
#line 2638 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 91:
#line 630 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.order) = new OrderDescription((yyvsp[0].order_type), (yyvsp[-1].expr)); }
#line 2644 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 92:
#line 631 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.order) = NULL; }
#line 2650 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 93:
#line 635 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.order_type) = kOrderAsc; }
#line 2656 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 94:
#line 636 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.order_type) = kOrderDesc; }
#line 2662 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 95:
#line 637 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.order_type) = kOrderAsc; }
#line 2668 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 96:
#line 642 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.limit) = new LimitDescription((yyvsp[0].expr)->ival, kNoOffset); delete (yyvsp[0].expr); }
#line 2674 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 97:
#line 643 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.limit) = new LimitDescription((yyvsp[-2].expr)->ival, (yyvsp[0].expr)->ival); delete (yyvsp[-2].expr); delete (yyvsp[0].expr); }
#line 2680 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 98:
#line 644 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.limit) = NULL; }
#line 2686 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 99:
#line 651 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr_vec) = new std::vector<nstore::expression::AbstractExpression*>(); (yyval.expr_vec)->push_back((yyvsp[0].expr)); }
#line 2692 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 100:
#line 652 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyvsp[-2].expr_vec)->push_back((yyvsp[0].expr)); (yyval.expr_vec) = (yyvsp[-2].expr_vec); }
#line 2698 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 101:
#line 656 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr_vec) = new std::vector<nstore::expression::AbstractExpression*>(); (yyval.expr_vec)->push_back((yyvsp[0].expr)); }
#line 2704 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 102:
#line 657 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyvsp[-2].expr_vec)->push_back((yyvsp[0].expr)); (yyval.expr_vec) = (yyvsp[-2].expr_vec); }
#line 2710 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 103:
#line 661 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.expr) = (yyvsp[-1].expr);
			(yyval.expr)->alias = (yyvsp[0].sval);
		}
#line 2719 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 104:
#line 668 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = (yyvsp[-1].expr); }
#line 2725 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 112:
#line 682 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::OperatorUnaryMinusExpression((yyvsp[0].expr)); }
#line 2731 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 113:
#line 683 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::OperatorUnaryNotExpression((yyvsp[0].expr)); }
#line 2737 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 115:
#line 688 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::OperatorExpression<nstore::expression::OpMinus>(nstore::EXPRESSION_TYPE_OPERATOR_MINUS, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2743 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 116:
#line 689 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::OperatorExpression<nstore::expression::OpPlus>(nstore::EXPRESSION_TYPE_OPERATOR_PLUS, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2749 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 117:
#line 690 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::OperatorExpression<nstore::expression::OpDivide>(nstore::EXPRESSION_TYPE_OPERATOR_DIVIDE, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2755 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 118:
#line 691 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::OperatorExpression<nstore::expression::OpMultiply>(nstore::EXPRESSION_TYPE_OPERATOR_MULTIPLY, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2761 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 119:
#line 692 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ConjunctionExpression<nstore::expression::ConjunctionAnd>(nstore::EXPRESSION_TYPE_CONJUNCTION_AND, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2767 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 120:
#line 693 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ConjunctionExpression<nstore::expression::ConjunctionOr>(nstore::EXPRESSION_TYPE_CONJUNCTION_OR, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2773 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 121:
#line 694 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ComparisonExpression<nstore::expression::CmpEq>(nstore::EXPRESSION_TYPE_COMPARE_LIKE, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2779 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 122:
#line 695 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ComparisonExpression<nstore::expression::CmpNe>(nstore::EXPRESSION_TYPE_COMPARE_LIKE, (yyvsp[-3].expr), (yyvsp[0].expr)); }
#line 2785 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 123:
#line 700 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ComparisonExpression<nstore::expression::CmpEq>(nstore::EXPRESSION_TYPE_COMPARE_EQ, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2791 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 124:
#line 701 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ComparisonExpression<nstore::expression::CmpNe>(nstore::EXPRESSION_TYPE_COMPARE_NE, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2797 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 125:
#line 702 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ComparisonExpression<nstore::expression::CmpLt>(nstore::EXPRESSION_TYPE_COMPARE_LT, (yyvsp[-2].expr), (yyvsp[0].expr));; }
#line 2803 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 126:
#line 703 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ComparisonExpression<nstore::expression::CmpGt>(nstore::EXPRESSION_TYPE_COMPARE_GT, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2809 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 127:
#line 704 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ComparisonExpression<nstore::expression::CmpLte>(nstore::EXPRESSION_TYPE_COMPARE_LTE, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2815 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 128:
#line 705 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ComparisonExpression<nstore::expression::CmpGte>(nstore::EXPRESSION_TYPE_COMPARE_GTE, (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 2821 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 129:
#line 709 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ParserExpression(nstore::EXPRESSION_TYPE_FUNCTION_REF, (yyvsp[-4].sval), (yyvsp[-1].expr), (yyvsp[-2].bval)); }
#line 2827 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 130:
#line 713 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ParserExpression(nstore::EXPRESSION_TYPE_COLUMN_REF, (yyvsp[0].sval)); }
#line 2833 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 131:
#line 714 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ParserExpression(nstore::EXPRESSION_TYPE_COLUMN_REF, (yyvsp[-2].sval), (yyvsp[0].sval)); }
#line 2839 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 135:
#line 724 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ConstantValueExpression(nstore::ValueFactory::GetStringValue((yyvsp[0].sval))); free((yyvsp[0].sval));}
#line 2845 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 136:
#line 729 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ConstantValueExpression(nstore::ValueFactory::GetDoubleValue((yyvsp[0].fval))); }
#line 2851 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 138:
#line 734 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ConstantValueExpression(nstore::ValueFactory::GetIntegerValue((yyvsp[0].ival))); (yyval.expr)->ival = (yyvsp[0].ival); }
#line 2857 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 139:
#line 738 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.expr) = new nstore::expression::ParserExpression(nstore::EXPRESSION_TYPE_STAR); }
#line 2863 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 140:
#line 743 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.expr) = new nstore::expression::ParserExpression(nstore::EXPRESSION_TYPE_PLACEHOLDER, yylloc.total_column);
			yyloc.placeholder_list.push_back((yyval.expr));
		}
#line 2872 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 142:
#line 755 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyvsp[0].table_vec)->push_back((yyvsp[-2].table));
			auto tbl = new TableRef(nstore::TABLE_REFERENCE_TYPE_CROSS_PRODUCT);
			tbl->list = (yyvsp[0].table_vec);
			(yyval.table) = tbl;
		}
#line 2883 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 144:
#line 766 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			auto tbl = new TableRef(nstore::TABLE_REFERENCE_TYPE_SELECT);
			tbl->select = (yyvsp[-2].select_stmt);
			tbl->alias = (yyvsp[0].sval);
			(yyval.table) = tbl;
		}
#line 2894 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 146:
#line 776 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.table_vec) = new std::vector<TableRef*>(); (yyval.table_vec)->push_back((yyvsp[0].table)); }
#line 2900 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 147:
#line 777 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyvsp[-2].table_vec)->push_back((yyvsp[0].table)); (yyval.table_vec) = (yyvsp[-2].table_vec); }
#line 2906 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 148:
#line 782 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			auto tbl = new TableRef(nstore::TABLE_REFERENCE_TYPE_NAME);
			tbl->name = (yyvsp[-1].sval);
			tbl->alias = (yyvsp[0].sval);
			(yyval.table) = tbl;
		}
#line 2917 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 149:
#line 792 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			(yyval.table) = new TableRef(nstore::TABLE_REFERENCE_TYPE_NAME);
			(yyval.table)->name = (yyvsp[0].sval);
		}
#line 2926 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 152:
#line 806 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.sval) = (yyvsp[0].sval); }
#line 2932 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 155:
#line 812 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.sval) = NULL; }
#line 2938 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 156:
#line 821 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { 
			(yyval.table) = new TableRef(nstore::TABLE_REFERENCE_TYPE_JOIN);
			(yyval.table)->join = new JoinDefinition();
			(yyval.table)->join->type = (nstore::JoinType) (yyvsp[-4].uval);
			(yyval.table)->join->left = (yyvsp[-5].table);
			(yyval.table)->join->right = (yyvsp[-2].table);
			(yyval.table)->join->condition = (yyvsp[0].expr);
		}
#line 2951 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 157:
#line 832 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = nstore::JOIN_TYPE_INNER; }
#line 2957 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 158:
#line 833 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = nstore::JOIN_TYPE_OUTER; }
#line 2963 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 159:
#line 834 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = nstore::JOIN_TYPE_LEFT; }
#line 2969 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 160:
#line 835 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = nstore::JOIN_TYPE_RIGHT; }
#line 2975 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 161:
#line 836 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.uval) = nstore::JOIN_TYPE_INNER; }
#line 2981 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 162:
#line 842 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    {
			auto tbl = new TableRef(nstore::TABLE_REFERENCE_TYPE_SELECT);
			tbl->select = (yyvsp[-2].select_stmt);
			tbl->alias = (yyvsp[0].sval);
			(yyval.table) = tbl;
		}
#line 2992 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 167:
#line 867 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyval.str_vec) = new std::vector<char*>(); (yyval.str_vec)->push_back((yyvsp[0].sval)); }
#line 2998 "parser_bison.cpp" /* yacc.c:1661  */
    break;

  case 168:
#line 868 "../../src/parser_bison.ypp" /* yacc.c:1661  */
    { (yyvsp[-2].str_vec)->push_back((yyvsp[0].sval)); (yyval.str_vec) = (yyvsp[-2].str_vec); }
#line 3004 "parser_bison.cpp" /* yacc.c:1661  */
    break;


#line 3008 "parser_bison.cpp" /* yacc.c:1661  */
      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (&yylloc, result, scanner, YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = (char *) YYSTACK_ALLOC (yymsg_alloc);
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (&yylloc, result, scanner, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }

  yyerror_range[1] = yylloc;

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, &yylloc, result, scanner);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  yyerror_range[1] = yylsp[1-yylen];
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYTERROR;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
                  yystos[yystate], yyvsp, yylsp, result, scanner);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  yyerror_range[2] = yylloc;
  /* Using YYLLOC is tempting, but would change the location of
     the lookahead.  YYLOC is available though.  */
  YYLLOC_DEFAULT (yyloc, yyerror_range, 2);
  *++yylsp = yyloc;

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined yyoverflow || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, result, scanner, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc, result, scanner);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  yystos[*yyssp], yyvsp, yylsp, result, scanner);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  return yyresult;
}
#line 871 "../../src/parser_bison.ypp" /* yacc.c:1906  */

/*********************************
 ** Section 4: Additional C code
 *********************************/

/* empty */

