/* A Bison parser, made by GNU Bison 3.0.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2013 Free Software Foundation, Inc.

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

#ifndef YY_BASE_YY_GRAM_H_INCLUDED
# define YY_BASE_YY_GRAM_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    IDENT = 258,
    FCONST = 259,
    SCONST = 260,
    BCONST = 261,
    XCONST = 262,
    Op = 263,
    ICONST = 264,
    PARAM = 265,
    TYPECAST = 266,
    DOT_DOT = 267,
    COLON_EQUALS = 268,
    EQUALS_GREATER = 269,
    LESS_EQUALS = 270,
    GREATER_EQUALS = 271,
    NOT_EQUALS = 272,
    ABORT_P = 273,
    ABSOLUTE_P = 274,
    ACCESS = 275,
    ACTION = 276,
    ADD_P = 277,
    ADMIN = 278,
    AFTER = 279,
    AGGREGATE = 280,
    ALL = 281,
    ALSO = 282,
    ALTER = 283,
    ALWAYS = 284,
    ANALYSE = 285,
    ANALYZE = 286,
    AND = 287,
    ANY = 288,
    ARRAY = 289,
    AS = 290,
    ASC = 291,
    ASSERTION = 292,
    ASSIGNMENT = 293,
    ASYMMETRIC = 294,
    AT = 295,
    ATTRIBUTE = 296,
    AUTHORIZATION = 297,
    BACKWARD = 298,
    BEFORE = 299,
    BEGIN_P = 300,
    BETWEEN = 301,
    BIGINT = 302,
    BINARY = 303,
    BIT = 304,
    BOOLEAN_P = 305,
    BOTH = 306,
    BY = 307,
    CACHE = 308,
    CALLED = 309,
    CASCADE = 310,
    CASCADED = 311,
    CASE = 312,
    CAST = 313,
    CATALOG_P = 314,
    CHAIN = 315,
    CHAR_P = 316,
    CHARACTER = 317,
    CHARACTERISTICS = 318,
    CHECK = 319,
    CHECKPOINT = 320,
    CLASS = 321,
    CLOSE = 322,
    CLUSTER = 323,
    COALESCE = 324,
    COLLATE = 325,
    COLLATION = 326,
    COLUMN = 327,
    COMMENT = 328,
    COMMENTS = 329,
    COMMIT = 330,
    COMMITTED = 331,
    CONCURRENTLY = 332,
    CONFIGURATION = 333,
    CONFLICT = 334,
    CONNECTION = 335,
    CONSTRAINT = 336,
    CONSTRAINTS = 337,
    CONTENT_P = 338,
    CONTINUE_P = 339,
    CONVERSION_P = 340,
    COPY = 341,
    COST = 342,
    CREATE = 343,
    CROSS = 344,
    CSV = 345,
    CUBE = 346,
    CURRENT_P = 347,
    CURRENT_CATALOG = 348,
    CURRENT_DATE = 349,
    CURRENT_ROLE = 350,
    CURRENT_SCHEMA = 351,
    CURRENT_TIME = 352,
    CURRENT_TIMESTAMP = 353,
    CURRENT_USER = 354,
    CURSOR = 355,
    CYCLE = 356,
    DATA_P = 357,
    DATABASE = 358,
    DAY_P = 359,
    DEALLOCATE = 360,
    DEC = 361,
    DECIMAL_P = 362,
    DECLARE = 363,
    DEFAULT = 364,
    DEFAULTS = 365,
    DEFERRABLE = 366,
    DEFERRED = 367,
    DEFINER = 368,
    DELETE_P = 369,
    DELIMITER = 370,
    DELIMITERS = 371,
    DESC = 372,
    DICTIONARY = 373,
    DISABLE_P = 374,
    DISCARD = 375,
    DISTINCT = 376,
    DO = 377,
    DOCUMENT_P = 378,
    DOMAIN_P = 379,
    DOUBLE_P = 380,
    DROP = 381,
    EACH = 382,
    ELSE = 383,
    ENABLE_P = 384,
    ENCODING = 385,
    ENCRYPTED = 386,
    END_P = 387,
    ENUM_P = 388,
    ESCAPE = 389,
    EVENT = 390,
    EXCEPT = 391,
    EXCLUDE = 392,
    EXCLUDING = 393,
    EXCLUSIVE = 394,
    EXECUTE = 395,
    EXISTS = 396,
    EXPLAIN = 397,
    EXTENSION = 398,
    EXTERNAL = 399,
    EXTRACT = 400,
    FALSE_P = 401,
    FAMILY = 402,
    FETCH = 403,
    FILTER = 404,
    FIRST_P = 405,
    FLOAT_P = 406,
    FOLLOWING = 407,
    FOR = 408,
    FORCE = 409,
    FOREIGN = 410,
    FORWARD = 411,
    FREEZE = 412,
    FROM = 413,
    FULL = 414,
    FUNCTION = 415,
    FUNCTIONS = 416,
    GLOBAL = 417,
    GRANT = 418,
    GRANTED = 419,
    GREATEST = 420,
    GROUP_P = 421,
    GROUPING = 422,
    HANDLER = 423,
    HAVING = 424,
    HEADER_P = 425,
    HOLD = 426,
    HOUR_P = 427,
    IDENTITY_P = 428,
    IF_P = 429,
    ILIKE = 430,
    IMMEDIATE = 431,
    IMMUTABLE = 432,
    IMPLICIT_P = 433,
    IMPORT_P = 434,
    IN_P = 435,
    INCLUDING = 436,
    INCREMENT = 437,
    INDEX = 438,
    INDEXES = 439,
    INHERIT = 440,
    INHERITS = 441,
    INITIALLY = 442,
    INLINE_P = 443,
    INNER_P = 444,
    INOUT = 445,
    INPUT_P = 446,
    INSENSITIVE = 447,
    INSERT = 448,
    INSTEAD = 449,
    INT_P = 450,
    INTEGER = 451,
    INTERSECT = 452,
    INTERVAL = 453,
    INTO = 454,
    INVOKER = 455,
    IS = 456,
    ISNULL = 457,
    ISOLATION = 458,
    JOIN = 459,
    KEY = 460,
    LABEL = 461,
    LANGUAGE = 462,
    LARGE_P = 463,
    LAST_P = 464,
    LATERAL_P = 465,
    LEADING = 466,
    LEAKPROOF = 467,
    LEAST = 468,
    LEFT = 469,
    LEVEL = 470,
    LIKE = 471,
    LIMIT = 472,
    LISTEN = 473,
    LOAD = 474,
    LOCAL = 475,
    LOCALTIME = 476,
    LOCALTIMESTAMP = 477,
    LOCATION = 478,
    LOCK_P = 479,
    LOCKED = 480,
    LOGGED = 481,
    MAPPING = 482,
    MATCH = 483,
    MATERIALIZED = 484,
    MAXVALUE = 485,
    MINUTE_P = 486,
    MINVALUE = 487,
    MODE = 488,
    MONTH_P = 489,
    MOVE = 490,
    NAME_P = 491,
    NAMES = 492,
    NATIONAL = 493,
    NATURAL = 494,
    NCHAR = 495,
    NEXT = 496,
    NO = 497,
    NONE = 498,
    NOT = 499,
    NOTHING = 500,
    NOTIFY = 501,
    NOTNULL = 502,
    NOWAIT = 503,
    NULL_P = 504,
    NULLIF = 505,
    NULLS_P = 506,
    NUMERIC = 507,
    OBJECT_P = 508,
    OF = 509,
    OFF = 510,
    OFFSET = 511,
    OIDS = 512,
    ON = 513,
    ONLY = 514,
    OPERATOR = 515,
    OPTION = 516,
    OPTIONS = 517,
    OR = 518,
    ORDER = 519,
    ORDINALITY = 520,
    OUT_P = 521,
    OUTER_P = 522,
    OVER = 523,
    OVERLAPS = 524,
    OVERLAY = 525,
    OWNED = 526,
    OWNER = 527,
    PARSER = 528,
    PARTIAL = 529,
    PARTITION = 530,
    PASSING = 531,
    PASSWORD = 532,
    PLACING = 533,
    PLANS = 534,
    POLICY = 535,
    POSITION = 536,
    PRECEDING = 537,
    PRECISION = 538,
    PRESERVE = 539,
    PREPARE = 540,
    PREPARED = 541,
    PRIMARY = 542,
    PRIOR = 543,
    PRIVILEGES = 544,
    PROCEDURAL = 545,
    PROCEDURE = 546,
    PROGRAM = 547,
    QUOTE = 548,
    RANGE = 549,
    READ = 550,
    REAL = 551,
    REASSIGN = 552,
    RECHECK = 553,
    RECURSIVE = 554,
    REF = 555,
    REFERENCES = 556,
    REFRESH = 557,
    REINDEX = 558,
    RELATIVE_P = 559,
    RELEASE = 560,
    RENAME = 561,
    REPEATABLE = 562,
    REPLACE = 563,
    REPLICA = 564,
    RESET = 565,
    RESTART = 566,
    RESTRICT = 567,
    RETURNING = 568,
    RETURNS = 569,
    REVOKE = 570,
    RIGHT = 571,
    ROLE = 572,
    ROLLBACK = 573,
    ROLLUP = 574,
    ROW = 575,
    ROWS = 576,
    RULE = 577,
    SAVEPOINT = 578,
    SCHEMA = 579,
    SCROLL = 580,
    SEARCH = 581,
    SECOND_P = 582,
    SECURITY = 583,
    SELECT = 584,
    SEQUENCE = 585,
    SEQUENCES = 586,
    SERIALIZABLE = 587,
    SERVER = 588,
    SESSION = 589,
    SESSION_USER = 590,
    SET = 591,
    SETS = 592,
    SETOF = 593,
    SHARE = 594,
    SHOW = 595,
    SIMILAR = 596,
    SIMPLE = 597,
    SKIP = 598,
    SMALLINT = 599,
    SNAPSHOT = 600,
    SOME = 601,
    SQL_P = 602,
    STABLE = 603,
    STANDALONE_P = 604,
    START = 605,
    STATEMENT = 606,
    STATISTICS = 607,
    STDIN = 608,
    STDOUT = 609,
    STORAGE = 610,
    STRICT_P = 611,
    STRIP_P = 612,
    SUBSTRING = 613,
    SYMMETRIC = 614,
    SYSID = 615,
    SYSTEM_P = 616,
    TABLE = 617,
    TABLES = 618,
    TABLESAMPLE = 619,
    TABLESPACE = 620,
    TEMP = 621,
    TEMPLATE = 622,
    TEMPORARY = 623,
    TEXT_P = 624,
    THEN = 625,
    TIME = 626,
    TIMESTAMP = 627,
    TO = 628,
    TRAILING = 629,
    TRANSACTION = 630,
    TRANSFORM = 631,
    TREAT = 632,
    TRIGGER = 633,
    TRIM = 634,
    TRUE_P = 635,
    TRUNCATE = 636,
    TRUSTED = 637,
    TYPE_P = 638,
    TYPES_P = 639,
    UNBOUNDED = 640,
    UNCOMMITTED = 641,
    UNENCRYPTED = 642,
    UNION = 643,
    UNIQUE = 644,
    UNKNOWN = 645,
    UNLISTEN = 646,
    UNLOGGED = 647,
    UNTIL = 648,
    UPDATE = 649,
    USER = 650,
    USING = 651,
    VACUUM = 652,
    VALID = 653,
    VALIDATE = 654,
    VALIDATOR = 655,
    VALUE_P = 656,
    VALUES = 657,
    VARCHAR = 658,
    VARIADIC = 659,
    VARYING = 660,
    VERBOSE = 661,
    VERSION_P = 662,
    VIEW = 663,
    VIEWS = 664,
    VOLATILE = 665,
    WHEN = 666,
    WHERE = 667,
    WHITESPACE_P = 668,
    WINDOW = 669,
    WITH = 670,
    WITHIN = 671,
    WITHOUT = 672,
    WORK = 673,
    WRAPPER = 674,
    WRITE = 675,
    XML_P = 676,
    XMLATTRIBUTES = 677,
    XMLCONCAT = 678,
    XMLELEMENT = 679,
    XMLEXISTS = 680,
    XMLFOREST = 681,
    XMLPARSE = 682,
    XMLPI = 683,
    XMLROOT = 684,
    XMLSERIALIZE = 685,
    YEAR_P = 686,
    YES_P = 687,
    ZONE = 688,
    NOT_LA = 689,
    NULLS_LA = 690,
    WITH_LA = 691,
    POSTFIXOP = 692,
    UMINUS = 693
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE YYSTYPE;
union YYSTYPE
{
#line 191 "/home/parallels/git/postgres/orig/../src/backend/parser/gram.y" /* yacc.c:1909  */

	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	FuncWithArgs		*funwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	WindowDef			*windef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	InferClause			*infer;
	OnConflictClause	*onconflict;
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	struct ImportQual	*importqual;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;

#line 533 "gram.h" /* yacc.c:1909  */
};
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif



int base_yyparse (core_yyscan_t yyscanner);

#endif /* !YY_BASE_YY_GRAM_H_INCLUDED  */
