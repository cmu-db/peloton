//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parsenodes.h
//
// Identification: src/include/parser/parsenodes.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "nodes.h"
#include "pg_list.h"

typedef enum SetOperation {
  SETOP_NONE = 0,
  SETOP_UNION,
  SETOP_INTERSECT,
  SETOP_EXCEPT
} SetOperation;

typedef struct Alias {
  NodeTag type;
  char *aliasname; /* aliased rel name (never qualified) */
  List *colnames;  /* optional list of column aliases */
} Alias;

typedef enum InhOption {
  INH_NO,     /* Do NOT scan child tables */
  INH_YES,    /* DO scan child tables */
  INH_DEFAULT /* Use current SQL_inheritance option */
} InhOption;

typedef enum BoolExprType {
  AND_EXPR,
  OR_EXPR,
  NOT_EXPR
} BoolExprType;

typedef struct Expr {
  NodeTag type;
} Expr;

typedef struct BoolExpr {
  Expr xpr;
  BoolExprType boolop;
  List *args;   /* arguments to this expression */
  int location; /* token location, or -1 if unknown */
} BoolExpr;

typedef enum A_Expr_Kind {
  AEXPR_OP,              /* normal operator */
  AEXPR_OP_ANY,          /* scalar op ANY (array) */
  AEXPR_OP_ALL,          /* scalar op ALL (array) */
  AEXPR_DISTINCT,        /* IS DISTINCT FROM - name must be "=" */
  AEXPR_NULLIF,          /* NULLIF - name must be "=" */
  AEXPR_OF,              /* IS [NOT] OF - name must be "=" or "<>" */
  AEXPR_IN,              /* [NOT] IN - name must be "=" or "<>" */
  AEXPR_LIKE,            /* [NOT] LIKE - name must be "~~" or "!~~" */
  AEXPR_ILIKE,           /* [NOT] ILIKE - name must be "~~*" or "!~~*" */
  AEXPR_SIMILAR,         /* [NOT] SIMILAR - name must be "~" or "!~" */
  AEXPR_BETWEEN,         /* name must be "BETWEEN" */
  AEXPR_NOT_BETWEEN,     /* name must be "NOT BETWEEN" */
  AEXPR_BETWEEN_SYM,     /* name must be "BETWEEN SYMMETRIC" */
  AEXPR_NOT_BETWEEN_SYM, /* name must be "NOT BETWEEN SYMMETRIC" */
  AEXPR_PAREN            /* nameless dummy node for parentheses */
} A_Expr_Kind;

typedef struct A_Expr {
  NodeTag type;
  A_Expr_Kind kind; /* see above */
  List *name;       /* possibly-qualified name of operator */
  Node *lexpr;      /* left argument, or NULL if none */
  Node *rexpr;      /* right argument, or NULL if none */
  int location;     /* token location, or -1 if unknown */
} A_Expr;

typedef struct JoinExpr {
  NodeTag type;
  JoinType jointype; /* type of join */
  bool isNatural;    /* Natural join? Will need to shape table */
  Node *larg;        /* left subtree */
  Node *rarg;        /* right subtree */
  List *usingClause; /* USING clause, if any (list of String) */
  Node *quals;       /* qualifiers on join, if any */
  Alias *alias;      /* user-written alias clause, if any */
  int rtindex;       /* RT index assigned for join, or 0 */
} JoinExpr;

typedef struct RangeSubselect {
  NodeTag type;
  bool lateral;   /* does it have LATERAL prefix? */
  Node *subquery; /* the untransformed sub-select clause */
  Alias *alias;   /* table alias & optional column aliases */
} RangeSubselect;

typedef struct RangeVar {
  NodeTag type;
  char *catalogname; /* the catalog (database) name, or NULL */
  char *schemaname;  /* the schema name, or NULL */
  char *relname;     /* the relation/sequence name */
  InhOption inhOpt;  /* expand rel by inheritance? recursively act
        * on children? */
  char relpersistence; /* see RELPERSISTENCE_* in pg_class.h */
  Alias *alias;        /* table alias & optional column aliases */
  int location;        /* token location, or -1 if unknown */
} RangeVar;

typedef struct WithClause {
  NodeTag type;
  List *ctes;     /* list of CommonTableExprs */
  bool recursive; /* true = WITH RECURSIVE */
  int location;   /* token location, or -1 if unknown */
} WithClause;

typedef enum OnCommitAction {
  ONCOMMIT_NOOP,          /* No ON COMMIT clause (do nothing) */
  ONCOMMIT_PRESERVE_ROWS, /* ON COMMIT PRESERVE ROWS (do nothing) */
  ONCOMMIT_DELETE_ROWS,   /* ON COMMIT DELETE ROWS */
  ONCOMMIT_DROP           /* ON COMMIT DROP */
} OnCommitAction;

typedef struct IntoClause {
  NodeTag type;

  RangeVar *rel;           /* target relation name */
  List *colNames;          /* column names to assign, or NIL */
  List *options;           /* options from WITH clause */
  OnCommitAction onCommit; /* what do we do at COMMIT? */
  char *tableSpaceName;    /* table space to use, or NULL */
  Node *viewQuery;         /* materialized view's SELECT query */
  bool skipData;           /* true for WITH NO DATA */
} IntoClause;

typedef enum SortByDir {
  SORTBY_DEFAULT,
  SORTBY_ASC,
  SORTBY_DESC,
  SORTBY_USING /* not allowed in CREATE INDEX ... */
} SortByDir;

typedef enum SortByNulls {
  SORTBY_NULLS_DEFAULT,
  SORTBY_NULLS_FIRST,
  SORTBY_NULLS_LAST
} SortByNulls;

typedef struct SortBy {
  NodeTag type;
  Node *node;               /* expression to sort on */
  SortByDir sortby_dir;     /* ASC/DESC/USING/default */
  SortByNulls sortby_nulls; /* NULLS FIRST/LAST */
  List *useOp;              /* name of op to use, if SORTBY_USING */
  int location;             /* operator location, or -1 if none/unknown */
} SortBy;

typedef struct InferClause {
  NodeTag type;
  List *indexElems;  /* IndexElems to infer unique index */
  Node *whereClause; /* qualification (partial-index predicate) */
  char *conname;     /* Constraint name, or NULL if unnamed */
  int location;      /* token location, or -1 if unknown */
} InferClause;

typedef struct OnConflictClause {
  NodeTag type;
  OnConflictAction action; /* DO NOTHING or UPDATE? */
  InferClause *infer;      /* Optional index inference clause */
  List *targetList;        /* the target list (of ResTarget) */
  Node *whereClause;       /* qualifications */
  int location;            /* token location, or -1 if unknown */
} OnConflictClause;

typedef struct InsertStmt {
  NodeTag type;
  RangeVar *relation; /* relation to insert into */
  List *cols;         /* optional: names of the target columns */
  Node *selectStmt;   /* the source SELECT/VALUES, or NULL */
  OnConflictClause *onConflictClause; /* ON CONFLICT clause */
  List *returningList;                /* list of expressions to return */
  WithClause *withClause;             /* WITH clause */
} InsertStmt;

typedef struct SelectStmt {
  NodeTag type;

  /*
   * These fields are used only in "leaf" SelectStmts.
   */
  List *distinctClause; /* NULL, list of DISTINCT ON exprs, or
             * lcons(NIL,NIL) for all (SELECT DISTINCT) */
  IntoClause *intoClause; /* target for SELECT INTO */
  List *targetList;       /* the target list (of ResTarget) */
  List *fromClause;       /* the FROM clause */
  Node *whereClause;      /* WHERE qualification */
  List *groupClause;      /* GROUP BY clauses */
  Node *havingClause;     /* HAVING conditional-expression */
  List *windowClause;     /* WINDOW window_name AS (...), ... */

  /*
   * In a "leaf" node representing a VALUES list, the above fields are all
   * null, and instead this field is set.  Note that the elements of the
   * sublists are just expressions, without ResTarget decoration. Also note
   * that a list element can be DEFAULT (represented as a SetToDefault
   * node), regardless of the context of the VALUES list. It's up to parse
   * analysis to reject that where not valid.
   */
  List *valuesLists; /* untransformed list of expression lists */

  /*
   * These fields are used in both "leaf" SelectStmts and upper-level
   * SelectStmts.
   */
  List *sortClause;       /* sort clause (a list of SortBy's) */
  Node *limitOffset;      /* # of result tuples to skip */
  Node *limitCount;       /* # of result tuples to return */
  List *lockingClause;    /* FOR UPDATE (list of LockingClause's) */
  WithClause *withClause; /* WITH clause */

  /*
   * These fields are used only in upper-level SelectStmts.
   */
  SetOperation op;         /* type of set op */
  bool all;                /* ALL specified? */
  struct SelectStmt *larg; /* left child */
  struct SelectStmt *rarg; /* right child */
  /* Eventually add fields for CORRESPONDING spec here */
} SelectStmt;

typedef struct TypeName
{
  NodeTag   type;
  List     *names;      /* qualified name (list of Value strings) */
  Oid     typeOid;    /* type identified by OID */
  bool    setof;      /* is a set? */
  bool    pct_type;   /* %TYPE specified? */
  List     *typmods;    /* type modifier expression(s) */
  int   typemod;    /* prespecified type modifier */
  List     *arrayBounds;  /* array bounds */
  int     location;   /* token location, or -1 if unknown */
} TypeName;

typedef struct ColumnDef
{
NodeTag		type;
char	   *colname;		/* name of column */
TypeName   *typeName;		/* type of column */
int			inhcount;		/* number of times column is inherited */
bool		is_local;		/* column has local (non-inherited) def'n */
bool		is_not_null;	/* NOT NULL constraint specified? */
bool		is_from_type;	/* column definition came from table type */
char		storage;		/* attstorage setting, or 0 for default */
Node	   *raw_default;	/* default value (untransformed parse tree) */
Node	   *cooked_default; /* default value (transformed expr tree) */
Node *collClause;	/* untransformed COLLATE spec, if any */
Oid			collOid;		/* collation OID (InvalidOid if not set) */
List	   *constraints;	/* other constraints on column */
List	   *fdwoptions;		/* per-column FDW options */
int			location;		/* parse location, or -1 if none/unknown */
} ColumnDef;

typedef struct CreateStmt
{
  NodeTag   type;
  RangeVar   *relation;   /* relation to create */
  List     *tableElts;    /* column definitions (list of ColumnDef) */
  List     *inhRelations; /* relations to inherit from (list of
                 * inhRelation) */
  TypeName   *ofTypename;   /* OF typename */
  List     *constraints;  /* constraints (list of Constraint nodes) */
  List     *options;    /* options from WITH clause */
  OnCommitAction oncommit;  /* what do we do at COMMIT? */
  char     *tablespacename; /* table space to use, or NULL */
  bool    if_not_exists;  /* just do nothing if it already exists? */
} CreateStmt;

typedef struct DeleteStmt
{
    NodeTag     type;
    RangeVar   *relation;       /* relation to delete from */
    List       *usingClause;    /* optional using clause for more tables */
    Node       *whereClause;    /* qualifications */
    List       *returningList;  /* list of expressions to return */
    WithClause *withClause;     /* WITH clause */
} DeleteStmt;

typedef struct ResTarget {
  NodeTag type;
  char *name;        /* column name or NULL */
  List *indirection; /* subscripts, field names, and '*', or NIL */
  Node *val;         /* the value expression to compute or assign */
  int location;      /* token location, or -1 if unknown */
} ResTarget;

typedef struct ColumnRef {
  NodeTag type;
  List *fields; /* field names (Value strings) or A_Star */
  int location; /* token location, or -1 if unknown */
} ColumnRef;

typedef struct A_Const {
  NodeTag type;
  value val;    /* value (includes type info, see value.h) */
  int location; /* token location, or -1 if unknown */
} A_Const;

typedef struct WindowDef {
  NodeTag type;
  char *name;            /* window's own name */
  char *refname;         /* referenced window name, if any */
  List *partitionClause; /* PARTITION BY expression list */
  List *orderClause;     /* ORDER BY (list of SortBy) */
  int frameOptions;      /* frame_clause options, see below */
  Node *startOffset;     /* expression for starting bound, if any */
  Node *endOffset;       /* expression for ending bound, if any */
  int location;          /* parse location, or -1 if none/unknown */
} WindowDef;

typedef struct FuncCall {
  NodeTag type;
  List *funcname;         /* qualified name of function */
  List *args;             /* the arguments (list of exprs) */
  List *agg_order;        /* ORDER BY (list of SortBy) */
  Node *agg_filter;       /* FILTER clause, if any */
  bool agg_within_group;  /* ORDER BY appeared in WITHIN GROUP */
  bool agg_star;          /* argument was really '*' */
  bool agg_distinct;      /* arguments were labeled DISTINCT */
  bool func_variadic;     /* last argument was labeled VARIADIC */
  struct WindowDef *over; /* OVER clause, if any */
  int location;           /* token location, or -1 if unknown */
} FuncCall;

typedef struct UpdateStmt
{
  NodeTag		type;
  RangeVar   *relation;		/* relation to update */
  List	   *targetList;		/* the target list (of ResTarget) */
  Node	   *whereClause;	/* qualifications */
  List	   *fromClause;		/* optional from clause for more tables */
  List	   *returningList;	/* list of expressions to return */
  WithClause *withClause;		/* WITH clause */
} UpdateStmt;
