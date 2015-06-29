[1mdiff --git a/src/postgres/include/nodes/parsenodes.h b/src/postgres/include/nodes/parsenodes.h[m
[1mindex ae943b9..71bf865 100644[m
[1m--- a/src/postgres/include/nodes/parsenodes.h[m
[1m+++ b/src/postgres/include/nodes/parsenodes.h[m
[36m@@ -230,9 +230,10 @@[m [mtypedef struct ColumnRef {[m
 /*[m
  * ParamRef - specifies a $n parameter reference[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ParamRef : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ParamRef {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -262,9 +263,10 @@[m [mtypedef enum A_Expr_Kind[m
 	AEXPR_PAREN					/* nameless dummy node for parentheses */[m
 } A_Expr_Kind;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct A_Expr : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct A_Expr {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -278,9 +280,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * A_Const - a literal constant[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct A_Const : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct A_Const {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -291,9 +294,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * TypeCast - a CAST expression[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct TypeCast : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct TypeCast {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -305,9 +309,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * CollateClause - a COLLATE expression[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CollateClause : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CollateClause {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -327,9 +332,10 @@[m [mtypedef enum RoleSpecType[m
 	ROLESPEC_PUBLIC			/* role name is "public" */[m
 } RoleSpecType;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct RoleSpec : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct RoleSpec {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -352,9 +358,10 @@[m [mtypedef struct XXX {[m
  * Normally, you'd initialize this via makeFuncCall() and then only change the[m
  * parts of the struct its defaults don't match afterwards, as needed.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct FuncCall : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct FuncCall {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -373,9 +380,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * TableSampleClause - a sampling method information[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct TableSampleClause : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct TableSampleClause {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -399,9 +407,10 @@[m [mtypedef struct XXX {[m
  * This can appear within ColumnRef.fields, A_Indirection.indirection, and[m
  * ResTarget.indirection lists.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct A_Star : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct A_Star {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -410,9 +419,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * A_Indices - array subscript or slice bounds ([lidx:uidx] or [uidx])[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct A_Indices : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct A_Indices {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -435,9 +445,10 @@[m [mtypedef struct XXX {[m
  * Currently, A_Star must appear only as the last list element --- the grammar[m
  * is responsible for enforcing this![m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct A_Indirection : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct A_Indirection {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -448,9 +459,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * A_ArrayExpr - an ARRAY[] construct[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct A_ArrayExpr : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct A_ArrayExpr {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -476,9 +488,10 @@[m [mtypedef struct XXX {[m
  *[m
  * See A_Indirection for more info about what can appear in 'indirection'.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ResTarget : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ResTarget {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -497,9 +510,10 @@[m [mtypedef struct XXX {[m
  * row-valued-expression (which parse analysis will process only once, when[m
  * handling the MultiAssignRef with colno=1).[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct MultiAssignRef : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct MultiAssignRef {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -511,9 +525,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * SortBy - for ORDER BY clause[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct SortBy : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct SortBy {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -534,9 +549,10 @@[m [mtypedef struct XXX {[m
  *[m
  * Peloton porting: use c++ inheritance[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct WindowDef : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct WindowDef {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -585,9 +601,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * RangeSubselect - subquery appearing in a FROM clause[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct RangeSubselect : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct RangeSubselect {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -610,9 +627,10 @@[m [mtypedef struct XXX {[m
  * at the top level.  (We disallow coldeflist appearing both here and[m
  * per-function, but that's checked in parse analysis, not by the grammar.)[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct RangeFunction : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct RangeFunction {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -632,9 +650,10 @@[m [mtypedef struct XXX {[m
  * custom tablesample methods which may need different input arguments so we[m
  * accept list of arguments.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct RangeTableSample : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct RangeTableSample {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -662,9 +681,10 @@[m [mtypedef struct XXX {[m
  * the item and set raw_default instead.  CONSTR_DEFAULT items[m
  * should not appear in any subsequent processing.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ColumnDef : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ColumnDef {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -687,9 +707,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * TableLikeClause - CREATE TABLE ( ... LIKE ... ) clause[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct TableLikeClause : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct TableLikeClause {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -714,9 +735,10 @@[m [mtypedef enum TableLikeOption[m
  * index, and 'expr' is NULL.  For an index expression, 'name' is NULL and[m
  * 'expr' is the expression tree.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct IndexElem : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct IndexElem {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -747,9 +769,10 @@[m [mtypedef enum DefElemAction[m
 	DEFELEM_DROP[m
 } DefElemAction;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DefElem : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DefElem {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -768,9 +791,10 @@[m [mtypedef struct XXX {[m
  * a location field --- currently, parse analysis insists on unqualified[m
  * names in LockingClause.)[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct LockingClause : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct LockingClause {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -782,9 +806,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * XMLSERIALIZE (in raw parse tree only)[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct XmlSerialize : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct XmlSerialize {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -885,9 +910,10 @@[m [mtypedef enum RTEKind[m
 /* Peloton porting: use c++ inheritance[m
  *[m
  * */[m
[32m+[m[32m#ifdef __cplusplus[m
 struct RangeTblEntry : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct RangeTblEntry {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -992,9 +1018,10 @@[m [mtypedef struct XXX {[m
  * (including dropped columns!), so that we can successfully ignore any[m
  * columns added after the query was parsed.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct RangeTblFunction : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct RangeTblFunction {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1024,9 +1051,10 @@[m [mtypedef enum WCOKind[m
 	WCO_RLS_CONFLICT_CHECK		/* RLS ON CONFLICT DO UPDATE USING policy */[m
 } WCOKind;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct WithCheckOption : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct WithCheckOption {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1094,9 +1122,10 @@[m [mtypedef struct XXX {[m
  * ORDER BY and set up for the Unique step.  This is semantically necessary[m
  * for DISTINCT ON, and presents no real drawback for DISTINCT.)[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct SortGroupClause : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct SortGroupClause {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1166,9 +1195,10 @@[m [mtypedef enum[m
 	GROUPING_SET_SETS[m
 } GroupingSetKind;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct GroupingSet : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct GroupingSet {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1191,9 +1221,10 @@[m [mtypedef struct XXX {[m
  * the orderClause might or might not be copied (see copiedOrder); the framing[m
  * options are never copied, per spec.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct WindowClause : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct WindowClause {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1220,9 +1251,10 @@[m [mtypedef struct XXX {[m
  * level.  Also, Query.hasForUpdate tells whether there were explicit FOR[m
  * UPDATE/SHARE/KEY SHARE clauses in the current query level.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct RowMarkClause : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct RowMarkClause {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1241,9 +1273,10 @@[m [mtypedef struct XXX {[m
  *[m
  * Peloton porting: use c++ inheritance[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct WithClause : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct WithClause {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1260,9 +1293,10 @@[m [mtypedef struct XXX {[m
  *[m
  * Peloton porting: use c++ inheritance[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct InferClause : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct InferClause {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1278,8 +1312,12 @@[m [mtypedef struct XXX {[m
  *[m
  * Note: OnConflictClause does not propagate into the Query representation.[m
  */[m
[31m-typedef struct OnConflictClause : Node[m
[31m-{[m
[32m+[m[32m#ifdef __cplusplus[m
[32m+[m[32mtypedef struct OnConflictClause : Node {[m
[32m+[m[32m#else[m
[32m+[m[32mtypedef struct OnConflictClause {[m
[32m+[m	[32mNodeTag		type;[m
[32m+[m[32m#endif[m
 	//NodeTag   type;[m
 	OnConflictAction action;		/* DO NOTHING or UPDATE? */[m
 	InferClause	   *infer;			/* Optional index inference clause */[m
[36m@@ -1294,9 +1332,10 @@[m [mtypedef struct OnConflictClause : Node[m
  *[m
  * We don't currently support the SEARCH or CYCLE clause.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CommonTableExpr : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CommonTableExpr {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1335,9 +1374,10 @@[m [mtypedef struct XXX {[m
  * is INSERT ... DEFAULT VALUES.[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct InsertStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct InsertStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1353,9 +1393,10 @@[m [mtypedef struct XXX {[m
  *		Delete Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DeleteStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DeleteStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1370,9 +1411,10 @@[m [mtypedef struct XXX {[m
  *		Update Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct UpdateStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct UpdateStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1407,9 +1449,10 @@[m [mtypedef enum SetOperation[m
 	SETOP_EXCEPT[m
 } SetOperation;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct SelectStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct SelectStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1477,9 +1520,10 @@[m [mtypedef struct XXX {[m
  * column has a collatable type.[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct SetOperationStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct SetOperationStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1570,9 +1614,10 @@[m [mtypedef enum ObjectType[m
  * executed after the schema itself is created.[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateSchemaStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateSchemaStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1592,9 +1637,10 @@[m [mtypedef enum DropBehavior[m
  *	Alter Table[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterTableStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterTableStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1667,9 +1713,10 @@[m [mtypedef enum AlterTableType[m
 	AT_GenericOptions			/* OPTIONS (...) */[m
 } AlterTableType;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ReplicaIdentityStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ReplicaIdentityStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1677,9 +1724,10 @@[m [mtypedef struct XXX {[m
 	char	   *name;[m
 } ReplicaIdentityStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterTableCmd	/* one subcommand of an ALTER TABLE */ : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterTableCmd {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1701,9 +1749,10 @@[m [mtypedef struct XXX {[m
  * this command.[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterDomainStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterDomainStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1751,9 +1800,10 @@[m [mtypedef enum GrantObjectType[m
 	ACL_OBJECT_TYPE				/* type */[m
 } GrantObjectType;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct GrantStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct GrantStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1774,9 +1824,10 @@[m [mtypedef struct XXX {[m
  * function.  So it is sufficient to identify an existing function, but it[m
  * is not enough info to define a function nor to call it.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct FuncWithArgs : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct FuncWithArgs {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1791,9 +1842,10 @@[m [mtypedef struct XXX {[m
  * Note that simple "ALL PRIVILEGES" is represented as a NIL list, not[m
  * an AccessPriv with both fields null.[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AccessPriv : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AccessPriv {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1810,9 +1862,10 @@[m [mtypedef struct XXX {[m
  * of role names, as Value strings.[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct GrantRoleStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct GrantRoleStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1828,9 +1881,10 @@[m [mtypedef struct XXX {[m
  *	Alter Default Privileges Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterDefaultPrivilegesStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterDefaultPrivilegesStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1846,9 +1900,10 @@[m [mtypedef struct XXX {[m
  * and "query" must be non-NULL.[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CopyStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CopyStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1879,9 +1934,10 @@[m [mtypedef enum[m
 	VAR_RESET_ALL				/* RESET ALL */[m
 } VariableSetKind;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct VariableSetStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct VariableSetStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1895,9 +1951,10 @@[m [mtypedef struct XXX {[m
  * Show Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct VariableShowStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct VariableShowStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1915,9 +1972,10 @@[m [mtypedef struct XXX {[m
  * ----------------------[m
  */[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -1993,9 +2051,10 @@[m [mtypedef enum ConstrType			/* types of constraints */[m
 #define FKCONSTR_MATCH_PARTIAL		'p'[m
 #define FKCONSTR_MATCH_SIMPLE		's'[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct Constraint : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct Constraint {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2046,9 +2105,10 @@[m [mtypedef struct XXX {[m
  * ----------------------[m
  */[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateTableSpaceStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateTableSpaceStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2058,9 +2118,10 @@[m [mtypedef struct XXX {[m
 	List	   *options;[m
 } CreateTableSpaceStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DropTableSpaceStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DropTableSpaceStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2068,9 +2129,10 @@[m [mtypedef struct XXX {[m
 	bool		missing_ok;		/* skip error if missing? */[m
 } DropTableSpaceStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterTableSpaceOptionsStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterTableSpaceOptionsStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2079,9 +2141,10 @@[m [mtypedef struct XXX {[m
 	bool		isReset;[m
 } AlterTableSpaceOptionsStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterTableMoveAllStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterTableMoveAllStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2097,9 +2160,10 @@[m [mtypedef struct XXX {[m
  * ----------------------[m
  */[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateExtensionStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateExtensionStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2109,9 +2173,10 @@[m [mtypedef struct XXX {[m
 } CreateExtensionStmt;[m
 [m
 /* Only used for ALTER EXTENSION UPDATE; later might need an action field */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterExtensionStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterExtensionStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2119,9 +2184,10 @@[m [mtypedef struct XXX {[m
 	List	   *options;		/* List of DefElem nodes */[m
 } AlterExtensionStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterExtensionContentsStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterExtensionContentsStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2137,9 +2203,10 @@[m [mtypedef struct XXX {[m
  * ----------------------[m
  */[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateFdwStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateFdwStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2148,9 +2215,10 @@[m [mtypedef struct XXX {[m
 	List	   *options;		/* generic options to FDW */[m
 } CreateFdwStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterFdwStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterFdwStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2164,9 +2232,10 @@[m [mtypedef struct XXX {[m
  * ----------------------[m
  */[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateForeignServerStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateForeignServerStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2177,9 +2246,10 @@[m [mtypedef struct XXX {[m
 	List	   *options;		/* generic options to server */[m
 } CreateForeignServerStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterForeignServerStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterForeignServerStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2206,9 +2276,10 @@[m [mtypedef struct CreateForeignTableStmt[m
  * ----------------------[m
  */[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateUserMappingStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateUserMappingStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2217,9 +2288,10 @@[m [mtypedef struct XXX {[m
 	List	   *options;		/* generic options to server */[m
 } CreateUserMappingStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterUserMappingStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterUserMappingStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2228,9 +2300,10 @@[m [mtypedef struct XXX {[m
 	List	   *options;		/* generic options to server */[m
 } AlterUserMappingStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DropUserMappingStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DropUserMappingStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2251,9 +2324,10 @@[m [mtypedef enum ImportForeignSchemaType[m
 	FDW_IMPORT_SCHEMA_EXCEPT	/* exclude listed tables from import */[m
 } ImportForeignSchemaType;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ImportForeignSchemaStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ImportForeignSchemaStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2269,9 +2343,10 @@[m [mtypedef struct XXX {[m
  *		Create POLICY Statement[m
  *----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreatePolicyStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreatePolicyStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2287,9 +2362,10 @@[m [mtypedef struct XXX {[m
  *		Alter POLICY Statement[m
  *----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterPolicyStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterPolicyStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2304,9 +2380,10 @@[m [mtypedef struct XXX {[m
  *		Create TRIGGER Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateTrigStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateTrigStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2332,9 +2409,10 @@[m [mtypedef struct XXX {[m
  *		Create EVENT TRIGGER Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateEventTrigStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateEventTrigStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2348,9 +2426,10 @@[m [mtypedef struct XXX {[m
  *		Alter EVENT TRIGGER Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterEventTrigStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterEventTrigStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2364,9 +2443,10 @@[m [mtypedef struct XXX {[m
  *		Create PROCEDURAL LANGUAGE Statements[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreatePLangStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreatePLangStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2394,9 +2474,10 @@[m [mtypedef enum RoleStmtType[m
 	ROLESTMT_GROUP[m
 } RoleStmtType;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateRoleStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateRoleStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2405,9 +2486,10 @@[m [mtypedef struct XXX {[m
 	List	   *options;		/* List of DefElem nodes */[m
 } CreateRoleStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterRoleStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterRoleStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2416,9 +2498,10 @@[m [mtypedef struct XXX {[m
 	int			action;			/* +1 = add members, -1 = drop members */[m
 } AlterRoleStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterRoleSetStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterRoleSetStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2427,9 +2510,10 @@[m [mtypedef struct XXX {[m
 	VariableSetStmt *setstmt;	/* SET or RESET subcommand */[m
 } AlterRoleSetStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DropRoleStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DropRoleStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2442,9 +2526,10 @@[m [mtypedef struct XXX {[m
  * ----------------------[m
  */[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateSeqStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateSeqStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2454,9 +2539,10 @@[m [mtypedef struct XXX {[m
 	bool		if_not_exists;	/* just do nothing if it already exists? */[m
 } CreateSeqStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterSeqStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterSeqStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2469,9 +2555,10 @@[m [mtypedef struct XXX {[m
  *		Create {Aggregate|Operator|Type} Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DefineStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DefineStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2486,9 +2573,10 @@[m [mtypedef struct XXX {[m
  *		Create Domain Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateDomainStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateDomainStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2502,9 +2590,10 @@[m [mtypedef struct XXX {[m
  *		Create Operator Class Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateOpClassStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateOpClassStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2520,9 +2609,10 @@[m [mtypedef struct XXX {[m
 #define OPCLASS_ITEM_FUNCTION		2[m
 #define OPCLASS_ITEM_STORAGETYPE	3[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateOpClassItem : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateOpClassItem {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2541,9 +2631,10 @@[m [mtypedef struct XXX {[m
  *		Create Operator Family Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateOpFamilyStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateOpFamilyStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2555,9 +2646,10 @@[m [mtypedef struct XXX {[m
  *		Alter Operator Family Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterOpFamilyStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterOpFamilyStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2572,9 +2664,10 @@[m [mtypedef struct XXX {[m
  * ----------------------[m
  */[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DropStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DropStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2590,9 +2683,10 @@[m [mtypedef struct XXX {[m
  *				Truncate Table Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct TruncateStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct TruncateStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2605,9 +2699,10 @@[m [mtypedef struct XXX {[m
  *				Comment On Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CommentStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CommentStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2621,9 +2716,10 @@[m [mtypedef struct XXX {[m
  *				SECURITY LABEL Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct SecLabelStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct SecLabelStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2652,9 +2748,10 @@[m [mtypedef struct XXX {[m
 #define CURSOR_OPT_GENERIC_PLAN 0x0040	/* force use of generic plan */[m
 #define CURSOR_OPT_CUSTOM_PLAN	0x0080	/* force use of custom plan */[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DeclareCursorStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DeclareCursorStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2667,9 +2764,10 @@[m [mtypedef struct XXX {[m
  *		Close Portal Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ClosePortalStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ClosePortalStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2693,9 +2791,10 @@[m [mtypedef enum FetchDirection[m
 [m
 #define FETCH_ALL	LONG_MAX[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct FetchStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct FetchStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2716,9 +2815,10 @@[m [mtypedef struct XXX {[m
  * properties are empty.[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct IndexStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct IndexStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2747,9 +2847,10 @@[m [mtypedef struct XXX {[m
  *		Create Function Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateFunctionStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateFunctionStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2771,9 +2872,10 @@[m [mtypedef enum FunctionParameterMode[m
 	FUNC_PARAM_TABLE = 't'		/* table function output column */[m
 } FunctionParameterMode;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct FunctionParameter : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct FunctionParameter {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2783,9 +2885,10 @@[m [mtypedef struct XXX {[m
 	Node	   *defexpr;		/* raw default expr, or NULL if not given */[m
 } FunctionParameter;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterFunctionStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterFunctionStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2799,18 +2902,20 @@[m [mtypedef struct XXX {[m
  * DoStmt is the raw parser output, InlineCodeBlock is the execution-time API[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DoStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DoStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
 	List	   *args;			/* List of DefElem nodes */[m
 } DoStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct InlineCodeBlock : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct InlineCodeBlock {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2823,9 +2928,10 @@[m [mtypedef struct XXX {[m
  *		Alter Object Rename Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct RenameStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct RenameStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2845,9 +2951,10 @@[m [mtypedef struct XXX {[m
  *		ALTER object SET SCHEMA Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterObjectSchemaStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterObjectSchemaStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2863,9 +2970,10 @@[m [mtypedef struct XXX {[m
  *		Alter Object Owner Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterOwnerStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterOwnerStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2881,9 +2989,10 @@[m [mtypedef struct XXX {[m
  *		Create Rule Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct RuleStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct RuleStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2900,9 +3009,10 @@[m [mtypedef struct XXX {[m
  *		Notify Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct NotifyStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct NotifyStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2914,9 +3024,10 @@[m [mtypedef struct XXX {[m
  *		Listen Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ListenStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ListenStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2927,9 +3038,10 @@[m [mtypedef struct XXX {[m
  *		Unlisten Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct UnlistenStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct UnlistenStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2954,9 +3066,10 @@[m [mtypedef enum TransactionStmtKind[m
 	TRANS_STMT_ROLLBACK_PREPARED[m
 } TransactionStmtKind;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct TransactionStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct TransactionStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2969,9 +3082,10 @@[m [mtypedef struct XXX {[m
  *		Create Type Statement, composite types[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CompositeTypeStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CompositeTypeStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2983,9 +3097,10 @@[m [mtypedef struct XXX {[m
  *		Create Type Statement, enum types[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateEnumStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateEnumStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -2997,9 +3112,10 @@[m [mtypedef struct XXX {[m
  *		Create Type Statement, range types[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateRangeStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateRangeStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3011,9 +3127,10 @@[m [mtypedef struct XXX {[m
  *		Alter Type Statement, enum types[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterEnumStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterEnumStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3035,9 +3152,10 @@[m [mtypedef enum ViewCheckOption[m
 	CASCADED_CHECK_OPTION[m
 } ViewCheckOption;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ViewStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ViewStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3053,9 +3171,10 @@[m [mtypedef struct XXX {[m
  *		Load Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct LoadStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct LoadStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3066,9 +3185,10 @@[m [mtypedef struct XXX {[m
  *		Createdb Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreatedbStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreatedbStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3080,9 +3200,10 @@[m [mtypedef struct XXX {[m
  *	Alter Database[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterDatabaseStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterDatabaseStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3090,9 +3211,10 @@[m [mtypedef struct XXX {[m
 	List	   *options;		/* List of DefElem nodes */[m
 } AlterDatabaseStmt;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterDatabaseSetStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterDatabaseSetStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3104,9 +3226,10 @@[m [mtypedef struct XXX {[m
  *		Dropdb Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DropdbStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DropdbStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3118,9 +3241,10 @@[m [mtypedef struct XXX {[m
  *		Alter System Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterSystemStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterSystemStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3131,9 +3255,10 @@[m [mtypedef struct XXX {[m
  *		Cluster Statement (support pbrown's cluster index implementation)[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ClusterStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ClusterStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3161,9 +3286,10 @@[m [mtypedef enum VacuumOption[m
 	VACOPT_SKIPTOAST = 1 << 6	/* don't process the TOAST table, if any */[m
 } VacuumOption;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct VacuumStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct VacuumStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3180,9 +3306,10 @@[m [mtypedef struct XXX {[m
  * planning of the query are always postponed until execution of EXPLAIN.[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ExplainStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ExplainStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3203,9 +3330,10 @@[m [mtypedef struct XXX {[m
  * can be a SELECT or an EXECUTE, but not other DML statements.[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateTableAsStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateTableAsStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3220,9 +3348,10 @@[m [mtypedef struct XXX {[m
  *		REFRESH MATERIALIZED VIEW Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct RefreshMatViewStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct RefreshMatViewStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3235,9 +3364,10 @@[m [mtypedef struct XXX {[m
  * Checkpoint Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CheckPointStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CheckPointStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3256,9 +3386,10 @@[m [mtypedef enum DiscardMode[m
 	DISCARD_TEMP[m
 } DiscardMode;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DiscardStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DiscardStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3269,9 +3400,10 @@[m [mtypedef struct XXX {[m
  *		LOCK Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct LockStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct LockStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3284,9 +3416,10 @@[m [mtypedef struct XXX {[m
  *		SET CONSTRAINTS Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ConstraintsSetStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ConstraintsSetStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3311,9 +3444,10 @@[m [mtypedef enum ReindexObjectType[m
 	REINDEX_OBJECT_DATABASE	/* database */[m
 } ReindexObjectType;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ReindexStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ReindexStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3327,9 +3461,10 @@[m [mtypedef struct XXX {[m
  *		CREATE CONVERSION Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateConversionStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateConversionStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3344,9 +3479,10 @@[m [mtypedef struct XXX {[m
  *	CREATE CAST Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateCastStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateCastStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3361,9 +3497,10 @@[m [mtypedef struct XXX {[m
  *	CREATE TRANSFORM Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct CreateTransformStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct CreateTransformStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3378,9 +3515,10 @@[m [mtypedef struct XXX {[m
  *		PREPARE Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct PrepareStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct PrepareStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3395,9 +3533,10 @@[m [mtypedef struct XXX {[m
  * ----------------------[m
  */[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ExecuteStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ExecuteStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3410,9 +3549,10 @@[m [mtypedef struct XXX {[m
  *		DEALLOCATE Statement[m
  * ----------------------[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DeallocateStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DeallocateStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3423,9 +3563,10 @@[m [mtypedef struct XXX {[m
 /*[m
  *		DROP OWNED statement[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct DropOwnedStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct DropOwnedStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3436,9 +3577,10 @@[m [mtypedef struct XXX {[m
 /*[m
  *		REASSIGN OWNED statement[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct ReassignOwnedStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct ReassignOwnedStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3449,9 +3591,10 @@[m [mtypedef struct XXX {[m
 /*[m
  * TS Dictionary stmts: DefineStmt, RenameStmt and DropStmt are default[m
  */[m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterTSDictionaryStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterTSDictionaryStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
[36m@@ -3471,9 +3614,10 @@[m [mtypedef enum AlterTSConfigType[m
 	ALTER_TSCONFIG_DROP_MAPPING[m
 } AlterTSConfigType;[m
 [m
[32m+[m[32m#ifdef __cplusplus[m
 typedef struct AlterTSConfigurationStmt : Node {[m
 #else[m
[31m-typedef struct XXX {[m
[32m+[m[32mtypedef struct AlterTSConfigurationStmt {[m
 	NodeTag		type;[m
 #endif[m
 [m
