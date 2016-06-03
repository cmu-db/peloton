//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_transformer.h
//
// Identification: src/backend/optimizer/query_transformer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/optimizer.h"
#include "backend/optimizer/query_operators.h"

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/params.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Postgres to Peloton QueryTransformer
//===--------------------------------------------------------------------===//

class QueryTransformer {
 public:
  QueryTransformer(const QueryTransformer &) = delete;
  QueryTransformer &operator=(const QueryTransformer &) = delete;
  QueryTransformer(QueryTransformer &&) = delete;
  QueryTransformer &operator=(QueryTransformer &&) = delete;

  QueryTransformer();

  Select *Transform(Query *pg_query);

 private:
  std::vector<Table *> GetJoinNodeTables(QueryJoinNode *expr);

  PelotonJoinType TransformJoinType(const JoinType type);

  Variable *ConvertVar(Var *expr);

  Constant *ConvertConst(Const *expr);

  QueryExpression *ConvertBoolExpr(BoolExpr *expr);

  OperatorExpression *ConvertOpExpr(OpExpr *expr);

  QueryExpression *ConvertPostgresExpression(Node *expr);

  QueryExpression *ConvertPostgresQuals(Node *quals);

  OrderBy *ConvertSortGroupClause(SortGroupClause *sort_clause,
                                  List *targetList);

  Attribute *ConvertTargetEntry(TargetEntry *te);

  Table *ConvertRangeTblEntry(RangeTblEntry *rte);

  QueryJoinNode *ConvertRangeTblRef(RangeTblRef *expr);

  QueryJoinNode *ConvertJoinExpr(JoinExpr *expr);

  QueryJoinNode *ConvertFromTreeNode(Node *node);

  std::pair<QueryJoinNode *, QueryExpression *> ConvertFromExpr(FromExpr *from);

  Select *ConvertQuery(Query *pg_query);

  //===--------------------------------------------------------------------===//
  // Member variables for tracking partial query state
  //===--------------------------------------------------------------------===//

  oid_t database_oid;

  // List of the attributes needed for output
  std::vector<std::unique_ptr<Attribute>> output_list;

  // List of all range table entries in original query rtable
  std::vector<RangeTblEntry*> rte_entries;

  // Current expression trees inner and outer tables
  std::vector<Table*> left_tables;
  std::vector<Table*> right_tables;
};

} /* namespace optimizer */
} /* namespace peloton */
