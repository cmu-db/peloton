//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_comparator.cpp
//
// Identification: src/codegen/plan_comparator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "codegen/plan_comparator.h"
#include "storage/database.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"


namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Compare two plans
//===----------------------------------------------------------------------===//
int PlanComparator::Compare(const planner::AbstractPlan &A, const planner::AbstractPlan &B) {
  PlanNodeType nodeTypeA = A.GetPlanNodeType();
  PlanNodeType nodeTypeB = B.GetPlanNodeType();
  if (nodeTypeA != nodeTypeB)
    return (nodeTypeA < nodeTypeB)? -1:1;
  switch (nodeTypeA) {
  case (PlanNodeType::SEQSCAN):
    return CompareSeqScan((planner::SeqScanPlan &) A, (planner::SeqScanPlan &) B);
  case (PlanNodeType::ORDERBY):
    return CompareOrderBy((planner::OrderByPlan &) A, (planner::OrderByPlan &) B);
  case (PlanNodeType::AGGREGATE_V2):
    return CompareAggregate((planner::AggregatePlan &) A, (planner::AggregatePlan &) B);
  case (PlanNodeType::HASH):
    return CompareHash((planner::HashPlan &) A, (planner::HashPlan &) B);
  case (PlanNodeType::HASHJOIN):
    return CompareHashJoin((planner::HashJoinPlan &) A, (planner::HashJoinPlan &) B);
  default:
    LOG_DEBUG("Plan type not supported");
    return -1;
  }
}

//===----------------------------------------------------------------------===//
// Compare function for SeqScanPlan
// Return: A < B: -1, A = B: 0, A > B: 1
//===----------------------------------------------------------------------===//
int PlanComparator::CompareSeqScan(const planner::SeqScanPlan & A, const planner::SeqScanPlan & B) {
  // compare table
  storage::DataTable *database_ptr_A = A.GetTable();
  storage::DataTable *database_ptr_B = B.GetTable();
  if (database_ptr_A != database_ptr_B) {
    LOG_DEBUG("table not equal A:%p, B:%p", (void *)database_ptr_A, (void *)database_ptr_B);
    return (database_ptr_A < database_ptr_B) ? -1 : 1;
  }

  //compare predicate
  int predicate_comp = ExpressionComparator::Compare(A.GetPredicate(), B.GetPredicate());
  if (predicate_comp != 0) {
    LOG_DEBUG("predicate not equal");
    return predicate_comp;
  }

  //compare column_ids
  size_t column_id_count_A = A.GetColumnIds().size();
  size_t column_id_count_B = B.GetColumnIds().size();
  if (column_id_count_A != column_id_count_B)
    return (column_id_count_A < column_id_count_B) ? -1:1;
  for (size_t it = 0; it < column_id_count_A; it++) {
    oid_t col_id_A = A.GetColumnIds()[it];
    oid_t col_id_B = B.GetColumnIds()[it];
    if (col_id_A != col_id_B) {
      LOG_DEBUG("column id not equal");
      return (col_id_A < col_id_B) ? -1 : 1;
    }
  }

  //compare is_for_update
  bool is_for_update_A = A.IsForUpdate();
  bool is_for_update_B = A.IsForUpdate();
  if (is_for_update_A != is_for_update_B) {
    LOG_DEBUG("is_for_update not equal");
    return (is_for_update_A < is_for_update_B) ? -1 : 1;
  }
  return CompareChildren(A, B);
}



//===----------------------------------------------------------------------===//
// Compare function for OrderByPlan
// Return: A < B: -1, A = B: 0, A > B: 1
//===----------------------------------------------------------------------===//
int PlanComparator::CompareOrderBy(const planner::OrderByPlan & A, const planner::OrderByPlan & B) {

  //compare sort_keys
  size_t sort_keys_count_A = A.GetSortKeys().size();
  size_t sort_keys_count_B = B.GetSortKeys().size();
  if (sort_keys_count_A != sort_keys_count_B)
    return (sort_keys_count_A < sort_keys_count_B) ? -1:1;

  for (size_t it = 0; it < sort_keys_count_A; it++) {
    oid_t sort_key_A = A.GetSortKeys()[it];
    oid_t sort_key_B = B.GetSortKeys()[it];
    if (sort_key_A != sort_key_B)
      return (sort_key_A < sort_key_B) ? -1:1;
  }

  //compare descend_flags
  size_t descend_flags_count_A = A.GetDescendFlags().size();
  size_t descend_flags_count_B = B.GetDescendFlags().size();
  if (descend_flags_count_A != descend_flags_count_B)
    return (descend_flags_count_A < descend_flags_count_B) ? -1:1;

  for (size_t it = 0; it < descend_flags_count_A; it++) {
    bool descend_flag_A = A.GetDescendFlags()[it];
    bool descend_flag_B = B.GetDescendFlags()[it];
    if (descend_flag_A != descend_flag_B)
      return (descend_flag_A < descend_flag_B) ? -1:1;
  }

  //compare output_column_ids
  size_t column_id_count_A = A.GetOutputColumnIds().size();
  size_t column_id_count_B = B.GetOutputColumnIds().size();
  if (column_id_count_A != column_id_count_B)
    return (column_id_count_A < column_id_count_B) ? -1:1;

  for (size_t it = 0; it < column_id_count_A; it++) {
    oid_t col_id_A = A.GetOutputColumnIds()[it];
    oid_t col_id_B = B.GetOutputColumnIds()[it];
    if (col_id_A != col_id_B)
      return (col_id_A < col_id_B) ? -1:1;
  }

  return CompareChildren(A, B);
}

//===----------------------------------------------------------------------===//
// Compare function for AggregatePlan
// Return: A < B: -1, A = B: 0, A > B: 1
//===----------------------------------------------------------------------===//
int PlanComparator::CompareAggregate(const planner::AggregatePlan & A, const planner::AggregatePlan & B) {

  //compare project_info
  int project_info_comp = CompareProjectInfo(A.GetProjectInfo(), B.GetProjectInfo());
  if (project_info_comp != 0)
    return project_info_comp;

  //compare predicate
  int predicate_comp = ExpressionComparator::Compare(A.GetPredicate(), B.GetPredicate());
  if (predicate_comp != 0)
    return predicate_comp;

  //compare unique_agg_term
  int aggtype_comp = CompareAggType(A.GetUniqueAggTerms(), B.GetUniqueAggTerms());
  if (aggtype_comp != 0)
    return aggtype_comp;

  //compare groupby_col_ids
  if (A.GetGroupbyColIds().size() != B.GetGroupbyColIds().size())
    return (A.GetGroupbyColIds().size() < B.GetGroupbyColIds().size())?-1:1;
  for (size_t i = 0; i < A.GetGroupbyColIds().size(); i++) {
    if (A.GetGroupbyColIds().at(i) != B.GetGroupbyColIds().at(i))
      return (A.GetGroupbyColIds().at(i) < B.GetGroupbyColIds().at(i))?-1:1;
  }

  //compare output_schema
  int schema_comp = CompareSchema(*A.GetOutputSchema(), *B.GetOutputSchema());
  if (schema_comp != 0)
    return schema_comp;

  //compare aggregate_strategy
  if (A.GetAggregateStrategy() != B.GetAggregateStrategy())
    return (A.GetAggregateStrategy() < B.GetAggregateStrategy())?-1:1;

  return CompareChildren(A, B);
}

//===----------------------------------------------------------------------===//
// Compare function for HashPlan
// Return: A < B: -1, A = B: 0, A > B: 1
//===----------------------------------------------------------------------===//
int PlanComparator::CompareHash(const planner::HashPlan & A, const planner::HashPlan & B) {
  if (A.GetHashKeys().size() != B.GetHashKeys().size())
    return (A.GetHashKeys().size() < B.GetHashKeys().size())?-1:1;
  for (size_t i = 0; i < A.GetHashKeys().size(); i++) {
    int ret = ExpressionComparator::Compare(A.GetHashKeys().at(i).get(), B.GetHashKeys().at(i).get());
    if (ret != 0)
      return ret;
  }
  return CompareChildren(A, B);
}

//===----------------------------------------------------------------------===//
// Compare function for HashJoinPlan
// Return: A < B: -1, A = B: 0, A > B: 1
//===----------------------------------------------------------------------===//
int PlanComparator::CompareHashJoin(const planner::HashJoinPlan & A, const planner::HashJoinPlan & B) {
  //compare join_type
  if (A.GetJoinType() != B.GetJoinType())
    return (A.GetJoinType() < B.GetJoinType())?-1:1;
  //compare predicate
  int predicate_comp = ExpressionComparator::Compare(A.GetPredicate(), B.GetPredicate());
  if (predicate_comp != 0)
    return predicate_comp;
  //compare proj_info
  int project_info_comp = CompareProjectInfo(A.GetProjInfo(), B.GetProjInfo());
  if (project_info_comp != 0)
    return project_info_comp;

  //compare proj_schema
  int schema_comp = CompareSchema(*A.GetSchema(), *B.GetSchema());
  if (schema_comp != 0)
    return schema_comp;

  std::vector<const expression::AbstractExpression *> keys_A, keys_B;
  A.GetLeftHashKeys(keys_A);
  B.GetLeftHashKeys(keys_B);
  if (keys_A.size() != keys_B.size())
    return keys_A.size() < keys_B.size();
  for (size_t i = 0; i < keys_A.size(); i++) {
    int ret = ExpressionComparator::Compare(keys_A.at(i), keys_B.at(i));
    if (ret != 0)
      return ret;
  }

  keys_A.clear();
  keys_B.clear();
  A.GetRightHashKeys(keys_A);
  A.GetRightHashKeys(keys_B);
  if (keys_A.size() != keys_B.size())
    return keys_A.size() < keys_B.size();
  for (size_t i = 0; i < keys_A.size(); i++) {
    int ret = ExpressionComparator::Compare(keys_A.at(i), keys_B.at(i));
    if (ret != 0)
      return ret;
  }
  return CompareChildren(A, B);
}

//===----------------------------------------------------------------------===//
// Compare two plans' children
//===----------------------------------------------------------------------===//
int PlanComparator::CompareChildren(const planner::AbstractPlan & A, const planner::AbstractPlan & B) {
  if (A.GetChildren().size() != B.GetChildren().size())
    return (A.GetChildren().size() < B.GetChildren().size())?-1:1;
  for (size_t i = 0; i < A.GetChildren().size(); i++) {
    int ret = Compare(*A.GetChild(i), *B.GetChild(i));
    if (ret != 0)
      return ret;
  }
  return 0;
}


//===----------------------------------------------------------------------===//
// Compare function for catalog::Schema, used in comparing AggregatePlan and
// HashjoinPlan
//===----------------------------------------------------------------------===//
int PlanComparator::CompareSchema(const catalog::Schema & A, const catalog::Schema & B) {
  if (A.GetColumnCount() != B.GetColumnCount())
    return (A.GetColumnCount() < B.GetColumnCount())?-1:1;
  if (A.GetUninlinedColumnCount() != B.GetUninlinedColumnCount())
    return (A.GetUninlinedColumnCount() < B.GetUninlinedColumnCount())?-1:1;
  if (A.IsInlined() != B.IsInlined())
    return (A.IsInlined() < B.IsInlined())?-1:1;

  for (oid_t column_itr = 0; column_itr < A.GetColumnCount(); column_itr++) {
    const catalog::Column &column_info_A = A.GetColumn(column_itr);
    const catalog::Column &column_info_B = B.GetColumn(column_itr);
    if (column_info_A.GetType() != column_info_B.GetType())
      return (column_info_A.GetType() < column_info_B.GetType())?-1:1;
    if (column_info_A.is_inlined != column_info_B.is_inlined)
      return (column_info_A.is_inlined < column_info_B.is_inlined)?-1:1;
  }
  return 0;
}


//===----------------------------------------------------------------------===//
// Compare function for AggTerm, used in comparing AggregatePlan
//===----------------------------------------------------------------------===//
int PlanComparator::CompareAggType(const std::vector<planner::AggregatePlan::AggTerm> & A,
                                   const std::vector<planner::AggregatePlan::AggTerm> & B) {
  if (A.size() != B.size())
    return (A.size() < B.size())?-1:1;
  for (size_t i = 0; i < A.size(); i++) {
    if (A.at(i).aggtype != B.at(i).aggtype)
      return (A.at(i).aggtype < B.at(i).aggtype)?-1:1;
    int ret = ExpressionComparator::Compare(A.at(i).expression, B.at(i).expression);
    if (ret != 0)
      return ret;
    if (A.at(i).distinct != B.at(i).distinct)
      return (A.at(i).distinct < B.at(i).distinct)?-1:1;
  }
  return 0;
}

//===----------------------------------------------------------------------===//
// Compare function for DerivedAttribute, used in comparing ProjectInfo
//===----------------------------------------------------------------------===//
int PlanComparator::CompareDerivedAttr(const planner::DerivedAttribute & A, const planner::DerivedAttribute & B) {
  if (A.attribute_info.type != B.attribute_info.type)
    return (A.attribute_info.type < B.attribute_info.type)?-1:1;
  if (A.attribute_info.attribute_id != B.attribute_info.attribute_id)
    return (A.attribute_info.attribute_id < B.attribute_info.attribute_id)?-1:1;
  return ExpressionComparator::Compare(A.expr, B.expr);
}

//===----------------------------------------------------------------------===//
// Compare function for ProjectInfo, used in comparing DerivedAttr
//===----------------------------------------------------------------------===//
int PlanComparator::CompareProjectInfo(const planner::ProjectInfo * A, const planner::ProjectInfo * B) {

  //compare TargetList
  if (A->GetTargetList().size() != B->GetTargetList().size())
    return (A->GetTargetList().size() < B->GetTargetList().size())?-1:1;
  for (size_t i = 0; i < A->GetTargetList().size(); i++) {
    if (A->GetTargetList().at(i).first != B->GetTargetList().at(i).first)
      return (A->GetTargetList().at(i).first < B->GetTargetList().at(i).first)?-1:1;
    int derived_comp = CompareDerivedAttr(A->GetTargetList().at(i).second, B->GetTargetList().at(i).second);
    if (derived_comp != 0)
      return derived_comp;
  }
  //compare DirectMapList
  if (A->GetDirectMapList().size() != B->GetDirectMapList().size())
    return (A->GetDirectMapList().size() < B->GetDirectMapList().size())?-1:1;
  for (size_t i = 0; i < A->GetDirectMapList().size(); i++) {
    if (A->GetDirectMapList().at(i).first != B->GetDirectMapList().at(i).first)
      return (A->GetDirectMapList().at(i).first < B->GetDirectMapList().at(i).first)?-1:1;
    if (A->GetDirectMapList().at(i).second.first != B->GetDirectMapList().at(i).second.first)
      return (A->GetDirectMapList().at(i).second.first < B->GetDirectMapList().at(i).second.first)?-1:1;
    if (A->GetDirectMapList().at(i).second.second != B->GetDirectMapList().at(i).second.second)
      return (A->GetDirectMapList().at(i).second.second < B->GetDirectMapList().at(i).second.second)?-1:1;
  }
  return 0;
}

//===----------------------------------------------------------------------===//
// Compare two expressions' children
//===----------------------------------------------------------------------===//
int ExpressionComparator::CompareChildren(const expression::AbstractExpression * A,
                                          const expression::AbstractExpression * B) {
  if (A->GetChildrenSize() != B->GetChildrenSize())
    return (A->GetChildrenSize() < B->GetChildrenSize())?-1:1;
  for (size_t i = 0; i < A->GetChildrenSize(); i++) {
    int ret = CompareChildren(A->GetChild(i), B->GetChild(i));
    if (ret != 0)
      return ret;
  }
  return 0;
}


//===----------------------------------------------------------------------===//
// Compare two expressions
//===----------------------------------------------------------------------===//
int ExpressionComparator::Compare(const expression::AbstractExpression * A, const expression::AbstractExpression * B) {
  if (A == nullptr && B == nullptr)
    return 0;
  if (A == nullptr)
    return 1;
  if (B == nullptr)
    return -1;

  ExpressionType nodeTypeA = A->GetExpressionType();
  ExpressionType nodeTypeB = B->GetExpressionType();

  if (nodeTypeA != nodeTypeB)
    return (nodeTypeA < nodeTypeB) ? -1:1;
  switch (nodeTypeA) {
  case (ExpressionType::COMPARE_EQUAL):
  case (ExpressionType::COMPARE_NOTEQUAL):
  case (ExpressionType::COMPARE_LESSTHAN):
  case (ExpressionType::COMPARE_GREATERTHAN):
  case (ExpressionType::COMPARE_LESSTHANOREQUALTO):
  case (ExpressionType::COMPARE_GREATERTHANOREQUALTO):
    return  CompareChildren(A, B); // ComparisonExpression
  case (ExpressionType::OPERATOR_NOT):
  case (ExpressionType::OPERATOR_PLUS):
  case (ExpressionType::OPERATOR_MINUS):
  case (ExpressionType::OPERATOR_MULTIPLY):
  case (ExpressionType::OPERATOR_DIVIDE):
  case (ExpressionType::OPERATOR_MOD): {
    // OperatorExpression
    // compare return_value_type_
    if (A->GetValueType() != B->GetValueType())
      return (A->GetValueType() < B->GetValueType()) ? -1 : 1;
    return CompareChildren(A, B);
  }
  case (ExpressionType::VALUE_CONSTANT): {
    // ConstantValueExpression
    expression::ConstantValueExpression * ptr_A = (expression::ConstantValueExpression *) A;
    expression::ConstantValueExpression * ptr_B = (expression::ConstantValueExpression *) B;
    type::Value va = ptr_A->GetValue();
    type::Value vb = ptr_B->GetValue();
    if (!va.CompareEquals(vb))
      return va.CompareLessThan(vb) ? -1:1;
    return 0;
  }
  case (ExpressionType::OPERATOR_UNARY_MINUS)://OperatorUnaryMinusExpression
    return CompareChildren(A, B);
  case (ExpressionType::AGGREGATE_COUNT):
  case (ExpressionType::AGGREGATE_SUM):
  case (ExpressionType::AGGREGATE_MIN):
  case (ExpressionType::AGGREGATE_MAX):
  case (ExpressionType::AGGREGATE_AVG): {
    // AggregateExpresion
    if (A->distinct_ != B->distinct_)
      return (A->distinct_ < B->distinct_) ? -1 : 1;
    return CompareChildren(A, B);
  }
  case (ExpressionType::CONJUNCTION_AND):
  case (ExpressionType::CONJUNCTION_OR):
    return  CompareChildren(A, B); // ConjunctionExpression
  case (ExpressionType::VALUE_TUPLE): {
    // TupleValueExpression
    expression::TupleValueExpression *ptr_A = (expression::TupleValueExpression *) A;
    expression::TupleValueExpression *ptr_B = (expression::TupleValueExpression *) B;
    //compare tuple_idx
    if (ptr_A->GetTupleId() != ptr_B->GetTupleId())
      return (ptr_A->GetTupleId() < ptr_B->GetTupleId()) ? -1:1;
    //compare column_idx
    if (ptr_A->GetColumnId() != ptr_B->GetColumnId())
      return (ptr_A->GetColumnId() < ptr_B->GetColumnId()) ? -1:1;
    //compare table_name
    if (ptr_A->GetTableName() != ptr_B->GetTableName())
      return ptr_A->GetTableName().compare(ptr_B->GetTableName());
    //compare column_name
    if (ptr_A->GetColumnName() != ptr_B->GetColumnName())
      return ptr_A->GetColumnName().compare(ptr_B->GetColumnName());
    //compare attribute_info
    if (ptr_A->GetAttributeRef()->type != ptr_B->GetAttributeRef()->type)
      return (ptr_A->GetAttributeRef()->type < ptr_B->GetAttributeRef()->type)?-1:1;
    if (ptr_A->GetAttributeRef()->attribute_id != ptr_B->GetAttributeRef()->attribute_id)
      return (ptr_A->GetAttributeRef()->attribute_id < ptr_B->GetAttributeRef()->attribute_id)?-1:1;

    break;
  }
  default:
    return -1;
  }
  return 0;
}

}  // namespace codegen
}  // namespace peloton