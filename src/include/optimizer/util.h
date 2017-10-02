//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// util.h
//
// Identification: src/include/optimizer/util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdlib>
#include <algorithm>
#include <string>

#include "expression/abstract_expression.h"
#include "planner/abstract_plan.h"
#include "parser/copy_statement.h"

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {
class DataTable;
}

namespace optimizer {
namespace util {

inline void to_lower_string(std::string &str) {
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
}

template<class T>
bool IsSubset (std::unordered_set<T>& super_set, std::unordered_set<T>& child_set) {
  for (auto element : child_set) {
    if (super_set.find(element) == super_set.end())
      return false;
  }
  return true;
}

template<class T>
void SetUnion (std::unordered_set<T>& new_set, std::unordered_set<T>& old_set) {
  for (auto element : old_set)
    new_set.insert(element);
}

// get the column IDs evaluated in a predicate
void GetPredicateColumns(const catalog::Schema *schema,
                                expression::AbstractExpression *expression,
                                std::vector<oid_t> &column_ids,
                                std::vector<ExpressionType> &expr_types,
                                std::vector<type::Value> &values,
                                bool &index_searchable);

bool CheckIndexSearchable(storage::DataTable *target_table,
                                 expression::AbstractExpression *expression,
                                 std::vector<oid_t> &key_column_ids,
                                 std::vector<ExpressionType> &expr_types,
                                 std::vector<type::Value> &values,
                                 oid_t &index_id);



void SplitPredicates(
    expression::AbstractExpression* expr,
    std::vector<expression::AbstractExpression*>& predicates);

expression::AbstractExpression* CombinePredicates(
    std::vector<expression::AbstractExpression*> predicates);

void ExtractPredicates(expression::AbstractExpression* expr,
                       SingleTablePredicates& where_predicates,
                       MultiTablePredicates& join_predicates);

expression::AbstractExpression* ConstructJoinPredicate(
    std::unordered_set<std::string>& table_alias_set,
    MultiTablePredicates& join_predicates);

bool ContainsJoinColumns(
    const std::unordered_set<std::string>& l_group_alias,
    const std::unordered_set<std::string>& r_group_alias,
    const expression::AbstractExpression* expr);


std::unique_ptr<planner::AbstractPlan> CreateCopyPlan(std::shared_ptr<parser::CopyStatement> copy_stmt);


} // namespace util
} // namespace optimizer
} // namespace peloton
