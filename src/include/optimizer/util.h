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

#include <algorithm>
#include <cstdlib>
#include <string>

#include "expression/abstract_expression.h"
#include "parser/copy_statement.h"
#include "planner/abstract_plan.h"

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {
class DataTable;
}

namespace optimizer {
namespace util {

  /**
   * @brief Convert upper case letters into lower case in a string
   *
   * @param str The string to operate on
   */
inline void to_lower_string(std::string &str) {
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
}

/**
 * @brief Check if a hashset is a subset of the other one
 *
 * @param super_set The potential super set
 * @param child_set The potential child set
 *
 * @return True if the second set is a subset of the first one
 */
template <class T>
bool IsSubset(const std::unordered_set<T> &super_set,
              const std::unordered_set<T> &child_set) {
  for (auto element : child_set) {
    if (super_set.find(element) == super_set.end()) return false;
  }
  return true;
}

/**
 * @brief Perform union operation on 2 hashsets
 *
 * @param new_set The first set, will hold the result after the union operation
 * @param old_set the other set
 */
template <class T>
void SetUnion(std::unordered_set<T> &new_set, std::unordered_set<T> &old_set) {
  for (auto element : old_set) new_set.insert(element);
}

/**
 * @breif Split conjunction expression tree into a vector of expressions with
 * AND
 */
void SplitPredicates(expression::AbstractExpression *expr,
                     std::vector<expression::AbstractExpression *> &predicates);

/**
 * @breif Combine a vector of expressions with AND
 */
expression::AbstractExpression *CombinePredicates(
    std::vector<std::shared_ptr<expression::AbstractExpression>> predicates);

/**
 * @breif Combine a vector of expressions with AND
 */
expression::AbstractExpression *CombinePredicates(
    std::vector<AnnotatedExpression> predicates);

/**
 * @brief Extract single table precates and multi-table predicates from the expr
 *
 * @param expr The original predicate
 * @param annotated_predicates The extracted conjunction predicates
 */
std::vector<AnnotatedExpression> ExtractPredicates(
    expression::AbstractExpression *expr,
    std::vector<AnnotatedExpression> annotated_predicates = {});

/**
 * @breif Construct a qualified join predicate (contains a subset of alias in
 *  the table_alias_set)
 *  and remove the multitable expressions in the original join_predicates
 *
 * @return The final join predicate
 */
expression::AbstractExpression *ConstructJoinPredicate(
    std::unordered_set<std::string> &table_alias_set,
    MultiTablePredicates &join_predicates);


/**
 * @breif Check if there are any join columns in the join expression
 *  For example, expr = (expr_1) AND (expr_2) AND (expr_3)
 *  we only extract expr_i that have the format (l_table.a = r_table.b)
 *  i.e. expr that is equality check for tuple columns from both of
 *  the underlying tables
 */
bool ContainsJoinColumns(const std::unordered_set<std::string> &l_group_alias,
                         const std::unordered_set<std::string> &r_group_alias,
                         const expression::AbstractExpression *expr);

/**
 * @brief Create a copy plan based on the copy statement
 */
std::unique_ptr<planner::AbstractPlan> CreateCopyPlan(
    parser::CopyStatement *copy_stmt);

/**
 * @brief Construct the map from subquery column name to the actual expression
 *  at the subquery level, for example SELECT a FROM (SELECT a + b as a FROM
 *  test), we'll build the map {"a" -> a + b}
 *
 * @param select_list The select list of a subquery
 *
 * @return The mapping mentioned above
 */
std::unordered_map<std::string, std::shared_ptr<expression::AbstractExpression>>
ConstructSelectElementMap(
    std::vector<std::unique_ptr<expression::AbstractExpression>> &select_list);

/**
 * @brief Walk a predicate, transform the tuple value expression into the actual
 *  expression in the sub-query
 *
 * @param alias_to_expr_map The column name to expression map
 * @param expr the predicate
 *
 * @return the transformed predicate
 */
expression::AbstractExpression *TransformQueryDerivedTablePredicates(
    const std::unordered_map<std::string,
                             std::shared_ptr<expression::AbstractExpression>>
        &alias_to_expr_map,
    expression::AbstractExpression *expr);

/**
 * @brief Walk through a set of join predicates, generate join keys base on the
 *  left/right table aliases set
 */
void ExtractEquiJoinKeys(
    const std::vector<AnnotatedExpression> join_predicates,
    std::vector<std::unique_ptr<expression::AbstractExpression>> &left_keys,
    std::vector<std::unique_ptr<expression::AbstractExpression>> &right_keys,
    const std::unordered_set<std::string> &left_alias,
    const std::unordered_set<std::string> &right_alias);

}  // namespace util
}  // namespace optimizer
}  // namespace peloton
