//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost.cpp
//
// Identification: src/optimizer/stats/cost.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats/cost.h"
#include "expression/comparison_expression.h"
#include "expression/tuple_value_expression.h"
#include "optimizer/stats/selectivity.h"
#include "type/value.h"

#include <cmath>

namespace peloton {
namespace optimizer {
//
// //===----------------------------------------------------------------------===//
// // Scan
// //===----------------------------------------------------------------------===//
// double Cost::SingleConditionSeqScanCost(
//     const std::shared_ptr<TableStats> &input_stats,
//     const ValueCondition &condition,
//     std::shared_ptr<TableStats> &output_stats) {
//   // PELOTON_ASSERT(input_stats != nullptr);
//   // PELOTON_ASSERT(condition != nullptr);
//
//   UpdateConditionStats(input_stats, condition, output_stats);
//
//   return input_stats->num_rows * DEFAULT_TUPLE_COST;
// }
//
// double Cost::SingleConditionIndexScanCost(
//     const std::shared_ptr<TableStats> &input_stats,
//     const ValueCondition &condition,
//     std::shared_ptr<TableStats> &output_stats) {
//   double index_height = default_index_height(input_stats->num_rows);
//   double index_cost = index_height * DEFAULT_INDEX_TUPLE_COST;
//
//   double selectivity = Selectivity::ComputeSelectivity(input_stats, condition);
//   double scan_cost = selectivity * DEFAULT_TUPLE_COST;
//
//   UpdateConditionStats(input_stats, condition, output_stats);
//
//   return index_cost + scan_cost;
// }
//
// void Cost::CombineConjunctionStats(const std::shared_ptr<TableStats> &lhs,
//                                    const std::shared_ptr<TableStats> &rhs,
//                                    const size_t num_rows,
//                                    const ExpressionType type,
//                                    std::shared_ptr<TableStats> &output_stats) {
//   PELOTON_ASSERT(lhs != nullptr);
//   PELOTON_ASSERT(rhs != nullptr);
//   PELOTON_ASSERT(num_rows > 0);
//
//   size_t num_tuples = 1;
//   double sel1 = lhs->num_rows / static_cast<double>(num_rows);
//   double sel2 = rhs->num_rows / static_cast<double>(num_rows);
//   LOG_TRACE("Conjunction sel1[%f] sel2[%f]", sel1, sel2);
//   switch (type) {
//     case ExpressionType::CONJUNCTION_AND:
//       // (sel1 * sel2) * num_rows
//       num_tuples = static_cast<size_t>(num_rows * sel1 * sel2);
//       break;
//     case ExpressionType::CONJUNCTION_OR:
//       // (sel1 + sel2 - sel1 * sel2) * num_rows
//       num_tuples = static_cast<size_t>((sel1 + sel2 - sel1 * sel2) * num_rows);
//       break;
//     default:
//       LOG_WARN("Cost model conjunction on expression type %s not supported",
//                ExpressionTypeToString(type).c_str());
//   }
//   if (output_stats != nullptr) {
//     output_stats->num_rows = num_tuples;
//   }
// }
//
// //===----------------------------------------------------------------------===//
// // GROUP BY
// //===----------------------------------------------------------------------===//
//
// double Cost::SortGroupByCost(const std::shared_ptr<TableStats> &input_stats,
//                              std::vector<std::string> columns,
//                              std::shared_ptr<TableStats> &output_stats) {
//   PELOTON_ASSERT(input_stats);
//   PELOTON_ASSERT(columns.size() > 0);
//
//   //    if (output_stats != nullptr) {
//   if (false) {
//     output_stats->num_rows = GetEstimatedGroupByRows(input_stats, columns);
//   }
//
//   double cost =
//       default_sorting_cost(input_stats->num_rows) * DEFAULT_TUPLE_COST;
//
//   // Update cost to trivial if first group by column has index.
//   // TODO: use more complicated cost when group by multiple columns when
//   // primary index operator is supported.
//   if (!columns.empty() && input_stats->HasPrimaryIndex(columns[0])) {
//     // underestimation of group by with index.
//     cost = DEFAULT_OPERATOR_COST;
//   }
//
//   return cost;
// }
//
// double Cost::HashGroupByCost(const std::shared_ptr<TableStats> &input_stats,
//                              std::vector<std::string> columns,
//                              std::shared_ptr<TableStats> &output_stats) {
//   PELOTON_ASSERT(input_stats);
//
//   if (output_stats != nullptr) {
//     output_stats->num_rows = GetEstimatedGroupByRows(input_stats, columns);
//   }
//
//   // Directly hash tuple
//   return input_stats->num_rows * DEFAULT_TUPLE_COST;
// }
//
// //===----------------------------------------------------------------------===//
// // DISTINCT
// //===----------------------------------------------------------------------===//
// // TODO: support multiple distinct columns
// // what if the column has index?
// double Cost::DistinctCost(const std::shared_ptr<TableStats> &input_stats,
//                           std::string column_name,
//                           std::shared_ptr<TableStats> &output_stats) {
//   PELOTON_ASSERT(input_stats);
//
//   if (output_stats != nullptr) {
//     // update number of rows to be number of unique element of column
//     output_stats->num_rows = input_stats->GetCardinality(column_name);
//   }
//   return input_stats->num_rows * DEFAULT_TUPLE_COST;
// }
//
// //===----------------------------------------------------------------------===//
// // Project
// //===----------------------------------------------------------------------===//
// double Cost::ProjectCost(const std::shared_ptr<TableStats> &input_stats,
//                          UNUSED_ATTRIBUTE std::vector<oid_t> columns,
//                          std::shared_ptr<TableStats> &output_stats) {
//   PELOTON_ASSERT(input_stats);
//
//   if (output_stats != nullptr) {
//     // update column information for output_stats table
//   }
//
//   return input_stats->num_rows * DEFAULT_TUPLE_COST;
// }
//
// //===----------------------------------------------------------------------===//
// // LIMIT
// //===----------------------------------------------------------------------===//
// double Cost::LimitCost(const std::shared_ptr<TableStats> &input_stats,
//                        size_t limit,
//                        std::shared_ptr<TableStats> &output_stats) {
//   PELOTON_ASSERT(input_stats != nullptr);
//   if (output_stats != nullptr) {
//     output_stats->num_rows = std::max(input_stats->num_rows, limit);
//   }
//   return limit * DEFAULT_TUPLE_COST;
// }
//
// //===----------------------------------------------------------------------===//
// // ORDER BY
// //===----------------------------------------------------------------------===//
// double Cost::OrderByCost(const std::shared_ptr<TableStats> &input_stats,
//                          const std::vector<std::string> &columns,
//                          const std::vector<bool> &orders,
//                          std::shared_ptr<TableStats> &output_stats) {
//   PELOTON_ASSERT(input_stats);
//   // Invalid case.
//   if (columns.size() == 0 || columns.size() != orders.size()) {
//     return DEFAULT_COST;
//   }
//   std::string column = columns[0];
//   bool order = orders[0];  // CmpTrue is ASC, CmpFalse is DESC
//   double cost = DEFAULT_COST;
//   // Special case when first column has index.
//   if (input_stats->HasPrimaryIndex(column)) {
//     if (order) {  // ascending
//       // No cost for order by for now. We might need to take
//       // cardinality of first column into account in the future.
//       cost = DEFAULT_OPERATOR_COST;
//     } else {  // descending
//       // Reverse sequence.
//       cost = input_stats->num_rows * DEFAULT_TUPLE_COST;
//     }
//   } else {
//     cost = default_sorting_cost(input_stats->num_rows) * DEFAULT_TUPLE_COST;
//   }
//   if (output_stats != nullptr) {
//     output_stats->num_rows = input_stats->num_rows;
//     // Also set HasPrimaryIndex for first column to true.
//   }
//   return cost;
// }
//
// //===----------------------------------------------------------------------===//
// // NL JOIN
// //===----------------------------------------------------------------------===//
// double Cost::NLJoinCost(
//     const std::shared_ptr<TableStats> &left_input_stats,
//     const std::shared_ptr<TableStats> &right_input_stats,
//     std::shared_ptr<TableStats> &output_stats,
//     const std::shared_ptr<expression::AbstractExpression> predicate,
//     JoinType join_type, bool enable_sampling) {
//   UpdateJoinOutputStats(left_input_stats, right_input_stats, output_stats,
//                         predicate, join_type, enable_sampling);
//
//   return left_input_stats->num_rows * right_input_stats->num_rows *
//          DEFAULT_TUPLE_COST;
// }
//
// //===----------------------------------------------------------------------===//
// // HASH JOIN
// //===----------------------------------------------------------------------===//
// double Cost::HashJoinCost(
//     const std::shared_ptr<TableStats> &left_input_stats,
//     const std::shared_ptr<TableStats> &right_input_stats,
//     std::shared_ptr<TableStats> &output_stats,
//     const std::shared_ptr<expression::AbstractExpression> predicate,
//     JoinType join_type, bool enable_sampling) {
//   UpdateJoinOutputStats(left_input_stats, right_input_stats, output_stats,
//                         predicate, join_type, enable_sampling);
//   return (left_input_stats->num_rows + right_input_stats->num_rows) *
//          DEFAULT_TUPLE_COST;
// }
//
// //===----------------------------------------------------------------------===//
// // Helper functions
// //===----------------------------------------------------------------------===//
// bool Cost::UpdateJoinOutputStatsWithSampling(
//     const std::shared_ptr<TableStats> &left_input_stats,
//     const std::shared_ptr<TableStats> &right_input_stats,
//     std::shared_ptr<TableStats> &output_stats,
//     const std::string &left_column_name, const std::string &right_column_name) {
//   std::string left = left_column_name, right = right_column_name;
//   if (!left_input_stats->HasColumnStats(left_column_name)) {
//     left = right_column_name;
//     right = left_column_name;
//   }
//   bool enable_sampling = true;
//   auto column_ids =
//       GenerateJoinSamples(left_input_stats, right_input_stats, output_stats,
//                           left, right, enable_sampling);
//   if (column_ids.empty()) {
//     return enable_sampling;
//   }
//   output_stats->UpdateJoinColumnStats(column_ids);
//   return true;
// }
//
// // Helper function for generating join samples for output stats and calculate
// // and update num_rows.
// std::vector<oid_t> Cost::GenerateJoinSamples(
//     const std::shared_ptr<TableStats> &left_input_stats,
//     const std::shared_ptr<TableStats> &right_input_stats,
//     std::shared_ptr<TableStats> &output_stats,
//     const std::string &left_column_name, const std::string &right_column_name,
//     bool &enable_sampling) {
//   std::vector<oid_t> column_ids;
//   auto sample_stats = left_input_stats, index_stats = right_input_stats;
//   auto sample_column = left_column_name, index_column = right_column_name;
//   if (!right_input_stats->IsBaseTable() ||
//       right_input_stats->GetIndex(right_column_name) == nullptr) {
//     sample_stats = right_input_stats;
//     index_stats = left_input_stats;
//     sample_column = right_column_name;
//     index_column = left_column_name;
//   }
//
//   auto index = index_stats->GetIndex(index_column);
//   // No index available or sample_stats doesn't have samples available
//   if (index == nullptr || !sample_stats->GetSampler()) {
//     enable_sampling = false;
//     return column_ids;
//   }
//   // index_stats should be base table and have non-null sampler
//   PELOTON_ASSERT(index_stats->GetSampler() != nullptr);
//
//   // Already have tuple sampled, copy the sampled tuples
//   if (!index_stats->GetSampler()->GetSampledTuples().empty()) {
//     output_stats->SetTupleSampler(index_stats->GetSampler());
//     return column_ids;
//   }
//
//   if (sample_stats->IsBaseTable() &&
//       sample_stats->GetSampler()->GetSampledTuples().empty()) {
//     sample_stats->SampleTuples();
//   }
//   auto column_id = sample_stats->GetColumnStats(sample_column)->column_id;
//   auto &sample_tuples = sample_stats->GetSampler()->GetSampledTuples();
//   if (sample_tuples.empty()) {
//     enable_sampling = false;
//     return column_ids;
//   }
//   int cnt = 0;
//   std::vector<std::vector<ItemPointer *>> matched_tuples;
//   for (size_t i = 0; i < sample_tuples.size(); i++) {
//     auto key = sample_tuples.at(i)->GetValue(column_id);
//
//     std::vector<ItemPointer *> fetched_tuples;
//     auto schema = sample_tuples.at(i)->GetSchema();
//     std::vector<oid_t> key_attrs;
//     key_attrs.push_back(column_id);
//     std::unique_ptr<catalog::Schema> key_schema(
//         catalog::Schema::CopySchema(schema, key_attrs));
//     auto key_tuple = std::make_shared<storage::Tuple>(key_schema.get(), true);
//     type::Value fetched_value = (sample_tuples.at(i)->GetValue(column_id));
//     key_tuple->SetValue(0, fetched_value);
//     index->ScanKey(key_tuple.get(), fetched_tuples);
//     matched_tuples.push_back(fetched_tuples);
//     cnt += fetched_tuples.size();
//   }
//
//   if (cnt == 0) {
//     enable_sampling = false;
//     return column_ids;
//   }
//
//   index_stats->GetSampler()->AcquireSampleTuplesForIndexJoin(
//       sample_tuples, matched_tuples, cnt);
//   output_stats->SetTupleSampler(index_stats->GetSampler());
//   output_stats->num_rows =
//       (size_t)(sample_stats->num_rows * cnt / (double)sample_tuples.size());
//
//   oid_t column_offset = sample_tuples.at(0)->GetColumnCount();
//   for (oid_t i = 0; i < output_stats->GetColumnCount(); i++) {
//     auto column_stats = output_stats->GetColumnStats(i);
//     if (index_stats->HasColumnStats(column_stats->column_name)) {
//       column_stats->column_id = column_stats->column_id + column_offset;
//     }
//     column_ids.push_back(column_stats->column_id);
//   }
//   return column_ids;
// }
//
// void Cost::UpdateJoinOutputStats(
//     const std::shared_ptr<TableStats> &left_input_stats,
//     const std::shared_ptr<TableStats> &right_input_stats,
//     std::shared_ptr<TableStats> &output_stats,
//     const std::shared_ptr<expression::AbstractExpression> predicate,
//     JoinType join_type, bool enable_sampling) {
//   size_t adjustment;
//   switch (join_type) {
//     case JoinType::INNER:
//       adjustment = 0;
//     case JoinType::LEFT:
//       adjustment = left_input_stats->num_rows;
//     case JoinType::RIGHT:
//       adjustment = right_input_stats->num_rows;
//     case JoinType::OUTER:
//       adjustment = left_input_stats->num_rows + right_input_stats->num_rows;
//     default:
//       adjustment = 0;
//   }
//   size_t default_join_size =
//       left_input_stats->num_rows * right_input_stats->num_rows + adjustment;
//   if (predicate == nullptr) {
//     output_stats->num_rows = default_join_size;
//   } else if (predicate->GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
//     // consider only A.a = B.a case here
//     if (predicate->GetChildrenSize() != 2 ||
//         predicate->GetChild(0)->GetExpressionType() !=
//             ExpressionType::VALUE_TUPLE ||
//         predicate->GetChild(1)->GetExpressionType() !=
//             ExpressionType::VALUE_TUPLE) {
//       output_stats->num_rows = default_join_size;
//       LOG_ERROR("Join predicate not supported %s",
//                 predicate->GetInfo().c_str());
//       return;
//     }
//
//     auto left_child =
//         reinterpret_cast<const expression::TupleValueExpression *>(
//             predicate->GetChild(0));
//     auto right_child =
//         reinterpret_cast<const expression::TupleValueExpression *>(
//             predicate->GetChild(1));
//     std::string left_column_name =
//         left_child->GetTableName() + "." + left_child->GetColumnName();
//     std::string right_column_name =
//         right_child->GetTableName() + "." + right_child->GetColumnName();
//
//     if (!enable_sampling ||
//         !UpdateJoinOutputStatsWithSampling(left_input_stats, right_input_stats,
//                                            output_stats, left_column_name,
//                                            right_column_name)) {
//       double left_cardinality, right_cardinality;
//       if (left_input_stats->HasColumnStats(left_column_name)) {
//         left_cardinality = left_input_stats->GetCardinality(left_column_name);
//       } else if (right_input_stats->HasColumnStats(left_column_name)) {
//         left_cardinality = right_input_stats->GetCardinality(left_column_name);
//       } else {
//         left_cardinality = 0;
//         LOG_ERROR("join column %s not found", left_column_name.c_str());
//       }
//
//       if (left_input_stats->HasColumnStats(right_column_name)) {
//         right_cardinality = left_input_stats->GetCardinality(right_column_name);
//       } else if (right_input_stats->HasColumnStats(right_column_name)) {
//         right_cardinality =
//             right_input_stats->GetCardinality(right_column_name);
//       } else {
//         right_cardinality = 0;
//         LOG_ERROR("join column %s not found", right_column_name.c_str());
//       }
//       if (left_cardinality == 0 || right_cardinality == 0) {
//         output_stats->num_rows = default_join_size;
//       } else {
//         // n_l * n_r / sqrt(V(A, l) * V(A, r))
//         output_stats->num_rows =
//             (size_t)(left_input_stats->num_rows * right_input_stats->num_rows /
//                      std::max(left_cardinality, right_cardinality)) +
//             adjustment;
//       }
//     }
//   } else {
//     // conjunction predicates
//     output_stats->num_rows = default_join_size;
//   }
// }
//
// void Cost::UpdateConditionStats(const std::shared_ptr<TableStats> &input_stats,
//                                 const ValueCondition &condition,
//                                 std::shared_ptr<TableStats> &output_stats) {
//   if (output_stats != nullptr) {
//     double selectivity =
//         Selectivity::ComputeSelectivity(input_stats, condition);
//     output_stats->num_rows = input_stats->num_rows * selectivity;
//   }
// }
//
// size_t Cost::GetEstimatedGroupByRows(
//     const std::shared_ptr<TableStats> &input_stats,
//     std::vector<std::string> &columns) {
//   // Idea is to assume each column is uniformaly network and get an
//   // overestimation.
//   // Then use max cardinality among all columns as underestimation.
//   // And combine them together.
//   double rows = 1;
//   double max_cardinality = 0;
//   for (auto column : columns) {
//     double cardinality = input_stats->GetCardinality(column);
//     max_cardinality = std::max(max_cardinality, cardinality);
//     rows *= cardinality;
//   }
//   return static_cast<size_t>(rows + max_cardinality / 2);
// }
//
}  // namespace optimizer
}  // namespace peloton
