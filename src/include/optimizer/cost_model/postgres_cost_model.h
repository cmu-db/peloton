//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_cost_calculator.h
//
// Identification: src/include/optimizer/postgres_cost_calculator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "optimizer/cost_model/abstract_cost_model.h"
#include "expression/tuple_value_expression.h"
#include "catalog/table_catalog.h"
#include "optimizer/memo.h"
#include "optimizer/operators.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"

// TODO: This is not fully reflective of the postgres cost model. Currently we
// are attempting
// to emulate their hash join cost model

namespace peloton {
namespace optimizer {

class Memo;
// Derive cost for a physical group expression
class PostgresCostModel : public AbstractCostModel {
 public:
  PostgresCostModel(){};

  double CalculateCost(GroupExpression *gexpr, Memo *memo,
                       concurrency::TransactionContext *txn) override {
    gexpr_ = gexpr;
    memo_ = memo;
    txn_ = txn;
    gexpr_->Op().Accept(this);
    return output_cost_;
  };

  void Visit(UNUSED_ATTRIBUTE const DummyScan *op) override {
    output_cost_ = 0.f;
  }

  void Visit(const PhysicalSeqScan *op) override {
    auto table_stats = std::dynamic_pointer_cast<TableStats>(
        StatsStorage::GetInstance()->GetTableStats(
            op->table_->GetDatabaseOid(), op->table_->GetTableOid(), txn_));
    if (table_stats->GetColumnCount() == 0) { // We have no table stats
      output_cost_ = 1.f;
      return;
    }
    output_cost_ = table_stats->num_rows * DEFAULT_TUPLE_COST;
  }

  void Visit(const PhysicalIndexScan *op) override {
    auto table_stats = std::dynamic_pointer_cast<TableStats>(
        StatsStorage::GetInstance()->GetTableStats(
            op->table_->GetDatabaseOid(), op->table_->GetTableOid(), txn_));
    if (table_stats->GetColumnCount() == 0 || table_stats->num_rows == 0) {
      output_cost_ = 0.f;
      return;
    }
    // Index search cost + scan cost
    output_cost_ = std::log2(table_stats->num_rows) * DEFAULT_INDEX_TUPLE_COST +
        memo_->GetGroupByID(gexpr_->GetGroupID())->GetNumRows() *
            DEFAULT_TUPLE_COST;
  }

  void Visit(
      UNUSED_ATTRIBUTE const QueryDerivedScan *op) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const PhysicalOrderBy *) override {
    SortCost();
  }

  void Visit(const PhysicalLimit *op) override {
    auto child_num_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

    output_cost_ =
        std::min((size_t)child_num_rows, (size_t)op->limit) * DEFAULT_TUPLE_COST;
  }

  void Visit(UNUSED_ATTRIBUTE const PhysicalInnerNLJoin *op) override {
    auto left_child_rows =
        std::max(0, memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows());
    auto right_child_rows =
        std::max(0, memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows());
    output_cost_ = left_child_rows * right_child_rows * DEFAULT_TUPLE_COST;
    LOG_DEBUG("----------NL Join Output--------");
    LOG_DEBUG("Left: %s | Rows: %d", GetTableName(op->left_keys).c_str(), left_child_rows);
    LOG_DEBUG("Right: %s | Rows: %d", GetTableName(op->right_keys).c_str(), right_child_rows);
    LOG_DEBUG("Cost: %f", output_cost_);
    LOG_DEBUG("--------------------------------");
  }
  void Visit(UNUSED_ATTRIBUTE const PhysicalLeftNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalRightNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalOuterNLJoin *op) override {}

  /* The main idea of this cost estimate is that the comparisons done is the outer
 * table (probe side) times tuples
 * bucket. Thus, we estimate attempt to estimate the bucket size in a similar
 * manner to postgres.
 */
  void Visit(const PhysicalInnerHashJoin *op) override {
    auto bucket_size_frac = 1.0;

    // Assuming you build table on right relation
    if (IsBaseTable(op->right_keys)) {
      auto right_group = memo_->GetGroupByID(gexpr_->GetChildGroupId(1));

      // Iterate over all keys, take the largest fraction (smallest bucket sizes)
      // TODO: Add more estimate adjustments from postgres
      for (auto &expr : op->right_keys) {
        auto tv_expr =
            reinterpret_cast<expression::TupleValueExpression *>(expr.get());
        auto stats = right_group->GetStats(tv_expr->GetColFullName());

        if (stats == nullptr) continue;

        // TODO: A new hash join PR uses 256 as default so we're using this for
        // now and hardcoding it here
        auto num_buckets = 256.0;

        double frac_est;

        if (stats->cardinality > num_buckets) {
          frac_est = 1.0 / num_buckets;
        } else {
          frac_est = 1.0 / std::max(stats->cardinality, 1.0);
        }

        /* Average frequency of values, taken from Postgres */
        auto avgfreq = (1.0 - stats->frac_null) / stats->cardinality;

        // Adjust for skew (Highest freq / avg freq)
        if (avgfreq > 0.0 && !stats->most_common_vals.empty() &&
            !stats->most_common_freqs.empty() &&
            (stats->most_common_freqs[0] / stats->num_rows) > avgfreq) {
          frac_est *= (stats->most_common_freqs[0] / stats->num_rows) / avgfreq;
        }

        // Clamp the bucket frac estimate (taken from postgres)
        if (frac_est < 1.0e-6) {
          frac_est = 1.0e-6;
        } else if (frac_est > 1.0) {
          frac_est = 1.0;
        }
        bucket_size_frac = std::min(bucket_size_frac, frac_est);
        LOG_DEBUG("Bucket_size_frac: %f", bucket_size_frac);
      }
    }

    auto left_child_rows =
        std::max(0, memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows());
    auto right_child_rows =
        std::max(0, memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows());


    output_cost_ = (left_child_rows + (right_child_rows * bucket_size_frac)) * DEFAULT_TUPLE_COST;
    LOG_DEBUG("---------Hash Join Output-------");
    LOG_DEBUG("Left: %s | Rows: %d", GetTableName(op->left_keys).c_str(), left_child_rows);
    LOG_DEBUG("Right: %s | Rows: %d", GetTableName(op->right_keys).c_str(), right_child_rows);
    LOG_DEBUG("Cost: %f", output_cost_);
    LOG_DEBUG("--------------------------------");


  }

  void Visit(UNUSED_ATTRIBUTE const PhysicalLeftHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalRightHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalOuterHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalInsert *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalInsertSelect *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalDelete *op) override{}
  void Visit(UNUSED_ATTRIBUTE const PhysicalUpdate *op) override {}

  void Visit(UNUSED_ATTRIBUTE const PhysicalHashGroupBy *op) override {
    // TODO(boweic): Integrate hash in groupby may cause us to miss the
    // opportunity to further optimize some query where the child output is
    // already hashed by the GroupBy key, we'll do a hash anyway
    output_cost_ = HashCost() + GroupByCost();
  }
  void Visit(UNUSED_ATTRIBUTE const PhysicalSortGroupBy *op) override {
    // Sort group by does not sort the tuples, it requires input columns to be
    // sorted
    output_cost_ = GroupByCost();
  }
  void Visit(UNUSED_ATTRIBUTE const PhysicalDistinct *op) override {
    output_cost_ = HashCost();
  }
  void Visit(UNUSED_ATTRIBUTE const PhysicalAggregate *op) override {
    // TODO(boweic): Ditto, separate groupby operator and implementation(e.g.
    // hash, sort) may enable opportunity for further optimization
    output_cost_ = HashCost() + GroupByCost();
  }

 private:
  double HashCost() {
    auto child_num_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    // O(tuple)
    return child_num_rows * DEFAULT_TUPLE_COST;
  }

  double SortCost() {
    auto child_num_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

    if (child_num_rows == 0) {
      return 1.0f;
    }
    // O(tuple * log(tuple))
    return child_num_rows * std::log2(child_num_rows) * DEFAULT_TUPLE_COST;
  }

  double GroupByCost() {
    auto child_num_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    // O(tuple)
    return child_num_rows * DEFAULT_TUPLE_COST;
  }


  GroupExpression *gexpr_;
  Memo *memo_;
  concurrency::TransactionContext *txn_;
  double output_cost_ = 0;

  /* Checks if keys for a join child only reference one table */
  bool IsBaseTable(
      const std::vector<std::unique_ptr<expression::AbstractExpression>> &keys) {
    std::unordered_set<std::string> seen_set;

    for (auto &expr : keys) {
      if (expr->GetExpressionType() != ExpressionType::VALUE_TUPLE) continue;

      auto tv_expr =
          reinterpret_cast<expression::TupleValueExpression *>(expr.get());
      seen_set.insert(tv_expr->GetTableName());
    }
    return seen_set.size() == 1;
  }

  // Returns string of tables, used for debugging
  std::string GetTableName(const std::vector<std::unique_ptr<expression::AbstractExpression>> &keys) {
    std::unordered_set<std::string> table_set;
    for (auto &expr : keys) {
      if (expr->GetExpressionType() != ExpressionType::VALUE_TUPLE) continue;

      auto tv_expr =
          reinterpret_cast<expression::TupleValueExpression *>(expr.get());
      table_set.insert(tv_expr->GetTableName());
    }

    std::stringstream stream;
    if (table_set.size() == 1) {
      stream << *table_set.begin();
    } else {
      for (auto table : table_set) {
        if (!stream.str().empty()) {
          stream << ",";
        }
        stream << table;
      }
    }

    return stream.str();
  }

};

}  // namespace optimizer
}  // namespace peloton