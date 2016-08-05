//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_plan.h
//
// Identification: src/include/planner/index_scan_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "planner/abstract_scan_plan.h"
#include "common/types.h"
#include "expression/abstract_expression.h"
#include "storage/tuple.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {
class Tuple;
}

namespace planner {

class IndexScanPlan : public AbstractScan {
 public:
  IndexScanPlan(const IndexScanPlan &) = delete;
  IndexScanPlan &operator=(const IndexScanPlan &) = delete;
  IndexScanPlan(IndexScanPlan &&) = delete;
  IndexScanPlan &operator=(IndexScanPlan &&) = delete;

  /*
   * class IndexScanDesc - Stores information to do the index scan
   */
  struct IndexScanDesc {
    IndexScanDesc() {}

    IndexScanDesc(std::shared_ptr<index::Index> p_index,
                  const std::vector<oid_t> &p_tuple_column_id_list,
                  const std::vector<ExpressionType> &expr_list_p,
                  const std::vector<Value> &p_value_list,
                  const std::vector<expression::AbstractExpression *> \
                    &p_runtime_key_list) :
      index_obj(p_index),
      tuple_column_id_list(p_tuple_column_id_list),
      expr_list(expr_list_p),
      value_list(p_value_list),
      runtime_key_list(p_runtime_key_list)
    {}

    // The index object for scanning
    std::shared_ptr<index::Index> index_obj;

    // A list of columns id in the base table that has a scan predicate
    // (only for indexed column in the base table)
    std::vector<oid_t> tuple_column_id_list;

    // A list of expressions
    std::vector<ExpressionType> expr_list;

    // A list of values either bounded or unbounded
    std::vector<Value> value_list;

    // ???
    std::vector<expression::AbstractExpression *> runtime_key_list;
  };

  IndexScanPlan(storage::DataTable *table,
                expression::AbstractExpression *predicate,
                const std::vector<oid_t> &column_ids,
                const IndexScanDesc &index_scan_desc);

  ~IndexScanPlan() {
    for (auto expr : runtime_keys_) {
      delete expr;
    }
  }

  std::shared_ptr<index::Index> GetIndex() const { return index_; }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  const std::vector<oid_t> &GetKeyColumnIds() const { return key_column_ids_; }

  const std::vector<ExpressionType> &GetExprTypes() const {
    return expr_types_;
  }

  const std::vector<Value> &GetValues() const { return values_; }

  const std::vector<expression::AbstractExpression *> &GetRunTimeKeys() const {
    return runtime_keys_;
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_INDEXSCAN;
  }

  const std::string GetInfo() const { return "IndexScan"; }

  void SetParameterValues(std::vector<Value> *values);

  std::unique_ptr<AbstractPlan> Copy() const {
    std::vector<expression::AbstractExpression *> new_runtime_keys;
    for (auto *key : runtime_keys_) {
      new_runtime_keys.push_back(key->Copy());
    }

    IndexScanDesc desc(index_, key_column_ids_, expr_types_, values_,
                       new_runtime_keys);
    IndexScanPlan *new_plan = new IndexScanPlan(
        GetTable(), GetPredicate()->Copy(), GetColumnIds(), desc);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  /** @brief index associated with index scan. */
  std::shared_ptr<index::Index> index_;

  const std::vector<oid_t> column_ids_;

  const std::vector<oid_t> key_column_ids_;

  const std::vector<ExpressionType> expr_types_;

  expression::AbstractExpression *predicate_with_params_ = nullptr;

  // LM: I removed a const keyword for binding purpose
  std::vector<Value> values_;

  const std::vector<expression::AbstractExpression *> runtime_keys_;
};

}  // namespace planner
}  // namespace peloton
