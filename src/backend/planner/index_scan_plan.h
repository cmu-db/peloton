//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_plan.h
//
// Identification: src/backend/planner/index_scan_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "backend/planner/abstract_scan_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/storage/tuple.h"

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

  struct IndexScanDesc {
    IndexScanDesc() : index(nullptr) {}

    IndexScanDesc(
        index::Index *index, const std::vector<oid_t> &column_ids,
        const std::vector<ExpressionType> &expr_types,
        const std::vector<Value> &values,
        const std::vector<expression::AbstractExpression *> &runtime_keys)
        : index(index),
          key_column_ids(column_ids),
          expr_types(expr_types),
          values(values),
          runtime_keys(runtime_keys) {}

    index::Index *index = nullptr;

    std::vector<oid_t> key_column_ids;

    std::vector<ExpressionType> expr_types;

    std::vector<Value> values;

    std::vector<expression::AbstractExpression *> runtime_keys;
  };

  IndexScanPlan(storage::DataTable *table,
                expression::AbstractExpression *predicate,
                const std::vector<oid_t> &column_ids,
                const IndexScanDesc &index_scan_desc)
      : AbstractScan(table, predicate, column_ids),
        index_(index_scan_desc.index),
        column_ids_(column_ids),
        key_column_ids_(std::move(index_scan_desc.key_column_ids)),
        expr_types_(std::move(index_scan_desc.expr_types)),
        values_(std::move(index_scan_desc.values)),
        runtime_keys_(std::move(index_scan_desc.runtime_keys)) {}

  ~IndexScanPlan() {
    for (auto expr : runtime_keys_) {
      delete expr;
    }
  }

  index::Index *GetIndex() const { return index_; }

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

  // Every class should implement SerializeTo method before using it.
  // The implementation in seq_scan_plan can be referenced
  bool SerializeTo(SerializeOutput &output) const {
	  PL_ASSERT(&output != nullptr);
	  throw SerializationException("This class should implement SerializeTo method");
  }

 private:
  /** @brief index associated with index scan. */
  index::Index *index_;

  const std::vector<oid_t> column_ids_;

  const std::vector<oid_t> key_column_ids_;

  const std::vector<ExpressionType> expr_types_;

  const std::vector<Value> values_;

  const std::vector<expression::AbstractExpression *> runtime_keys_;
};

}  // namespace planner
}  // namespace peloton
