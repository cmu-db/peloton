//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hybrid_scan_plan.h
//
// Identification: src/include/planner/hybrid_scan_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_scan_plan.h"

namespace peloton {

class Value;

namespace planner {


enum HybridType { UNKNOWN, SEQ, INDEX, HYBRID };

class HybridScanPlan : public AbstractScan {
 public:
  HybridScanPlan(const HybridScanPlan &) = delete;
  HybridScanPlan &operator=(const HybridScanPlan &) = delete;
  HybridScanPlan(HybridScanPlan &&) = delete;
  HybridScanPlan &operator=(HybridScanPlan &&) = delete;

  HybridScanPlan(index::Index *index, storage::DataTable *table,
                 expression::AbstractExpression *predicate,
                 const std::vector<oid_t> &column_ids,
                 const IndexScanPlan::IndexScanDesc &index_scan_desc);

  HybridScanPlan(storage::DataTable *table,
                 expression::AbstractExpression *predicate,
                 const std::vector<oid_t> &column_ids);

  HybridScanPlan(storage::DataTable *table,
                 expression::AbstractExpression *predicate,
                 const std::vector<oid_t> &column_ids,
                 const IndexScanPlan::IndexScanDesc &index_scan_desc);

  index::Index *GetDataIndex() const { return this->index_; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(nullptr);
  }

  PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_SEQSCAN; }

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

  HybridType GetHybridType() const { return type_; }

 private:
  index::Index *index_ = nullptr;

  const std::vector<oid_t> column_ids_;

  const std::vector<oid_t> key_column_ids_;

  const std::vector<ExpressionType> expr_types_;

  const std::vector<Value> values_;

  const std::vector<expression::AbstractExpression *> runtime_keys_;

  HybridType type_;
};

}  // namespace planner
}  // namespace peloton
