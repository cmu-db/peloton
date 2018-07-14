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

#include "common/internal_types.h"
#include "expression/abstract_expression.h"
#include "index/index.h"
#include "planner/abstract_scan_plan.h"
#include "planner/index_scan_plan.h"
#include "storage/data_table.h"

namespace peloton {

// Forward declaration for the index optimizer
namespace index {
class IndexScanPredicate;
}

namespace planner {

class HybridScanPlan : public AbstractScan {
 public:
  HybridScanPlan(storage::DataTable *table,
                 expression::AbstractExpression *predicate,
                 const std::vector<oid_t> &column_ids,
                 const IndexScanPlan::IndexScanDesc &index_scan_desc,
                 HybridScanType hybrid_scan_type);

  ~HybridScanPlan() {}

  std::unique_ptr<AbstractPlan> Copy() const override {
    return std::unique_ptr<AbstractPlan>(nullptr);
  }

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::SEQSCAN; }

  oid_t GetIndexId() const { return index_id_; }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  const std::vector<oid_t> &GetKeyColumnIds() const { return key_column_ids_; }

  const std::vector<ExpressionType> &GetExprTypes() const {
    return expr_types_;
  }

  const std::vector<type::Value> &GetValues() const { return values_; }

  const std::vector<expression::AbstractExpression *> &GetRunTimeKeys() const {
    return runtime_keys_;
  }

  HybridScanType GetHybridType() const { return type_; }

  const std::string GetInfo() const override { return "HybridScanPlan"; }

 private:
  HybridScanType type_ = HybridScanType::INVALID;

  const std::vector<oid_t> column_ids_;

  const std::vector<oid_t> key_column_ids_;

  const std::vector<ExpressionType> expr_types_;

  const std::vector<type::Value> values_;

  const std::vector<expression::AbstractExpression *> runtime_keys_;

  oid_t index_id_;

 private:
  DISALLOW_COPY_AND_MOVE(HybridScanPlan);
};

}  // namespace planner
}  // namespace peloton
