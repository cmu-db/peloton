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
#include "planner/index_scan_plan.h"
#include "storage/data_table.h"
#include "index/index.h"
#include "common/types.h"
#include "expression/abstract_expression.h"

namespace peloton {
namespace planner {

class HybridScanPlan : public AbstractScan {
 public:
  HybridScanPlan(const HybridScanPlan &) = delete;
  HybridScanPlan &operator=(const HybridScanPlan &) = delete;
  HybridScanPlan(HybridScanPlan &&) = delete;
  HybridScanPlan &operator=(HybridScanPlan &&) = delete;

  HybridScanPlan(storage::DataTable *table,
                 expression::AbstractExpression *predicate,
                 const std::vector<oid_t> &column_ids,
                 const IndexScanPlan::IndexScanDesc &index_scan_desc,
                 HybridScanType hybrid_scan_type)
      : AbstractScan(table, predicate, column_ids),
        type_(hybrid_scan_type),
        column_ids_(column_ids),
        key_column_ids_(std::move(index_scan_desc.key_column_ids)),
        expr_types_(std::move(index_scan_desc.expr_types)),
        values_(std::move(index_scan_desc.values)),
        runtime_keys_(std::move(index_scan_desc.runtime_keys)),
        index_(index_scan_desc.index){}

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

  HybridScanType GetHybridType() const { return type_; }

 private:

  HybridScanType type_ = HYBRID_SCAN_TYPE_INVALID;

  const std::vector<oid_t> column_ids_;

  const std::vector<oid_t> key_column_ids_;

  const std::vector<ExpressionType> expr_types_;

  const std::vector<Value> values_;

  const std::vector<expression::AbstractExpression *> runtime_keys_;

  index::Index *index_ = nullptr;

};

}  // namespace planner
}  // namespace peloton
