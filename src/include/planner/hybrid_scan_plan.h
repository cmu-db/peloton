//
// Created by Rui Wang on 16-4-29.
//

#pragma once

#include "backend/planner/abstract_scan_plan.h"
#include "backend/planner/index_scan_plan.h"
#include "backend/storage/data_table.h"
#include "backend/index/index.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"


namespace peloton {
namespace planner{

enum HybridType {
  UNKNOWN,
  SEQ,
  INDEX,
  HYBRID
};

class HybridScanPlan : public AbstractScan {
public:
  HybridScanPlan(const HybridScanPlan &) = delete;
  HybridScanPlan &operator=(const HybridScanPlan &) = delete;
  HybridScanPlan(HybridScanPlan &&) = delete;
  HybridScanPlan &operator=(HybridScanPlan &&) = delete;


  HybridScanPlan(index::Index *index, storage::DataTable *table,
                 expression::AbstractExpression *predicate,
                 const std::vector<oid_t> &column_ids,
                 const IndexScanPlan::IndexScanDesc &index_scan_desc)
    : AbstractScan(table, predicate, column_ids),
                index_(index),
                column_ids_(column_ids),
                key_column_ids_(std::move(index_scan_desc.key_column_ids)),
                expr_types_(std::move(index_scan_desc.expr_types)),
                values_(std::move(index_scan_desc.values)),
                runtime_keys_(std::move(index_scan_desc.runtime_keys)),
                type_(HYBRID) {}


  HybridScanPlan(storage::DataTable *table, expression::AbstractExpression *predicate,
      const std::vector<oid_t> &column_ids)
      : AbstractScan(table, predicate, column_ids),
        column_ids_(column_ids),
        type_(SEQ) {}

  HybridScanPlan(storage::DataTable *table,
                expression::AbstractExpression *predicate,
                const std::vector<oid_t> &column_ids,
                const IndexScanPlan::IndexScanDesc &index_scan_desc)
                : AbstractScan(table, predicate, column_ids),
                  index_(index_scan_desc.index),
                  column_ids_(column_ids),
                  key_column_ids_(std::move(index_scan_desc.key_column_ids)),
                  expr_types_(std::move(index_scan_desc.expr_types)),
                  values_(std::move(index_scan_desc.values)),
                  runtime_keys_(std::move(index_scan_desc.runtime_keys)),
                  type_(INDEX){}


  index::Index *GetDataIndex() const {
    return this->index_;
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(nullptr);
  }


  PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_SEQSCAN;
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

  HybridType GetHybridType() const {
    return type_;
  }

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
