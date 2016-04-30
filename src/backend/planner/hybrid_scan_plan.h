//
// Created by Rui Wang on 16-4-29.
//

#pragma once

#include "backend/planner/abstract_plan.h"
#include "backend/planner/index_scan_plan.h"
#include "backend/storage/data_table.h"
#include "backend/index/index.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {
namespace planner{

class HybridScanPlan : public AbstractPlan {
public:
  HybridScanPlan(const HybridScanPlan &) = delete;
  HybridScanPlan &operator=(const HybridScanPlan &) = delete;
  HybridScanPlan(HybridScanPlan &&) = delete;
  HybridScanPlan &operator=(HybridScanPlan &&) = delete;


  HybridScanPlan(index::Index *index, storage::DataTable *table)
    : index_(index), table_(table) {}


  HybridScanPlan(storage::DataTable *table, expression::AbstractExpression *predicate,
      const std::vector<oid_t> &column_ids)
      : table_(table), predicate_(predicate), column_ids_(column_ids) {}

  HybridScanPlan(storage::DataTable *table,
                expression::AbstractExpression *predicate,
                const std::vector<oid_t> &column_ids,
                const IndexScanPlan::IndexScanDesc &index_scan_desc)
                : index_(index_scan_desc.index), table_(table), 
                  predicate_(predicate),
                  column_ids_(column_ids),
                  key_column_ids_(std::move(index_scan_desc.key_column_ids)),
                  expr_types_(std::move(index_scan_desc.expr_types)),
                  values_(std::move(index_scan_desc.values)),
                  runtime_keys_(std::move(index_scan_desc.runtime_keys)) {}


  index::Index *GetDataIndex() const {
    return this->index_;
  }

  storage::DataTable *GetDataTable() const {
    return this->table_;
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(nullptr);
  }

private:
  index::Index *index_ = nullptr;

  storage::DataTable *table_ = nullptr;

  const std::unique_ptr<expression::AbstractExpression> predicate_ = std::unique_ptr<expression::AbstractExpression>(nullptr);

  const std::vector<oid_t> column_ids_;

  const std::vector<oid_t> key_column_ids_;

  const std::vector<ExpressionType> expr_types_;

  const std::vector<Value> values_;

  const std::vector<expression::AbstractExpression *> runtime_keys_;
};

}  // namespace planner
}  // namespace peloton
