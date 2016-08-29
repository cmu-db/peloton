//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_scan_plan.h
//
// Identification: src/include/planner/abstract_scan_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_plan.h"
#include "common/types.h"
#include "expression/abstract_expression.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class AbstractScan : public AbstractPlan {
 public:
  AbstractScan(const AbstractScan &) = delete;
  AbstractScan &operator=(const AbstractScan &) = delete;
  AbstractScan(AbstractScan &&) = delete;
  AbstractScan &operator=(AbstractScan &&) = delete;

  AbstractScan(storage::DataTable *table,
               expression::AbstractExpression *predicate,
               const std::vector<oid_t> &column_ids)
      : target_table_(table), predicate_(predicate), column_ids_(column_ids) {}

  // We should add an empty constructor to support an empty object
  AbstractScan() : target_table_(nullptr), predicate_(nullptr) {}

  inline const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  inline const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_ABSTRACT_SCAN;
  }

  inline const std::string GetInfo() const { return "AbstractScan"; }

  inline storage::DataTable *GetTable() const { return target_table_; }

 protected:
  // These methods only used by its derived classes (when deserialization)
  expression::AbstractExpression *Predicate() { return predicate_.get(); }
  std::vector<oid_t> &ColumnIds() { return column_ids_; }
  void SetTargetTable(storage::DataTable *table) { target_table_ = table; }
  void SetColumnId(oid_t col_id) { column_ids_.push_back(col_id); }
  void SetPredicate(expression::AbstractExpression *predicate) {
    predicate_ = std::unique_ptr<expression::AbstractExpression>(predicate);
  }

  /** @brief Predicate with ValueParameters inside. Needed to be binded at
   * the binding stage to generate the "real" predicate.
   */
  std::unique_ptr<expression::AbstractExpression> predicate_with_params_;

 private:
  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;

  /** @brief Selection predicate. We remove const to make it used when
   * deserialization*/
  std::unique_ptr<expression::AbstractExpression> predicate_;

  /** @brief Columns from tile group to be added to logical tile output. */
  std::vector<oid_t> column_ids_;
};

}  // namespace planner
}  // namespace peloton
