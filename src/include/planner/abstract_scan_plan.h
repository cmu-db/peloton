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
#include <numeric>
#include <string>
#include <vector>

#include "abstract_plan.h"
#include "type/types.h"
#include "expression/abstract_expression.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class AbstractScan : public AbstractPlan {
 public:
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
    return PlanNodeType::ABSTRACT_SCAN;
  }

  void GetOutputColumns(std::vector<oid_t> &columns) const {
    columns.resize(GetColumnIds().size());
    std::iota(columns.begin(), columns.end(), 0);
  }

  inline const std::string GetInfo() const { return "AbstractScan"; }

  inline storage::DataTable *GetTable() const { return target_table_; }

  void GetAttributes(std::vector<const AttributeInfo *> &ais) const {
    for (const auto &ai : attributes_) {
      ais.push_back(&ai);
    }
  }

  inline bool IsForUpdate() const {
    // return is_for_update;

    // TODO: Manually disable the select_for_update feature.
    // There is some bug with the current select_for_update logic, we need to
    // fix it later -- Jiexi
    return false;
  }

  // Attribute binding
  void PerformBinding(BindingContext &binding_context) override;

 protected:
  void SetTargetTable(storage::DataTable *table) { target_table_ = table; }

  void AddColumnId(oid_t col_id) { column_ids_.push_back(col_id); }

  void SetPredicate(expression::AbstractExpression *predicate) {
    predicate_ = std::unique_ptr<expression::AbstractExpression>(predicate);
  }

  void SetForUpdateFlag(bool flag) { is_for_update = flag; }

 private:
  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;

  /** @brief Selection predicate. We remove const to make it used when
   * deserialization*/
  std::unique_ptr<expression::AbstractExpression> predicate_;

  /** @brief Columns from tile group to be added to logical tile output. */
  std::vector<oid_t> column_ids_;

  std::vector<AttributeInfo> attributes_;

  // "For Update" Flag
  bool is_for_update = false;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractScan);
};

}  // namespace planner
}  // namespace peloton
