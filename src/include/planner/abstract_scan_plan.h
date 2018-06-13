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

#include "common/internal_types.h"
#include "expression/abstract_expression.h"
#include "planner/abstract_plan.h"

namespace peloton {

namespace storage {
class DataTable;
}  // namespace storage

namespace planner {

class AbstractScan : public AbstractPlan {
 public:
  // We should add an empty constructor to support an empty object
  AbstractScan()
      : target_table_(nullptr), predicate_(nullptr), parallel_(false) {}

  AbstractScan(storage::DataTable *table,
               expression::AbstractExpression *predicate,
               const std::vector<oid_t> &column_ids, bool parallel)
      : target_table_(table),
        predicate_(predicate),
        column_ids_(column_ids),
        parallel_(parallel) {}

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  void GetOutputColumns(std::vector<oid_t> &columns) const override {
    columns.resize(GetColumnIds().size());
    std::iota(columns.begin(), columns.end(), 0);
  }

  storage::DataTable *GetTable() const { return target_table_; }

  virtual void GetAttributes(std::vector<const AttributeInfo *> &ais) const {
    for (const auto &ai : attributes_) {
      ais.push_back(&ai);
    }
  }

  bool IsForUpdate() const { return is_for_update_; }

  void PerformBinding(BindingContext &binding_context) override;

  bool IsParallel() const { return parallel_; }

 protected:
  void SetTargetTable(storage::DataTable *table) { target_table_ = table; }

  void AddColumnId(oid_t col_id) { column_ids_.push_back(col_id); }

  void SetPredicate(expression::AbstractExpression *predicate) {
    predicate_ = std::unique_ptr<expression::AbstractExpression>(predicate);
  }

  void SetForUpdateFlag(bool flag) { is_for_update_ = flag; }

  const std::string GetPredicateInfo() const {
    return predicate_ != nullptr ? predicate_->GetInfo() : "";
  }

 private:
  // Pointer to table to scan from
  storage::DataTable *target_table_ = nullptr;

  // Selection predicate. We remove const to make it used when deserialization
  std::unique_ptr<expression::AbstractExpression> predicate_;

  // Columns from tile group to be added to logical tile output
  std::vector<oid_t> column_ids_;
  std::vector<AttributeInfo> attributes_;

  // Are the tuples produced by this plan intended for update?
  bool is_for_update_ = false;

  // Should this scan be performed in parallel?
  bool parallel_;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractScan);
};

}  // namespace planner
}  // namespace peloton
