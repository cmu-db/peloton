//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_plan.h
//
// Identification: src/include/planner/order_by_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"
#include "planner/attribute_info.h"

namespace peloton {
namespace planner {

class OrderByPlan : public AbstractPlan {
 public:
  // Constructor for SORT without LIMIT
  OrderByPlan(const std::vector<oid_t> &sort_keys,
              const std::vector<bool> &descend_flags,
              const std::vector<oid_t> &output_column_ids);

  OrderByPlan(const std::vector<oid_t> &sort_keys,
              const std::vector<bool> &descend_flags,
              const std::vector<oid_t> &output_column_ids, uint64_t limit,
              uint64_t offset);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  const std::vector<oid_t> &GetSortKeys() const { return sort_keys_; }

  const std::vector<const planner::AttributeInfo *> &GetSortKeyAIs() const {
    return sort_key_ais_;
  }

  const std::vector<bool> &GetDescendFlags() const { return descend_flags_; }

  const std::vector<oid_t> &GetOutputColumnIds() const {
    return output_column_ids_;
  }

  const std::vector<const planner::AttributeInfo *> &GetOutputColumnAIs()
      const {
    return output_ais_;
  }

  PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::ORDERBY;
  }

  void GetOutputColumns(std::vector<oid_t> &columns) const override {
    columns = GetOutputColumnIds();
  }

  const std::string GetInfo() const override;

  bool HasLimit() const { return has_limit_; }

  uint64_t GetLimit() const { return limit_; }

  uint64_t GetOffset() const { return offset_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utils
  ///
  //////////////////////////////////////////////////////////////////////////////

  void PerformBinding(BindingContext &binding_context) override;

  std::unique_ptr<AbstractPlan> Copy() const override;

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

 private:
  /* Column Ids used (in order) to sort input tuples */
  const std::vector<oid_t> sort_keys_;

  /* Sort order flag. */
  const std::vector<bool> descend_flags_;

  /* Projected columns Ids */
  const std::vector<oid_t> output_column_ids_;

  std::vector<const AttributeInfo *> output_ais_;
  std::vector<const AttributeInfo *> sort_key_ais_;

  /* Whether there is limit clause */
  bool has_limit_;

  /* How many tuples to return */
  uint64_t limit_;

  /* How many tuples to skip first */
  uint64_t offset_;

 private:
  DISALLOW_COPY_AND_MOVE(OrderByPlan);
};

}  // namespace planner
}  // namespace peloton
