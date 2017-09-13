//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_plan.h
//
// Identification: src/include/planner/order_by_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"
#include "planner/attribute_info.h"

namespace peloton {
namespace planner {

/**
 * IMPORTANT All tiles got from child must have the same physical schema.
 */

class OrderByPlan : public AbstractPlan {
 public:
  OrderByPlan(const std::vector<oid_t> &sort_keys,
              const std::vector<bool> &descend_flags,
              const std::vector<oid_t> &output_column_ids);

  void PerformBinding(BindingContext &binding_context) override;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

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

  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::ORDERBY; }

  void GetOutputColumns(std::vector<oid_t> &columns) const override {
    columns = GetOutputColumnIds();
  }

  const std::string GetInfo() const override { return "OrderBy"; }

  void SetUnderlyingOrder(bool same_order) { underling_ordered_ = same_order; }

  void SetLimit(bool limit) { limit_ = limit; }

  void SetLimitNumber(uint64_t limit_number) { limit_number_ = limit_number; }

  void SetLimitOffset(uint64_t limit_offset) { limit_offset_ = limit_offset; }

  bool GetUnderlyingOrder() const { return underling_ordered_; }

  bool GetLimit() const { return limit_; }

  uint64_t GetLimitNumber() const { return limit_number_; }

  uint64_t GetLimitOffset() const { return limit_offset_; }

  std::unique_ptr<AbstractPlan> Copy() const override {
    return std::unique_ptr<AbstractPlan>(
        new OrderByPlan(sort_keys_, descend_flags_, output_column_ids_));
  }

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;
  bool operator!=(const AbstractPlan &rhs) const override {
    return !(*this == rhs);
  }

 private:
  /** @brief Column Ids to sort keys w.r.t input tiles.
   *  Primary sort key comes first, secondary comes next, etc.
   */
  const std::vector<oid_t> sort_keys_;

  /** @brief Sort order flags. */
  const std::vector<bool> descend_flags_;

  /** @brief Projected columns Ids. */
  const std::vector<oid_t> output_column_ids_;

  std::vector<const AttributeInfo *> output_ais_;
  std::vector<const AttributeInfo *> sort_key_ais_;

  // Used to show that whether the output is has the same ordering with order by
  // expression. If the so, we can directly used the output result without any
  // additional sorting operation
  bool underling_ordered_ = false;

  // Whether there is limit clause;
  bool limit_ = false;

  // How many tuples denoted by limit clause
  uint64_t limit_number_ = 0;

  // The point denoted by limit clause
  uint64_t limit_offset_ = 0;

 private:
  DISALLOW_COPY_AND_MOVE(OrderByPlan);
};

}  // namespace planner
}  // namespace peloton
