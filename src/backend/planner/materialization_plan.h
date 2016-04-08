//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// materialization_plan.h
//
// Identification: src/backend/planner/materialization_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "backend/planner/abstract_plan.h"
#include "backend/common/types.h"
#include "backend/catalog/schema.h"

namespace peloton {

namespace planner {

class MaterializationPlan : public AbstractPlan {
 public:
  MaterializationPlan(const MaterializationPlan &) = delete;
  MaterializationPlan &operator=(const MaterializationPlan &) = delete;
  MaterializationPlan(MaterializationPlan &&) = delete;
  MaterializationPlan &operator=(MaterializationPlan &&) = delete;

  MaterializationPlan(const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
                      std::shared_ptr<const catalog::Schema> &schema,
                      bool physify_flag)
      : old_to_new_cols_(old_to_new_cols),
        schema_(schema),
        physify_flag_(physify_flag) {}

  MaterializationPlan(bool physify_flag)
      : schema_(nullptr), physify_flag_(physify_flag) {}

  inline const std::unordered_map<oid_t, oid_t> &old_to_new_cols() const {
    return old_to_new_cols_;
  }

  inline const catalog::Schema *GetSchema() const {
    return schema_.get();
  }

  inline bool GetPhysifyFlag() const { return physify_flag_; }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_MATERIALIZE;
  }

  const std::string GetInfo() const { return "Materialize"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(schema_));
    return std::unique_ptr<AbstractPlan>(
        new MaterializationPlan(old_to_new_cols_, schema_copy, physify_flag_));
  }

 private:
  /**
   * @brief Mapping of old column ids to new column ids after materialization.
   */
  std::unordered_map<oid_t, oid_t> old_to_new_cols_;

  /** @brief Schema of newly materialized tile. */
  std::shared_ptr<const catalog::Schema> schema_;

  /**
   * @brief whether to create a physical tile or just pass thru underlying
   * logical tile
   */
  bool physify_flag_;
};

}  // namespace planner
}  // namespace peloton
