//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_join_plan.h
//
// Identification: src/include/planner/abstract_join_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <vector>
#include <map>
#include <vector>

#include "abstract_plan.h"
#include "expression/abstract_expression.h"
#include "planner/attribute_info.h"
#include "planner/project_info.h"
#include "common/internal_types.h"

namespace peloton {
namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Join Plan Node
//===--------------------------------------------------------------------===//

class AbstractJoinPlan : public AbstractPlan {
 public:
  AbstractJoinPlan(
      JoinType joinType,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema)
      : AbstractPlan(),
        join_type_(joinType),
        predicate_(std::move(predicate)),
        proj_info_(std::move(proj_info)),
        proj_schema_(proj_schema) {
    // Fuck off!
  }

  void GetOutputColumns(std::vector<oid_t> &columns) const override;

  void PerformBinding(BindingContext &context) override;

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

  void VisitParameters(
      codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  JoinType GetJoinType() const { return join_type_; }

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  const std::vector<const AttributeInfo *> &GetLeftAttributes() const {
    return left_attributes_;
  }

  const std::vector<const AttributeInfo *> &GetRightAttributes() const {
    return right_attributes_;
  }

  const ProjectInfo *GetProjInfo() const { return proj_info_.get(); }

  const catalog::Schema *GetSchema() const { return proj_schema_.get(); }

 protected:
  // For joins, attributes arrive from both the left and right side of the join
  // This method enables this merging by properly identifying each side's
  // attributes in separate contexts.
  virtual void HandleSubplanBinding(bool from_left,
                                    const BindingContext &input) = 0;

 private:
  /** @brief The type of join that we're going to perform */
  JoinType join_type_;

  /** @brief Join predicate. */
  const std::unique_ptr<const expression::AbstractExpression> predicate_;

  /** @brief Projection info */
  std::unique_ptr<const ProjectInfo> proj_info_;

  /** @brief Projection schema */
  std::shared_ptr<const catalog::Schema> proj_schema_;

  std::vector<const AttributeInfo *> left_attributes_;
  std::vector<const AttributeInfo *> right_attributes_;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractJoinPlan);
};

}  // namespace planner
}  // namespace peloton
