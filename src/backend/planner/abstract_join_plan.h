//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_join_plan.h
//
// Identification: src/backend/planner/abstract_join_plan.h
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
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/project_info.h"

namespace peloton {
namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Join Plan Node
//===--------------------------------------------------------------------===//

class AbstractJoinPlan : public AbstractPlan {
 public:
  AbstractJoinPlan(const AbstractJoinPlan &) = delete;
  AbstractJoinPlan &operator=(const AbstractJoinPlan &) = delete;
  AbstractJoinPlan(AbstractJoinPlan &&) = delete;
  AbstractJoinPlan &operator=(AbstractJoinPlan &&) = delete;

  AbstractJoinPlan(
      PelotonJoinType joinType,
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

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  PelotonJoinType GetJoinType() const { return join_type_; }

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  const ProjectInfo* GetProjInfo() const {
    return proj_info_.get();
  }

  const catalog::Schema *GetSchema() const {
    return proj_schema_.get();
  }

  std::unique_ptr<AbstractPlan> Copy() const = 0;

 private:
  /** @brief The type of join that we're going to perform */
  PelotonJoinType join_type_;

  /** @brief Join predicate. */
  const std::unique_ptr<const expression::AbstractExpression> predicate_;

  /** @brief Projection info */
  std::unique_ptr<const ProjectInfo> proj_info_;

  /** @brief Projection schema */
  std::shared_ptr<const catalog::Schema> proj_schema_;
};

}  // namespace planner
}  // namespace peloton
