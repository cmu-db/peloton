//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_join_node.h
//
// Identification: src/backend/planner/abstract_join_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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

  AbstractJoinPlan(PelotonJoinType joinType,
                   const expression::AbstractExpression *predicate,
                   const ProjectInfo *proj_info,
                   const catalog::Schema *proj_schema)
      : AbstractPlan(),
        join_type_(joinType),
        predicate_(predicate),
        proj_info_(proj_info),
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

  const ProjectInfo *GetProjInfo() const { return proj_info_.get(); }

  const catalog::Schema *GetSchema() const { return proj_schema_.get(); }

 private:
  /** @brief The type of join that we're going to perform */
  PelotonJoinType join_type_;

  /** @brief Join predicate. */
  const std::unique_ptr<const expression::AbstractExpression> predicate_;

  /** @brief Projection info */
  std::unique_ptr<const ProjectInfo> proj_info_;

  /** @brief Projection schema */
  std::unique_ptr<const catalog::Schema> proj_schema_;
};

}  // namespace planner
}  // namespace peloton
