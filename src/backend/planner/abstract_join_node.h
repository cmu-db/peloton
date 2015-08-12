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

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_plan_node.h"
#include "backend/planner/project_info.h"

namespace peloton {
namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Join Plan Node
//===--------------------------------------------------------------------===//

class AbstractJoinPlanNode : public AbstractPlanNode {
 public:
  AbstractJoinPlanNode(const AbstractJoinPlanNode &) = delete;
  AbstractJoinPlanNode &operator=(const AbstractJoinPlanNode &) = delete;
  AbstractJoinPlanNode(AbstractJoinPlanNode &&) = delete;
  AbstractJoinPlanNode &operator=(AbstractJoinPlanNode &&) = delete;

  AbstractJoinPlanNode(PelotonJoinType joinType,
                       const expression::AbstractExpression *predicate,
                       const ProjectInfo *proj_info)
      : AbstractPlanNode(),
        joinType_(joinType),
        predicate_(predicate),
        proj_info_(proj_info) {
    // Fuck off!
  }

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  PelotonJoinType GetJoinType() const { return joinType_; }

  void SetJoinType(const PelotonJoinType jointype) {
    this->joinType_ = jointype;
  }

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  const ProjectInfo *GetProjInfo() const { return proj_info_.get(); }

 private:
  /** @brief The type of join that we're going to perform */
  PelotonJoinType joinType_;

  /** @brief Join predicate. */
  const std::unique_ptr<const expression::AbstractExpression> predicate_;

  /** @brief Projection info */
  std::unique_ptr<const ProjectInfo> proj_info_;
};

}  // namespace planner
}  // namespace peloton
