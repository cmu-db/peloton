//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// result_node.h
//
// Identification: src/backend/planner/result_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_plan.h"
#include "backend/common/types.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace planner {

/**
 * @brief	Result plan node.
 * The counter-part of Postgres Result plan
 * that returns a single constant tuple.
 */
class ResultPlan : public AbstractPlan {
 public:
  ResultPlan(const ResultPlan &) = delete;
  ResultPlan &operator=(const ResultPlan &) = delete;
  ResultPlan(ResultPlan &&) = delete;
  ResultPlan &operator=(ResultPlan &&) = delete;

  ResultPlan(storage::Tuple *tuple, storage::AbstractBackend *backend)
      : tuple_(tuple), backend_(backend) {}

  // Accessors
  const storage::Tuple *GetTuple() const { return tuple_.get(); }

  storage::AbstractBackend *GetBackend() const { return backend_; }

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_RESULT; }

  inline std::string GetInfo() const { return "Result"; }

 private:
  /**
   * @brief A backend is needed to create physical tuple
   * TODO: Can we move backend out of the plan?
   */
  storage::AbstractBackend *backend_;
  std::unique_ptr<storage::Tuple> tuple_;
};

} /* namespace planner */
} /* namespace peloton */
