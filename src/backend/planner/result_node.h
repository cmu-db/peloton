/**
 * @brief Header for result plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "backend/common/types.h"
#include "backend/planner/abstract_plan_node.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace planner {

/**
 * @brief	Result plan node.
 * The counter-part of Postgres Result plan
 * that returns a single constant tuple.
 */
class ResultNode: public AbstractPlanNode {
public:
  ResultNode(const ResultNode &) = delete;
  ResultNode& operator=(const ResultNode &) = delete;
  ResultNode(ResultNode &&) = delete;
  ResultNode& operator=(ResultNode &&) = delete;

  ResultNode(storage::Tuple* tuple, storage::AbstractBackend* backend)
  : tuple_(tuple),
    backend_(backend){
  }

  // Accessors
  const storage::Tuple* GetTuple() const {
    return tuple_.get();
  }

  storage::AbstractBackend* GetBackend() const {
    return backend_;
  }

  inline PlanNodeType GetPlanNodeType() const{
    return PLAN_NODE_TYPE_RESULT;
  }

  inline std::string GetInfo() const{
    return "Result";
  }


private:
  /**
   * @brief A backend is needed to create physical tuple
   * TODO: Can we move backend out of the plan?
   */
  storage::AbstractBackend* backend_;
  std::unique_ptr<storage::Tuple> tuple_;


};

} /* namespace planner */
} /* namespace peloton */

