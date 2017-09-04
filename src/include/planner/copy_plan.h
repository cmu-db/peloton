//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_plan.h
//
// Identification: src/include/planner/copy_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../parser/copy_statement.h"
#include "../parser/select_statement.h"
#include "planner/abstract_plan.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace parser {
class CopyStatement;
}

namespace planner {

class CopyPlan : public AbstractPlan {
 public:
  CopyPlan() = delete;

  explicit CopyPlan(std::string file_path, bool deserialize_parameters)
      : file_path(file_path), deserialize_parameters(deserialize_parameters) {
    LOG_DEBUG("Creating a Copy Plan");
  }

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::COPY; }

  const std::string GetInfo() const { return "CopyPlan"; }

  // TODO: Implement copy mechanism
  std::unique_ptr<AbstractPlan> Copy() const { return nullptr; }

  // The path of the target file
  std::string file_path;

  // Whether the copying requires deserialization of parameters
  bool deserialize_parameters = false;

 private:
  DISALLOW_COPY_AND_MOVE(CopyPlan);
};

}  // namespace planner
}  // namespace peloton