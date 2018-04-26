//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_plan.h
//
// Identification: src/include/planner/copy_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

namespace peloton {

namespace storage {
class DataTable;
}  // namespace storage

namespace planner {

class CopyPlan : public AbstractPlan {
 public:
  explicit CopyPlan(std::string file_path) : file_path(std::move(file_path)) {}

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::COPY; }

  const std::string GetInfo() const override { return "CopyPlan"; }

  // TODO: Implement copy mechanism
  std::unique_ptr<AbstractPlan> Copy() const override { return nullptr; }

  // The path of the target file
  std::string file_path;

 private:
  DISALLOW_COPY_AND_MOVE(CopyPlan);
};

}  // namespace planner
}  // namespace peloton