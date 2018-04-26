//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scan_plan.h
//
// Identification: src/include/planner/csv_scan_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

namespace peloton {
namespace planner {

class CSVScanPlan : public AbstractPlan {
 public:
  CSVScanPlan(const std::string file_name) : file_name_(std::move(file_name)) {}

  PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::CSVSCAN;
  }

  std::unique_ptr<AbstractPlan> Copy() const override;

 private:
  const std::string file_name_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline std::unique_ptr<AbstractPlan> CSVScanPlan::Copy() const {
  // TODO
  return std::unique_ptr<AbstractPlan>();
}

}  // namespace planner
}  // namespace peloton