
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_util.h
//
// Identification: /peloton/src/include/planner/plan_util.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "planner/abstract_plan.h"
#include "util/string_util.h"

namespace peloton {
namespace planner {

class PlanUtil {
 public:
  /**
   * @brief Pretty print the plan tree.
   * @param The plan tree
   * @return The string with the pretty-print plan
   */
  static std::string GetInfo(const planner::AbstractPlan *plan) {
    std::ostringstream os;
    std::string spacer("");

    if (plan == nullptr) {
      os << "<NULL>";
    } else {
      PlanUtil::GetInfo(plan, os, spacer);
    }
    return (os.str());
  }

 private:
  static void GetInfo(const planner::AbstractPlan *plan, std::ostringstream &os,
                      std::string prefix) {
    prefix += "  ";
    os << "Plan Type: " << PlanNodeTypeToString(plan->GetPlanNodeType())
       << std::endl;
    os << prefix << plan->GetInfo() << std::endl;

    auto &children = plan->GetChildren();
    os << "NumChildren: " << children.size();

    for (auto &child : children) {
      GetInfo(child.get(), os, prefix);
    }
  }
};
}
}
