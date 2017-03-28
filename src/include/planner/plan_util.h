
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

#include <set>
#include <string>

#include "planner/abstract_plan.h"
#include "planner/populate_index_plan.h"
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

 public:
  /**
   * @brief Get the tables referenced in the plan
   * @param The plan tree
   * @return set of the Table oids
   */
  static const std::set<oid_t> GetTablesReferenced(
      const planner::AbstractPlan *plan) {
    std::set<oid_t> table_ids;
    if (plan != nullptr) {
      GetTablesReferenced(plan, table_ids);
    }
    return (table_ids);
  }

 private:
  static void GetTablesReferenced(const planner::AbstractPlan *plan,
                                  std::set<oid_t> &table_ids) {
    switch (plan->GetPlanNodeType()) {
      case PlanNodeType::ABSTRACT_SCAN:
      case PlanNodeType::SEQSCAN:
      case PlanNodeType::INDEXSCAN: {
        const planner::AbstractScan *scan_node =
            reinterpret_cast<const planner::AbstractScan *>(plan);
        table_ids.insert(scan_node->GetTable()->GetOid());
        break;
      }
      case PlanNodeType::INSERT: {
        const planner::InsertPlan *insert_node =
            reinterpret_cast<const planner::InsertPlan *>(plan);
        table_ids.insert(insert_node->GetTable()->GetOid());
        break;
      }
      case PlanNodeType::POPULATE_INDEX: {
        const planner::PopulateIndexPlan *populate_index_node =
                reinterpret_cast<const planner::PopulateIndexPlan *>(plan);
        table_ids.insert(populate_index_node->GetTable()->GetOid());
        break;
      }
      default: {
        // Nothing to do, nothing to see...
        break;
      }
    }  // SWITCH
    for (auto &child : plan->GetChildren()) {
      if (child != nullptr) {
        GetTablesReferenced(child.get(), table_ids);
      }
    }
  }
};
}
}
