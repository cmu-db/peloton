//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_util.h
//
// Identification: src/include/planner/plan_util.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <set>
#include <string>

#include "planner/abstract_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "planner/populate_index_plan.h"
#include "storage/data_table.h"
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
  static std::string GetInfo(const planner::AbstractPlan *plan);

  /**
   * @brief Get the tables referenced in the plan
   * @param The plan tree
   * @return set of the Table oids
   */
  static const std::set<oid_t> GetTablesReferenced(
      const planner::AbstractPlan *plan);

 private:
  ///
  /// Helpers for GetInfo() and GetTablesReferenced()
  ///

  static void GetInfo(const planner::AbstractPlan *plan, std::ostringstream &os,
                      int num_indent);

  static void GetTablesReferenced(const planner::AbstractPlan *plan,
                                  std::set<oid_t> &table_ids);
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline std::string PlanUtil::GetInfo(const planner::AbstractPlan *plan) {
  std::ostringstream os;
  int num_indent = 0;

  if (plan == nullptr) {
    os << "<NULL>";
  } else {
    os << peloton::GETINFO_SINGLE_LINE << std::endl;
    PlanUtil::GetInfo(plan, os, num_indent);
    os << peloton::GETINFO_SINGLE_LINE;
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

inline void PlanUtil::GetInfo(const planner::AbstractPlan *plan,
                              std::ostringstream &os, int num_indent) {
  os << StringUtil::Indent(num_indent)
     << "-> Plan Type: " << PlanNodeTypeToString(plan->GetPlanNodeType())
     << std::endl;
  os << StringUtil::Indent(num_indent + peloton::ARROW_INDENT)
     << "Info: " << plan->GetInfo() << std::endl;

  auto &children = plan->GetChildren();
  os << StringUtil::Indent(num_indent + peloton::ARROW_INDENT)
     << "NumChildren: " << children.size() << std::endl;
  for (auto &child : children) {
    GetInfo(child.get(), os, num_indent + peloton::ARROW_INDENT);
  }
}

inline const std::set<oid_t> PlanUtil::GetTablesReferenced(
    const planner::AbstractPlan *plan) {
  std::set<oid_t> table_ids;
  if (plan != nullptr) {
    GetTablesReferenced(plan, table_ids);
  }
  return (table_ids);
}

inline void PlanUtil::GetTablesReferenced(const planner::AbstractPlan *plan,
                                          std::set<oid_t> &table_ids) {
  switch (plan->GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN:
    case PlanNodeType::INDEXSCAN: {
      const auto *scan_node =
          reinterpret_cast<const planner::AbstractScan *>(plan);
      table_ids.insert(scan_node->GetTable()->GetOid());
      break;
    }
    case PlanNodeType::INSERT: {
      const auto *insert_node =
          reinterpret_cast<const planner::InsertPlan *>(plan);
      table_ids.insert(insert_node->GetTable()->GetOid());
      break;
    }
    case PlanNodeType::UPDATE: {
      const auto *update_node =
          reinterpret_cast<const planner::UpdatePlan *>(plan);
      table_ids.insert(update_node->GetTable()->GetOid());
      break;
    }
    case PlanNodeType::DELETE: {
      const auto *delete_node =
          reinterpret_cast<const planner::DeletePlan *>(plan);
      table_ids.insert(delete_node->GetTable()->GetOid());
      break;
    }
    case PlanNodeType::POPULATE_INDEX: {
      const auto *populate_index_node =
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

}  // namespace planner
}  // namespace peloton
