//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// seq_scan_node.h
//
// Identification: src/backend/planner/seq_scan_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_scan_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class SeqScanPlan : public AbstractScan {
 public:
  SeqScanPlan(const SeqScanPlan &) = delete;
  SeqScanPlan &operator=(const SeqScanPlan &) = delete;
  SeqScanPlan(SeqScanPlan &&) = delete;
  SeqScanPlan &operator=(SeqScanPlan &&) = delete;

  SeqScanPlan(storage::DataTable *table,
              expression::AbstractExpression *predicate,
              const std::vector<oid_t> &column_ids)
      : AbstractScan(predicate, column_ids), target_table_(table) {}

  storage::DataTable *GetTable() const {
    return target_table_;
  }

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_SEQSCAN; }

  inline std::string GetInfo() const { return "SeqScan"; }

 private:

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;
};

}  // namespace planner
}  // namespace peloton
