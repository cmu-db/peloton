//===----------------------------------------------------------------------===//
//
//							PelotonDB
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

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_scan_node.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class SeqScanNode : public AbstractScanNode {
public:
  SeqScanNode(const SeqScanNode &) = delete;
  SeqScanNode &operator=(const SeqScanNode &) = delete;
  SeqScanNode(SeqScanNode &&) = delete;
  SeqScanNode &operator=(SeqScanNode &&) = delete;

  SeqScanNode(storage::DataTable *table,
              expression::AbstractExpression *predicate,
              const std::vector<oid_t> &column_ids)
      : AbstractScanNode(predicate, column_ids), table_(table) {}

  const storage::DataTable *GetTable() const { return table_; }

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_SEQSCAN; }

  inline std::string GetInfo() const { return "SeqScan"; }

private:
  /** @brief Pointer to table to scan from. */
  const storage::DataTable *table_;
};

} // namespace planner
} // namespace peloton
