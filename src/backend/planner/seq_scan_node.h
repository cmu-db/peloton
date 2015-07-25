/**
 * @brief Header for sequential scan plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_plan_node.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class SeqScanNode : public AbstractPlanNode {
 public:
  SeqScanNode(const SeqScanNode &) = delete;
  SeqScanNode &operator=(const SeqScanNode &) = delete;
  SeqScanNode(SeqScanNode &&) = delete;
  SeqScanNode &operator=(SeqScanNode &&) = delete;

  SeqScanNode(storage::DataTable *table,
              expression::AbstractExpression *predicate,
              const std::vector<oid_t> &column_ids)
      : table_(table), predicate_(predicate), column_ids_(column_ids) {}

  const storage::DataTable *GetTable() const { return table_; }

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_SEQSCAN; }

  inline std::string GetInfo() const { return "SeqScan"; }

 private:
  /** @brief Pointer to table to scan from. */
  const storage::DataTable *table_;

  /** @brief Selection predicate. */
  const std::unique_ptr<expression::AbstractExpression> predicate_;

  /** @brief Columns from tile group to be added to logical tile output. */
  const std::vector<oid_t> column_ids_;
};

}  // namespace planner
}  // namespace peloton
