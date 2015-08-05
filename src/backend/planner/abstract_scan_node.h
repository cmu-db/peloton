//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_scan_node.h
//
// Identification: src/backend/planner/abstract_scan_node.h
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
#include "backend/planner/abstract_plan_node.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class AbstractScanNode : public AbstractPlanNode {
public:
  AbstractScanNode(const AbstractScanNode &) = delete;
  AbstractScanNode &operator=(const AbstractScanNode &) = delete;
  AbstractScanNode(AbstractScanNode &&) = delete;
  AbstractScanNode &operator=(AbstractScanNode &&) = delete;

  AbstractScanNode(expression::AbstractExpression *predicate,
                   const std::vector<oid_t> &column_ids)
      : predicate_(predicate), column_ids_(column_ids) {}

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_ABSTRACT_SCAN;
  }

  inline std::string GetInfo() const { return "AbstractScan"; }

private:
  /** @brief Selection predicate. */
  const std::unique_ptr<expression::AbstractExpression> predicate_;

  /** @brief Columns from tile group to be added to logical tile output. */
  const std::vector<oid_t> column_ids_;
};

} // namespace planner
} // namespace peloton
