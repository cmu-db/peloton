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

#include "abstract_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {

class AbstractScan : public AbstractPlan {
 public:
  AbstractScan(const AbstractScan &) = delete;
  AbstractScan &operator=(const AbstractScan &) = delete;
  AbstractScan(AbstractScan &&) = delete;
  AbstractScan &operator=(AbstractScan &&) = delete;

  AbstractScan(expression::AbstractExpression *predicate,
                   const std::vector<oid_t> &column_ids)
      : predicate_(predicate),
        column_ids_(column_ids){
  }

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
  std::vector<oid_t> column_ids_;
};

}  // namespace planner
}  // namespace peloton
