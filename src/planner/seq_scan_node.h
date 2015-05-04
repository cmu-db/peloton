/**
 * @brief Header for sequential scan plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/types.h"
#include "expression/abstract_expression.h"
#include "planner/abstract_plan_node.h"

namespace nstore {
namespace planner {

class SeqScanNode : public AbstractPlanNode {
 public:
  SeqScanNode(const SeqScanNode &) = delete;
  SeqScanNode& operator=(const SeqScanNode &) = delete;
  SeqScanNode(SeqScanNode &&) = delete;
  SeqScanNode& operator=(SeqScanNode &&) = delete;

  SeqScanNode(
      oid_t table_id,
      expression::AbstractExpression *predicate,
      const std::vector<id_t> &column_ids)
    : table_id_(table_id),
      predicate_(predicate),
      column_ids_(column_ids) {
  }

  oid_t table_id() const {
    return table_id_;
  }

  const expression::AbstractExpression *predicate() const {
    return predicate_.get();
  }

  const std::vector<id_t>& column_ids() const {
    return column_ids_;
  }

  inline PlanNodeType GetPlanNodeType() const {
    //TODO Implement.
    return PLAN_NODE_TYPE_INVALID;
  }

  inline std::string GetInfo() const {
    //TODO Implement.
    return "";
  }

 private:
  /** @brief Oid of table to scan from. */
  const oid_t table_id_;

  /** @brief Selection predicate. */
  const std::unique_ptr<expression::AbstractExpression> predicate_;

  /** @brief Columns from tile group to be added to logical tile output. */
  const std::vector<id_t> column_ids_;
};

} // namespace planner
} // namespace nstore
