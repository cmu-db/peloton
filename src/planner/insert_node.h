/**
 * @brief Header for insert plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "planner/abstract_plan_node.h"
#include "common/types.h"
#include "storage/table.h"

namespace nstore {
namespace planner {

class InsertNode : public AbstractPlanNode {
 public:
  InsertNode() = delete;
  InsertNode(const InsertNode &) = delete;
  InsertNode& operator=(const InsertNode &) = delete;
  InsertNode(InsertNode &&) = delete;
  InsertNode& operator=(InsertNode &&) = delete;

  explicit InsertNode(storage::Table* table,
                      const std::vector<storage::Tuple *>& tuples)
    : target_table_(table),
      tuples(tuples) {
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_INSERT;
  }

  storage::Table *GetTable() const{
    return target_table_;
  }

  std::string GetInfo() const {
    return target_table_->GetName();
  }

  const std::vector<storage::Tuple *>& GetTuples() const {
    return tuples;
  }

 private:
  /** @brief Target table. */
  storage::Table *target_table_;

  // tuples to be inserted
  std::vector<storage::Tuple *> tuples;
};

} // namespace planner
} // namespace nstore
