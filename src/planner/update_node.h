/**
 * @brief Header for delete plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "planner/abstract_plan_node.h"
#include "common/types.h"
#include "storage/table.h"

namespace nstore {
namespace planner {

class UpdateNode : public AbstractPlanNode {
 public:
  UpdateNode() = delete;
  UpdateNode(const UpdateNode &) = delete;
  UpdateNode& operator=(const UpdateNode &) = delete;
  UpdateNode(UpdateNode &&) = delete;
  UpdateNode& operator=(UpdateNode &&) = delete;

  explicit UpdateNode(storage::Table* table,
                      const std::vector<id_t>& column_ids,
                      const std::vector<Value>& values)
    : target_table_(table),
      column_ids(column_ids),
      values(values){
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_UPDATE;
  }

  storage::Table *GetTable() const{
    return target_table_;
  }

  std::string GetInfo() const {
    return target_table_->GetName();
  }

  const std::vector<id_t>& GetUpdatedColumns(){
    return column_ids;
  }

  const std::vector<Value>& GetUpdatedColumnValues(){
    return values;
  }

 private:

  /** @brief Target table. */
  storage::Table *target_table_;

  /** @brief Columns altered by update */
  const std::vector<id_t>& column_ids;

  /** @brief Values of columns altered by update */
  const std::vector<Value>& values;
};

} // namespace planner
} // namespace nstore
