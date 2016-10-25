//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_plan.h
//
// Identification: src/include/planner/insert_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "planner/abstract_plan.h"
#include "planner/project_info.h"

namespace peloton {

namespace storage {
class DataTable;
class Tuple;
}

namespace parser{
  class InsertStatement;
}

namespace planner {
class InsertPlan : public AbstractPlan {
 public:
  InsertPlan() = delete;
  InsertPlan(const InsertPlan &) = delete;
  InsertPlan &operator=(const InsertPlan &) = delete;
  InsertPlan(InsertPlan &&) = delete;
  InsertPlan &operator=(InsertPlan &&) = delete;

  // This constructor takes in neither a project info nor a tuple
  // It must be used when the input is a logical tile
  explicit InsertPlan(storage::DataTable *table, oid_t bulk_insert_count = 1);

  // This constructor takes in a project info
  explicit InsertPlan(
      storage::DataTable *table,
      std::unique_ptr<const planner::ProjectInfo> &&project_info,
      oid_t bulk_insert_count = 1);
  // This constructor takes in a tuple
  explicit InsertPlan(storage::DataTable *table,
                      std::unique_ptr<storage::Tuple> &&tuple,
                      oid_t bulk_insert_count = 1);
  // This constructor takes in a table name and a tuple
  explicit InsertPlan(std::string table_name,
                        std::unique_ptr<storage::Tuple> &&tuple,
                        oid_t bulk_insert_count = 1);

  explicit InsertPlan(parser::InsertStatement* parse_tree, oid_t bulk_insert_count = 1);


  // Get a varlen pool (will construct the pool only if needed)
  common::VarlenPool *GetPlanPool();

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_INSERT; }

  void SetParameterValues(std::vector<common::Value>* values);

  storage::DataTable *GetTable() const { return target_table_; }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  oid_t GetBulkInsertCount() const { return bulk_insert_count; }

  const storage::Tuple *GetTuple() const { return tuple_.get(); }

  const std::string GetInfo() const { return "InsertPlan"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    // TODO: Add copying mechanism
    std::unique_ptr<AbstractPlan> dummy;
    return dummy;
  }


 private:
  /** @brief Target table. */
  storage::DataTable *target_table_ = nullptr;

  /** @brief Projection Info */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  /** @brief Tuple */
  std::unique_ptr<storage::Tuple> tuple_;

  // <tuple_column_index, parameter_index>
  std::unique_ptr<std::vector<std::pair<oid_t, oid_t>>> parameter_vector_;

  // Parameter values
  std::unique_ptr<std::vector<common::Type::TypeId>> params_value_type_;

  /** @brief Number of times to insert */
  oid_t bulk_insert_count;

  // pool for variable length types
  std::unique_ptr<common::VarlenPool> pool_;
};

}  // namespace planner
}  // namespace peloton
