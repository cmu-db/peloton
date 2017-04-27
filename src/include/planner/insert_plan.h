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

namespace parser {
class InsertStatement;
}

namespace planner {

/**
 * There are several different flavors of Insert.
 *
 * - Insert a tile (from a scan of another table).
 * - Insert a tile, but with a different schema.
 * - Insert raw tuples.
 */
class InsertPlan : public AbstractPlan {
 public:

  /**
   * Insert tuples from a scan of another table.
   * The scan plan will be added as a child later.
   * @param table             The table to insert into (not scan from).
   * @param bulk_insert_count The number of tuples to insert (1).
   */
  explicit InsertPlan(storage::DataTable *table,
                      oid_t bulk_insert_count = 1);

  // This constructor takes in a project info
  explicit InsertPlan(
      storage::DataTable *table,
      std::unique_ptr<const planner::ProjectInfo> &&project_info,
      oid_t bulk_insert_count = 1);

  /**
   * Insert one raw tuple into a table.
   * @param table             The table to insert into.
   * @param tuple             The tuple to insert.
   * @param bulk_insert_count The number of tuples to insert (1).
   */
  explicit InsertPlan(storage::DataTable *table,
                      std::unique_ptr<storage::Tuple> &&tuple,
                      oid_t bulk_insert_count = 1);

  /**
   * Insert one raw tuple into a table.
   * @param table_name        The name of the table to insert into.
   * @param tuple             The tuple to insert.
   * @param bulk_insert_count The number of tuples to insert (1).
   */
  explicit InsertPlan(std::string table_name,
                      std::unique_ptr<storage::Tuple> &&tuple,
                      oid_t bulk_insert_count = 1);

  explicit InsertPlan(
      storage::DataTable *table, std::vector<char *> *columns,
      std::vector<std::vector<peloton::expression::AbstractExpression *> *> *
          insert_values);

  // Get a varlen pool (will construct the pool only if needed)
  type::AbstractPool *GetPlanPool();

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::INSERT; }

  void SetParameterValues(std::vector<type::Value> *values);

  storage::DataTable *GetTable() const { return target_table_; }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  oid_t GetBulkInsertCount() const { return bulk_insert_count; }

  const storage::Tuple *GetTuple(int tuple_idx) const {
    if (tuple_idx >= (int)tuples_.size()) {
      return nullptr;
    }
    return tuples_[tuple_idx].get();
  }

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
  std::vector<std::unique_ptr<storage::Tuple>> tuples_;

  // <tuple_index, tuple_column_index, parameter_index>
  std::unique_ptr<std::vector<std::tuple<oid_t, oid_t, oid_t>>>
      parameter_vector_;

  // Parameter values
  std::unique_ptr<std::vector<type::Type::TypeId>> params_value_type_;

  /** @brief Number of times to insert */
  oid_t bulk_insert_count;

  // pool for variable length types
  std::unique_ptr<type::AbstractPool> pool_;

 private:
  DISALLOW_COPY_AND_MOVE(InsertPlan);
};

}  // namespace planner
}  // namespace peloton