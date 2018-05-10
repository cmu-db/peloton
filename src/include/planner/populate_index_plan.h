//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// populate_index_plan.h
//
// Identification: src/include/planner/populate_index_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

namespace peloton {

namespace storage {
class DataTable;
// class Tuple;
}

namespace planner {

/**
 * IMPORTANT All tiles got from child must have the same physical schema.
 */

class PopulateIndexPlan : public AbstractPlan {
 public:
  PopulateIndexPlan(const PopulateIndexPlan &) = delete;
  PopulateIndexPlan &operator=(const PopulateIndexPlan &) = delete;
  PopulateIndexPlan(const PopulateIndexPlan &&) = delete;
  PopulateIndexPlan &operator=(const PopulateIndexPlan &&) = delete;

  explicit PopulateIndexPlan(storage::DataTable *table,
                             std::vector<oid_t> column_ids, std::string index_name, bool concurrent);

  inline PlanNodeType GetPlanNodeType() const {
    return PlanNodeType::POPULATE_INDEX;
  }

  inline const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  const std::string GetInfo() const { return "PopulateIndex"; }

  storage::DataTable *GetTable() const { return target_table_; }

  const std::string GetIndexName() const { return index_name_; }

  bool GetConcurrent() const{ return concurrent_; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(
        new PopulateIndexPlan(target_table_, column_ids_, index_name_, concurrent_));
  }

 private:
  /** @brief Target table. */
  storage::DataTable *target_table_ = nullptr;
  /** @brief Column Ids. */
  std::vector<oid_t> column_ids_;
  /** @brief Index name */
  std::string index_name_;
  /** @brief If the index is to be created concurrently */
  bool concurrent_;

};
}
}
