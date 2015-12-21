//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_join_executor.h
//
// Identification: src/backend/executor/abstract_join_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/schema.h"
#include "backend/executor/abstract_executor.h"
#include "backend/planner/project_info.h"

#include <vector>
#include <unordered_set>

namespace peloton {
namespace executor {

class AbstractJoinExecutor : public AbstractExecutor {
  AbstractJoinExecutor(const AbstractJoinExecutor &) = delete;
  AbstractJoinExecutor &operator=(const AbstractJoinExecutor &) = delete;

 public:
  explicit AbstractJoinExecutor(const planner::AbstractPlan *node,
                                ExecutorContext *executor_context);

  virtual ~AbstractJoinExecutor() {
  }

  const char *GetJoinTypeString() const {
    switch (join_type_) {
      case JOIN_TYPE_LEFT:
        return "JOIN_TYPE_LEFT";
      case JOIN_TYPE_RIGHT:
        return "JOIN_TYPE_RIGHT";
      case JOIN_TYPE_INNER:
        return "JOIN_TYPE_INNER";
      case JOIN_TYPE_OUTER:
        return "JOIN_TYPE_OUTER";
      case JOIN_TYPE_INVALID:
      default:
        return "JOIN_TYPE_INVALID";
    }
  }

 protected:
  /**
   * A RowSet is a set of oid_t representing rows that have no matching row
   * when doing joins. Half of rows in the set should be set
   * as NULL and output in corresponding types of joins.
   *
   * The number of tile should be identical to the number of row sets,
   * as a row can be uniquely identified by tile id and row oid.
   */
  typedef std::vector<std::unordered_set<oid_t> > RowSets;

  bool DInit();

  bool DExecute() = 0;

  //===--------------------------------------------------------------------===//
  // Helper functions
  //===--------------------------------------------------------------------===//

  // Build a join output logical tile
  std::unique_ptr<LogicalTile> BuildOutputLogicalTile(
      LogicalTile *left_tile,
      LogicalTile *right_tile);

  // Build the schema of the joined tile based on the projection info
  std::vector<LogicalTile::ColumnInfo> BuildSchema(
      std::vector<LogicalTile::ColumnInfo> &left,
      std::vector<LogicalTile::ColumnInfo> &right);

  // Build position lists
  std::vector<std::vector<oid_t> > BuildPostitionLists(
      LogicalTile *left_tile,
      LogicalTile *right_tile);


  void BufferLeftTile(LogicalTile *left_tile);
  void BufferRightTile(LogicalTile *right_tile);

  void UpdateJoinRowSets();
  void UpdateLeftJoinRowSets();
  void UpdateRightJoinRowSets();
  void UpdateFullJoinRowSets();

  /**
   * Record a matched left row, which should not be constructed
   * when building join outputs
   */
  inline void RecordMatchedLeftRow(size_t tile_idx, oid_t row_idx) {
    switch (join_type_) {
      case JOIN_TYPE_LEFT:
      case JOIN_TYPE_OUTER:
        no_matching_left_row_sets_[tile_idx].erase(row_idx);
        break;
      default:
        break;
    }
  }

  /**
   * Record a matched right row, which should not be constructed
   * when building join outputs
   */
  inline void RecordMatchedRightRow(size_t tile_idx, oid_t row_idx) {
    switch (join_type_) {
      case JOIN_TYPE_RIGHT:
      case JOIN_TYPE_OUTER:
        no_matching_right_row_sets_[tile_idx].erase(row_idx);
        break;
      default:
        break;
    }
  }


  bool BuildOuterJoinOutput();
  bool BuildLeftJoinOutPut();
  bool BuildRightJoinOutPut();

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of join. */
  std::vector<LogicalTile *> result;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Join predicate. */
  const expression::AbstractExpression *predicate_ = nullptr;

  /** @brief Projection info */
  const planner::ProjectInfo *proj_info_ = nullptr;

  /* @brief Join Type */
  PelotonJoinType join_type_ = JOIN_TYPE_INVALID;

  RowSets no_matching_left_row_sets_;
  RowSets no_matching_right_row_sets_;
  size_t left_matching_idx = 0;
  size_t right_matching_idx = 0;

  /* Buffer to store left child's result tiles */
  std::vector<std::unique_ptr<executor::LogicalTile> > left_result_tiles_;
  /* Buffer to store right child's result tiles */
  std::vector<std::unique_ptr<executor::LogicalTile> > right_result_tiles_;

  bool left_child_done_ = false;
  bool right_child_done_ = false;

};

}  // namespace executor
}  // namespace peloton
