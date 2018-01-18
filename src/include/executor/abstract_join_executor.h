//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_join_executor.h
//
// Identification: src/include/executor/abstract_join_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <unordered_set>

#include "catalog/schema.h"
#include "executor/abstract_executor.h"
#include "planner/project_info.h"

namespace peloton {
namespace executor {

/**
 * Base class for all join algorithm implementations.
 * This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class AbstractJoinExecutor : public AbstractExecutor {
  AbstractJoinExecutor(const AbstractJoinExecutor &) = delete;
  AbstractJoinExecutor &operator=(const AbstractJoinExecutor &) = delete;

 public:
  explicit AbstractJoinExecutor(const planner::AbstractPlan *node,
                                ExecutorContext *executor_context);

  virtual ~AbstractJoinExecutor() {}

  const char *GetJoinTypeString() const {
    switch (join_type_) {
      case JoinType::LEFT:
        return "JoinType::LEFT";
      case JoinType::RIGHT:
        return "JoinType::RIGHT";
      case JoinType::INNER:
        return "JoinType::INNER";
      case JoinType::OUTER:
        return "JoinType::OUTER";
      case JoinType::INVALID:
      default:
        return "JoinType::INVALID";
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
   * For example, if we get 3 result tiles from a child, the row sets
   * [ <oid_t1, oid_t2, oid_t4>, <oid_t6, oid_t9>, </> ]
   * represents that rows oid_t1, oid_t2 and oid_4 from the first tile
   * don't have matched row; row oid_t6 and oid_t9 of the second tile
   * don't have matched row; every row in the last tile has matched row.
   *
   */
  typedef std::vector<std::unordered_set<oid_t>> RowSets;

  bool DInit();

  bool DExecute() = 0;

  //===--------------------------------------------------------------------===//
  // Helper functions
  //===--------------------------------------------------------------------===//

  // Build a join output logical tile
  std::unique_ptr<LogicalTile> BuildOutputLogicalTile(LogicalTile *left_tile,
                                                      LogicalTile *right_tile);

  // Build the joined tile with one of the child tile and the schema
  // of the result tile
  std::unique_ptr<LogicalTile> BuildOutputLogicalTile(
      LogicalTile *left_tile, LogicalTile *right_tile,
      const catalog::Schema *output_schema);

  // Build the schema of the joined tile based on the projection info
  std::vector<LogicalTile::ColumnInfo> BuildSchema(
      std::vector<LogicalTile::ColumnInfo> &left,
      std::vector<LogicalTile::ColumnInfo> &right);

  // build the column information from the right tile and the output schema
  std::vector<LogicalTile::ColumnInfo> BuildSchemaFromRightTile(
      const std::vector<LogicalTile::ColumnInfo> *right_schema,
      const catalog::Schema *output_schema);

  // build the column information from the left tile and the output schema
  std::vector<LogicalTile::ColumnInfo> BuildSchemaFromLeftTile(
      const std::vector<LogicalTile::ColumnInfo> *left_schema,
      const catalog::Schema *output_schema, oid_t left_pos_list_count);

  // Build position lists
  std::vector<std::vector<oid_t>> BuildPostitionLists(LogicalTile *left_tile,
                                                      LogicalTile *right_tile);

  void BufferLeftTile(LogicalTile *left_tile);
  void BufferRightTile(LogicalTile *right_tile);

  void UpdateJoinRowSets();
  void UpdateLeftJoinRowSets();
  void UpdateRightJoinRowSets();
  void UpdateFullJoinRowSets();

  inline LogicalTile *GetNonEmptyTile(LogicalTile *left_tile,
                                      LogicalTile *right_tile) {
    LogicalTile *non_empty_tile = nullptr;
    if (left_tile == nullptr) {
      non_empty_tile = right_tile;
    }
    if (right_tile == nullptr) {
      non_empty_tile = left_tile;
    }
    PL_ASSERT(non_empty_tile != nullptr);
    return non_empty_tile;
  }

  /**
   * Record a matched left row, which should not be constructed
   * when building join outputs
   */
  inline void RecordMatchedLeftRow(size_t tile_idx, oid_t row_idx) {
    switch (join_type_) {
      case JoinType::LEFT:
      case JoinType::OUTER:
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
      case JoinType::RIGHT:
      case JoinType::OUTER:
        no_matching_right_row_sets_[tile_idx].erase(row_idx);
        break;
      default:
        break;
    }
  }

  bool BuildOuterJoinOutput();
  bool BuildLeftJoinOutput();
  bool BuildRightJoinOutput();

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

  /** @brief Join Type */
  JoinType join_type_ = JoinType::INVALID;

  /** @brief Schema of the output tile before projection */
  const catalog::Schema *proj_schema_ = nullptr;

  /** @brief Left and right row sets corresponding to tuples with no matching
   * counterpart */
  RowSets no_matching_left_row_sets_;
  RowSets no_matching_right_row_sets_;

  /** @brief Left and right matching iterator */
  size_t left_matching_idx = 0;
  size_t right_matching_idx = 0;

  /** @brief Buffer to store left child's result tiles */
  std::vector<std::unique_ptr<executor::LogicalTile>> left_result_tiles_;

  /** @brief Buffer to store right child's result tiles */
  std::vector<std::unique_ptr<executor::LogicalTile>> right_result_tiles_;

  bool left_child_done_ = false;
  bool right_child_done_ = false;
};

}  // namespace executor
}  // namespace peloton
