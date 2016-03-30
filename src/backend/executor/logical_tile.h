//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_tile.h
//
// Identification: src/backend/executor/logical_tile.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iterator>
#include <vector>
#include <memory>

#include "backend/common/printable.h"
#include "backend/common/types.h"

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {
class Tile;
class TileGroup;
}

namespace executor {

//===--------------------------------------------------------------------===//
// Logical Tile
//===--------------------------------------------------------------------===//

/**
 * Represents a Logical Tile that can represent columns in many Physical Tiles.
 *
 * LT :: <C1, C2>
 * C1 :: col 5 in PT 5
 * C2 :: col 3 in PT 9 ...
 *
 * LogicalTiles are only instantiated via LogicalTileFactory.
 */
class LogicalTile : public Printable {
  friend class LogicalTileFactory;

 public:
  struct ColumnInfo;

  /* A vector of position to represent a column */
  typedef std::vector<oid_t> PositionList;

  /* A vector of column to represent a tile */
  typedef std::vector<PositionList> PositionLists;

  LogicalTile(const LogicalTile &) = delete;
  LogicalTile &operator=(const LogicalTile &) = delete;
  LogicalTile(LogicalTile &&) = delete;
  LogicalTile &operator=(LogicalTile &&) = delete;

  ~LogicalTile();

  void AddColumn(const std::shared_ptr<storage::Tile> &base_tile,
                 oid_t origin_column_id, oid_t position_list_idx);

  void AddColumns(const std::shared_ptr<storage::TileGroup> &tile_group,
                  const std::vector<oid_t> &column_ids);

  void ProjectColumns(const std::vector<oid_t> &original_column_ids,
                      const std::vector<oid_t> &column_ids);

  int AddPositionList(PositionList &&position_list);

  void RemoveVisibility(oid_t tuple_id);

  storage::Tile *GetBaseTile(oid_t column_id);

  Value GetValue(oid_t tuple_id, oid_t column_id);

  size_t GetTupleCount();

  size_t GetColumnCount();

  const std::vector<ColumnInfo> &GetSchema() const;

  const ColumnInfo &GetColumnInfo(const oid_t column_id) const;

  catalog::Schema *GetPhysicalSchema() const;

  void SetSchema(std::vector<LogicalTile::ColumnInfo> &&schema);

  const PositionLists &GetPositionLists() const;

  const PositionList &GetPositionList(const oid_t column_id) const;

  void SetPositionLists(PositionLists &&position_lists);

  void SetPositionListsAndVisibility(PositionLists &&position_lists);

  // Get a string representation for debugging
  const std::string GetInfo() const;

  //===--------------------------------------------------------------------===//
  // Logical Tile Iterator
  //===--------------------------------------------------------------------===//

  /**
   * @brief Iterates through tuple ids in this logical tile.
   *
   * This provides easy access to tuples that are visible.
   */
  class iterator : public std::iterator<std::input_iterator_tag, oid_t> {
    // It's a friend so it can call this iterator's private constructor.
    friend class LogicalTile;

   public:
    iterator &operator++();

    iterator operator++(int);

    bool operator==(const iterator &rhs);

    bool operator!=(const iterator &rhs);

    oid_t operator*();

   private:
    iterator(LogicalTile *tile, bool begin);

    /** @brief Keeps track of position of iterator. */
    oid_t pos_;

    /** @brief Tile that this iterator is iterating over. */
    LogicalTile *tile_;
  };

  iterator begin();

  iterator end();

  //===--------------------------------------------------------------------===//
  // Column Info
  //===--------------------------------------------------------------------===//

  /** @brief Column metadata for logical tile */
  struct ColumnInfo {
    /** @brief Position list in logical tile that will correspond to this
     * column. */
    oid_t position_list_idx;

    /**
     * @brief Pointer to base tile that column is from.
     * IMPORTANT: We use a pointer instead of the oid of the tile to minimize
     * indirection.
     */
    std::shared_ptr<storage::Tile> base_tile;

    /** @brief Original column id of this logical tile column in its associated
     * base tile. */
    oid_t origin_column_id;
  };

  //===--------------------------------------------------------------------===//
  // Position Lists Builder
  //===--------------------------------------------------------------------===//
  class PositionListsBuilder {
   public:
    PositionListsBuilder();

    PositionListsBuilder(LogicalTile *left_tile, LogicalTile *right_tile);

    PositionListsBuilder(const PositionLists *left_pos_list,
                         const PositionLists *right_pos_list);

    inline void SetLeftSource(const PositionLists *left_source) {
      left_source_ = left_source;
    }

    inline void SetRightSource(const PositionLists *right_source) {
      right_source_ = right_source;
    }

    inline void AddRow(size_t left_itr, size_t right_itr) {
      assert(!invalid_);
      // First, copy the elements in left logical tile's tuple
      for (size_t output_tile_column_itr = 0;
           output_tile_column_itr < left_source_->size();
           output_tile_column_itr++) {
        output_lists_[output_tile_column_itr].push_back(
            (*left_source_)[output_tile_column_itr][left_itr]);
      }

      // Then, copy the elements in right logical tile's tuple
      for (size_t output_tile_column_itr = 0;
           output_tile_column_itr < right_source_->size();
           output_tile_column_itr++) {
        output_lists_[left_source_->size() + output_tile_column_itr].push_back(
            (*right_source_)[output_tile_column_itr][right_itr]);
      }
    }

    inline void AddLeftNullRow(size_t right_itr) {
      assert(!invalid_);
      // Determine the number of null position list on the left
      oid_t left_pos_list_size;
      if (left_source_ == nullptr) {
        left_pos_list_size = 1;
      } else {
        left_pos_list_size = left_source_->size();
      }

      // First, copy the elements in left logical tile's tuple
      for (size_t output_tile_column_itr = 0;
           output_tile_column_itr < left_pos_list_size;
           output_tile_column_itr++) {
        output_lists_[output_tile_column_itr].push_back(NULL_OID);
      }

      // Then, copy the elements in right logical tile's tuple
      for (size_t output_tile_column_itr = 0;
           output_tile_column_itr < right_source_->size();
           output_tile_column_itr++) {
        output_lists_[left_pos_list_size + output_tile_column_itr].push_back(
            (*right_source_)[output_tile_column_itr][right_itr]);
      }
    }

    inline void AddRightNullRow(size_t left_itr) {
      assert(!invalid_);
      // Determine the number of null position list on the right
      oid_t right_pos_list_size;
      if (right_source_ == nullptr) {
        right_pos_list_size = 1;
      } else {
        right_pos_list_size = right_source_->size();
      }

      // First, copy the elements in left logical tile's tuple
      for (size_t output_tile_column_itr = 0;
           output_tile_column_itr < left_source_->size();
           output_tile_column_itr++) {
        output_lists_[output_tile_column_itr].push_back(
            (*left_source_)[output_tile_column_itr][left_itr]);
      }

      // Then, copy the elements in right logical tile's tuple
      for (size_t output_tile_column_itr = 0;
           output_tile_column_itr < right_pos_list_size;
           output_tile_column_itr++) {
        output_lists_[left_source_->size() + output_tile_column_itr].push_back(
            NULL_OID);
      }
    }

    inline PositionLists &&Release() {
      invalid_ = true;
      return std::move(output_lists_);
    }

    inline size_t Size() const {
      if (output_lists_.size() >= 1) return output_lists_[0].size();
      return 0;
    }

   private:
    const PositionLists *left_source_ = nullptr;
    const PositionLists *right_source_ = nullptr;
    PositionLists output_lists_;
    bool invalid_ = false;
  };

 private:
  // Dummy default constructor
  LogicalTile(){};

  /**
   * @brief Mapping of column ids in this logical tile to the underlying
   *        position lists and columns in base tiles.
   */
  std::vector<ColumnInfo> schema_;

  /**
   * @brief Lists of position lists.
   * Each list contains positions corresponding to particular tiles/columns.
   */
  PositionLists position_lists_;

  /**
   * @brief Bit-vector storing visibility of each row in the position lists.
   * Used to cheaply invalidate rows of positions.
   */
  std::vector<bool> visible_rows_;

  /** @brief Total # of allocated slots in the logical tile **/
  oid_t total_tuples_ = 0;

  /** @brief Keeps track of the number of tuples that are still visible. */
  oid_t visible_tuples_ = 0;
};

}  // namespace executor
}  // namespace peloton
