//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_header.h
//
// Identification: src/backend/storage/tile_group_header.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/logger.h"
#include "backend/common/platform.h"
#include "backend/common/printable.h"
#include "backend/common/pool.h"
#include "backend/common/types.h"
#include "backend/logging/log_manager.h"
#include "backend/planner/project_info.h"

#include <atomic>
#include <mutex>
#include <cassert>
#include <unordered_map>

namespace peloton {

class VarlenPool;

namespace catalog {
  class Schema;
}

namespace storage {

class AbstractTable;
class Tuple;

struct ColIdOffsetPair {
  oid_t col_id;
  size_t offset;
};
/**
 *  Data layout:
 *  | next_seg_ptr (8 bytes) | timestamp (8 bytes) | column_count (8 bytes)
 *  | id_offset_pairs (column_count * 16 bytes) | segment data
 */


class RollbackSegmentPool {
  RollbackSegmentPool(const RollbackSegmentPool&) = delete;
  RollbackSegmentPool &operator=(const RollbackSegmentPool&) = delete;
public:
  RollbackSegmentPool(BackendType backend_type): pool_(backend_type){}
  RollbackSegmentPool(BackendType backend_type,
                      uint64_t allocation_size,
                      uint64_t max_chunk_count)
                      : pool_(backend_type, allocation_size, max_chunk_count){}
  ~RollbackSegmentPool() {}

  /**
   * Public Getters
   */

  inline char *GetNextPtr(char *rb_seg_ptr) const {
    return *(reinterpret_cast<char**>(rb_seg_ptr + next_ptr_offset_));
  }

  inline cid_t GetTimeStamp(char *rb_seg_ptr) const {
    return *(reinterpret_cast<cid_t*>(rb_seg_ptr + timestamp_offset_));
  }

  inline size_t GetColCount(char *rb_seg_ptr) const {
    return *(reinterpret_cast<size_t*>(rb_seg_ptr + col_count_offset_));
  }

  /**
   * Public setters
   */

  inline void SetNextPtr(char *rb_seg_ptr, char *next_seg) {
    *(reinterpret_cast<char**>(rb_seg_ptr + next_ptr_offset_)) = next_seg;
  }

  inline void SetTimeStamp(char *rb_seg_ptr, cid_t ts) {
    *(reinterpret_cast<cid_t*>(rb_seg_ptr + timestamp_offset_)) = ts;
  }

private:
  /**
   * Private utilities
   */
  inline ColIdOffsetPair *GetIdOffsetPair(char *rb_seg_ptr, int idx) {
    return (reinterpret_cast<ColIdOffsetPair*>(rb_seg_ptr + pairs_start_offset
                                               + sizeof(ColIdOffsetPair) * idx));
  }

  inline void SetColIdOffsetPair(char *rb_seg_ptr,
                                 size_t idx, oid_t col_id, size_t off) {
    auto pair = GetIdOffsetPair(rb_seg_ptr, idx);
    pair->col_id = col_id;
    pair->offset = off;
  }

  inline void SetColCount(char *rb_seg_ptr, size_t col_count) {
    *(reinterpret_cast<size_t*>(rb_seg_ptr + col_count_offset_)) = col_count;
  }

  inline char * GetDataPtr(char *rb_seg_ptr) {
    size_t col_count = GetColCount(rb_seg_ptr);
    return rb_seg_ptr + pairs_start_offset + col_count * sizeof(ColIdOffsetPair);
  }

  static const size_t next_ptr_offset_ = 0;
  static const size_t timestamp_offset_ = next_ptr_offset_ + sizeof(void*);
  static const size_t col_count_offset_ = timestamp_offset_ + sizeof(cid_t);
  static const size_t pairs_start_offset = col_count_offset_ + sizeof(size_t);

  // Get a prepared rollback segment from a tuple
  // Return nullptr if there is no need to generate a new segment
  char *GetSegmentFromTuple(const catalog::Schema *schema,
                            const planner::ProjectInfo::TargetList &target_list,
                            const AbstractTuple *tuple,
                            const ColBitmap *bitmap = nullptr);

private:
  VarlenPool pool_;
};

}  // End storage namespace
}  // End peloton namespace
