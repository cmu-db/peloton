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

typedef char* RBSegType;

/* @brief per transaction rollback segment pool. Rollback segments genereted
 * by each transaction will be allocated from its own pool. The responsibility 
 * of the RollbackSegmentPool is data (de)allocation, and garbage collection.
 */
class RollbackSegmentPool {
  RollbackSegmentPool(const RollbackSegmentPool&) = delete;
  RollbackSegmentPool &operator=(const RollbackSegmentPool&) = delete;
public:
  /**
    *  Data layout:
    *  | next_seg_ptr (8 bytes) | timestamp (8 bytes) | column_count (8 bytes)
    *  | id_offset_pairs (column_count * 16 bytes) | segment data
    */
  static const size_t next_ptr_offset_ = 0;
  static const size_t timestamp_offset_ = next_ptr_offset_ + sizeof(void*);
  static const size_t col_count_offset_ = timestamp_offset_ + sizeof(cid_t);
  static const size_t pairs_start_offset = col_count_offset_ + sizeof(size_t);

  RollbackSegmentPool(BackendType backend_type):
    pool_(backend_type), tombstone_(false), timestamp_(MAX_CID) {}

  RollbackSegmentPool(BackendType backend_type,
                      uint64_t allocation_size,
                      uint64_t max_chunk_count)
                      : pool_(backend_type, allocation_size, max_chunk_count),
                        tombstone_(false),
                        timestamp_(MAX_CID) {}

  // pool_ will be deallocated here, so all space will be reclaimed
  ~RollbackSegmentPool() {}

  /**
   * Public Getters
   */
  inline static char *GetNextPtr(char *rb_seg_ptr) {
    return *(reinterpret_cast<char**>(rb_seg_ptr + next_ptr_offset_));
  }

  // The semantics of timestamp on rollback segment:
  //    The timestamp of a rollback segment stands for its "end timestamp".
  //    The "start timestamp" of a rollback segment should be discovered from 
  //    the next rollback segment
  inline static cid_t GetTimeStamp(char *rb_seg_ptr) {
    return *(reinterpret_cast<cid_t*>(rb_seg_ptr + timestamp_offset_));
  }

  inline static size_t GetColCount(const char *rb_seg_ptr) {
    return *(reinterpret_cast<const size_t*>(rb_seg_ptr + col_count_offset_));
  }

  inline static ColIdOffsetPair *GetIdOffsetPair(char *rb_seg_ptr, int idx) {
    assert(idx >= 0);
    return (reinterpret_cast<ColIdOffsetPair*>(rb_seg_ptr + pairs_start_offset
                                               + sizeof(ColIdOffsetPair) * idx));
  }

  inline cid_t GetPoolTimestamp() const {
    return timestamp_;
  }

  inline bool IsMarkedAsGarbage() const {
    return tombstone_;
  }

  inline static char * GetDataLocation(char *rb_seg_ptr) {
    size_t col_count = GetColCount(rb_seg_ptr);
    return rb_seg_ptr + pairs_start_offset + col_count * sizeof(ColIdOffsetPair);
  }

  inline static char *GetColDataLocation(char *rb_seg_ptr, int idx) {
    auto offset = GetIdOffsetPair(rb_seg_ptr, idx)->offset;
    return GetDataLocation(rb_seg_ptr) + offset;
  }

  // Get the value on the rollback segment by the table's schema and the idx 
  // within this rollback segment
  static Value GetValue(char *rb_seg_ptr, const catalog::Schema *schema, int idx);
  /**
   * Public setters
   */

  inline static void SetNextPtr(char *rb_seg_ptr, const char *next_seg) {
    *(reinterpret_cast<const char**>(rb_seg_ptr + next_ptr_offset_)) = next_seg;
  }

  inline static void SetTimeStamp(char *rb_seg_ptr, cid_t ts) {
    *(reinterpret_cast<cid_t*>(rb_seg_ptr + timestamp_offset_)) = ts;
  }

  inline void SetPoolTimestamp(const cid_t ts) {
    timestamp_ = ts;
  }

  // FIXME: should set timestamp_ as the next commit id here
  inline void MarkedAsGarbage() {
    tombstone_ = true;
  }

  // Get a prepared rollback segment from a tuple
  // TODO: Return nullptr if there is no need to generate a new segment
  char *CreateSegmentFromTuple(const catalog::Schema *schema,
                            const planner::ProjectInfo::TargetList &target_list,
                            const AbstractTuple *tuple);

  inline static void SetColIdOffsetPair(char *rb_seg_ptr,
                                 size_t idx, oid_t col_id, size_t off) {
    auto pair = GetIdOffsetPair(rb_seg_ptr, idx);
    pair->col_id = col_id;
    pair->offset = off;
  }

private:
  VarlenPool pool_;
  // If the pool has been marked as garbage
  bool tombstone_;
  // When tombstone_ is false, timestamp_ is the end commit time of associated
  // Transaction. When tombstone_ is true, timestamp_ is the time when the pool
  // is marked as garbage
  cid_t timestamp_;

  inline static void SetColCount(char *rb_seg_ptr, size_t col_count) {
    *(reinterpret_cast<size_t*>(rb_seg_ptr + col_count_offset_)) = col_count;
  }
};

}  // End storage namespace
}  // End peloton namespace
