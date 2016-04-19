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
namespace storage {

class AbstractTable;
class Tuple;

class RollbackSegmentHeader {
  friend class RollbackSegmentManager;

  RollbackSegmentHeader &operator=(const RollbackSegmentHeader &) = delete;
  RollbackSegmentHeader(const RollbackSegmentHeader &) = delete;

private:
  RollbackSegmentHeader()
    :begin_cid_(INVALID_CID), end_cid_(INVALID_CID), col_offset_map_()
  {}

  inline bool HasColumn(oid_t col_id) {
    return col_offset_map_.find(col_id) != col_offset_map_.end();
  }

public:
  ~RollbackSegmentHeader(){}

private:
  // begin and end timestamps
  cid_t begin_cid_;
  cid_t end_cid_;

  // column offset map
  // col_id -> col offset count from the header
  std::unordered_map<oid_t, size_t> col_offset_map_;

};

class RollbackSegmentManager {
  RollbackSegmentManager() = delete;
  RollbackSegmentManager(const RollbackSegmentManager&) = delete;
  RollbackSegmentManager &operator=(const RollbackSegmentManager&) = delete;
public:
  RollbackSegmentManager(BackendType backend_type, AbstractTable *table)
    :table_(table) {
    // TODO: tune the setting of the pool
    pool_ = new VarlenPool(backend_type);
  }

  ~RollbackSegmentManager() {
    delete pool_;
  }

  RollbackSegmentHeader *GetRollbackSegment(const peloton::planner::ProjectInfo::TargetList &target_list);

  void SetSegmentValue(RollbackSegmentHeader *rb_header,const oid_t col_id, const Value &value, VarlenPool *data_pool);

  Value GetSegmentValue(RollbackSegmentHeader *rb_header, const oid_t col_id) const;

  inline void SetBeginCommitId(RollbackSegmentHeader *rb_header, const oid_t &begin_cid) {
    rb_header->begin_cid_ = begin_cid;
  }

  inline void SetEndCommitId(RollbackSegmentHeader *rb_header, const oid_t &end_cid) {
    rb_header->end_cid_ = end_cid;
  }

  inline cid_t GetBeginCommitId(const RollbackSegmentHeader *rb_header) {
    return rb_header->begin_cid_;
  }

  inline cid_t GetEndCommitId(const RollbackSegmentHeader *rb_header) {
    return rb_header->end_cid_;
  }


private:
  // abstract table
  AbstractTable *table_;

  // varlen pool
  VarlenPool *pool_;
};

}  // End storage namespace
}  // End peloton namespace
