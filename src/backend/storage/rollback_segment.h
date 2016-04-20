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

namespace catalog {
  class Schema;
}

namespace storage {

class AbstractTable;
class Tuple;

class RollbackSegment {
  friend class RollbackSegmentManager;

  RollbackSegment &operator=(const RollbackSegment &) = delete;
  RollbackSegment(const RollbackSegment &) = delete;

private:
  RollbackSegment()
    :begin_cid_(INVALID_CID), end_cid_(INVALID_CID), col_offset_map_(),
    next_segment_(), data_(nullptr) {}

public:
  ~RollbackSegment(){
    if (data_ != nullptr) delete data_;
  }

  inline bool HasColumn(oid_t col_id) const {
    return col_offset_map_.find(col_id) != col_offset_map_.end();
  }

  inline void SetBeginCommitId(const oid_t &begin_cid) {
    begin_cid_ = begin_cid;
  }

  inline void SetEndCommitId(const oid_t &end_cid) {
    end_cid_ = end_cid;
  }

  inline cid_t GetBeginCommitId() const {
    return begin_cid_;
  }

  inline cid_t GetEndCommitId() const {
    return end_cid_;
  }

  void SetSegmentValue(const catalog::Schema *schema, const oid_t col_id, const Value &value, VarlenPool *data_pool);

  Value GetSegmentValue(const catalog::Schema *schema, const oid_t col_id) const;

private:
  // begin and end timestamps
  cid_t begin_cid_;
  cid_t end_cid_;

  // column offset map
  // col_id -> col offset count from the header
  std::unordered_map<oid_t, size_t> col_offset_map_;

  // Next rollback segment
  std::shared_ptr<RollbackSegment> next_segment_;

  // Data
  char *data_;
};

class RollbackSegmentManager {
  RollbackSegmentManager(const RollbackSegmentManager&) = delete;
  RollbackSegmentManager &operator=(const RollbackSegmentManager&) = delete;
public:
  RollbackSegmentManager() {}
  ~RollbackSegmentManager() {}

  RollbackSegment *GetRollbackSegment(const catalog::Schema *schema,
                                      const peloton::planner::ProjectInfo::TargetList &target_list);
};

}  // End storage namespace
}  // End peloton namespace
