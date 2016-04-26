//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rollback_segment.cpp
//
// Identification: src/backend/storage/rollback_segment.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/rollback_segment.h"

namespace peloton {
namespace storage {

/**
 * @brief create a rollback segment by selecting columns from a tuple
 * @param target_list The columns to be selected
 * @param tuple The tuple to construct the RB
 *
 * TODO: Optimization can be done. We can save copying those columns that are already
 * in the rollback segment created by the same transaction. What we need to do is to add
 * a bitmap in the rollback segment indicating columns it contains. After that we
 * can bypass these columns when making a new rollback segment.
 */
RBSegType RollbackSegmentPool::CreateSegmentFromTuple(const catalog::Schema *schema,
                                                const planner::ProjectInfo::TargetList &target_list,
                                                const AbstractTuple *tuple) {
  LOG_INFO("Create RB Seg from tuple with %d columns", (int)target_list.size());
  assert(schema);
  assert(target_list.size() != 0); 

  size_t col_count = target_list.size();
  size_t header_size = pairs_start_offset + col_count * sizeof(ColIdOffsetPair);
  size_t data_size = 0;
  RBSegType rb_seg = nullptr;

  // First figure out the total size of the rollback segment data area
  for (auto &target : target_list) {
    auto col_id = target.first;
    data_size += schema->GetLength(col_id);
  }

  // Allocate the RBSeg
  rb_seg = (RBSegType)pool_.AllocateZeroes(header_size + data_size);
  assert(rb_seg);

  // Fill in the header
  SetNextPtr(rb_seg, nullptr);
  SetTimeStamp(rb_seg, MAX_CID);
  SetColCount(rb_seg, col_count);

  // Fill in the col_id & offset pair and set the data field
  size_t offset = 0;
  for (size_t idx = 0; idx < target_list.size(); ++idx) {
    auto &target = target_list[idx];
    auto col_id = target.first;

    const bool is_inlined = schema->IsInlined(col_id);
    const bool is_inbytes = false;

    size_t inline_col_size = schema->GetLength(col_id);
    size_t allocate_col_size = (is_inlined) ? inline_col_size : schema->GetVariableLength(col_id);

    SetColIdOffsetPair(rb_seg, target.first, offset, idx);

    // Set the value
    char *value_location = GetColDataLocation(rb_seg, idx);
    Value value = tuple->GetValue(col_id);
    assert(schema->GetType(col_id) == value.GetValueType());
    value.SerializeToTupleStorageAllocateForObjects(
                value_location, is_inlined, allocate_col_size, is_inbytes, &pool_);

    // Update the offset
    offset += inline_col_size;
  }

  return rb_seg;
}

/**
 * @brief Get a value of a column from the rollback segment
 *
 * @param idx The index of the recored column in the rollback segment
 */
Value RollbackSegmentPool::GetValue(RBSegType rb_seg, const catalog::Schema *schema, int idx) {
  auto col_id = GetIdOffsetPair(rb_seg, idx)->col_id;

  const ValueType column_type = schema->GetType(col_id);
  const char *data_ptr = GetColDataLocation(rb_seg, idx);
  const bool is_inlined = schema->IsInlined(col_id);

  return Value::InitFromTupleStorage(data_ptr, column_type, is_inlined);
}

}  // End storage namespace
}  // End peloton namespace
