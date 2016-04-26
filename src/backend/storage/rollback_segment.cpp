//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_header.cpp
//
// Identification: src/backend/storage/tile_group_header.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/rollback_segment.h"

namespace peloton {
namespace storage {

/**
 * TODO: Optimization can be done. We can save copying those columns that are already
 * in the rollback segment created by the same transaction. What we need to do is to add
 * a bitmap or set in the rollback segment indicating columns it contains. After that we
 * can bypass these columns when making a new rollback segment.
 */
char * RollbackSegmentPool::GetSegmentFromTuple(const catalog::Schema *schema,
                                                const planner::ProjectInfo::TargetList &target_list,
                                                const AbstractTuple *tuple) {
  assert(schema);

  size_t col_count = target_list.size();
  size_t data_size = 0;
  char *rb_seg = nullptr;

  // First figure out the total size of the rollback segment
  for (auto target : target_list) {
    auto col_id = target.first;
    // Set the column bitmap and update the length of the data field
    data_size += schema->GetLength(col_id);
  }

  // Check if there is no need to generate a new segment
  if (col_count == 0) {
    return nullptr;
  }

  // Allocate the data
  assert(col_count > 0);
  size_t header_size = pairs_start_offset + col_count * sizeof(ColIdOffsetPair);
  rb_seg = (char*) pool_.AllocateZeroes(header_size + data_size);
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

    size_t col_length = schema->GetLength(col_id);
    size_t allocate_col_length = (is_inlined) ? col_length : schema->GetVariableLength(col_id);

    SetColIdOffsetPair(rb_seg, target.first, offset, idx);

    // Set the value
    char *value_location = GetColDataLocation(rb_seg, idx);
    Value value = tuple->GetValue(col_id);
    assert(schema->GetType(col_id) == value.GetValueType());
    value.SerializeToTupleStorageAllocateForObjects(
                value_location, is_inlined, allocate_col_length, is_inbytes, &pool_);

    // Update the offset
    offset += col_length;
  }

  return rb_seg;
}

Value RollbackSegmentPool::GetValue(char *rb_seg_ptr, const catalog::Schema *schema, int idx) {
  auto col_id = GetIdOffsetPair(rb_seg_ptr, idx)->col_id;

  const ValueType column_type = schema->GetType(col_id);
  const char *data_ptr = GetColDataLocation(rb_seg_ptr, idx);
  const bool is_inlined = schema->IsInlined(col_id);

  return Value::InitFromTupleStorage(data_ptr, column_type, is_inlined);
}

}  // End storage namespace
}  // End peloton namespace
