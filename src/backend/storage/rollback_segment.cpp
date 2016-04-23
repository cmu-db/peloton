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
#include "backend/storage/tuple.h"
#include "backend/storage/abstract_table.h"
#include "backend/catalog/schema.h"

namespace peloton {
namespace storage {

char * RollbackSegmentPool::GetSegmentFromTuple(const catalog::Schema *schema,
                                                const planner::ProjectInfo::TargetList &target_list,
                                                const AbstractTuple *tuple,
                                                const ColBitmap *bitmap) {
  assert(schema);

  size_t col_count = target_list.size();
  size_t data_size = 0;
  char *rb_seg = nullptr;

  // First figure out the total size of the rollback segment
  for (auto target : target_list) {
    auto col_id = target.first;

    if (bitmap != nullptr && bitmap->test(col_id)) {
      // By pass already updated column
      col_count -= 1;
      continue;
    }
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
  SetTimeStamp(rb_seg, INVALID_CID);
  SetColCount(rb_seg, col_count);

  char *data_location = GetDataPtr(rb_seg);

  // Fill in the col_id & offset pair and set the data field
  size_t offset = 0;
  for (size_t idx = 0; idx < target_list.size(); ++idx) {
    auto &target = target_list[idx];
    auto col_id = target.first;

    if (bitmap != nullptr && bitmap->test(col_id)) {
      // By pass already updated column
      continue;
    }

    const bool is_inlined = schema->IsInlined(col_id);
    const bool is_inbytes = false;

    char *value_location = data_location + offset;
    size_t col_length = schema->GetLength(col_id);
    size_t allocate_col_length = (is_inlined) ? col_length : schema->GetVariableLength(col_id);

    SetColIdOffsetPair(rb_seg, target.first, offset, idx);

    // Set the value
    Value value = tuple->GetValue(col_id);
    assert(schema->GetType(col_id) == value.GetValueType());
    value.SerializeToTupleStorageAllocateForObjects(
                value_location, is_inlined, allocate_col_length, is_inbytes, &pool_);

    // Update the offset
    offset += col_length;
  }

  return rb_seg;
}

}  // End storage namespace
}  // End peloton namespace
