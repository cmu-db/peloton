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

namespace peloton {
namespace storage {

RollbackSegmentHeader *RollbackSegmentManager::GetRollbackSegment(
  const peloton::planner::ProjectInfo::TargetList &target_list) {

  // Figure out the size of the data field
  size_t data_size = 0;

  for (auto target : target_list) {
    auto col_id = target.first;
    data_size += table_->GetSchema()->GetLength(col_id);
  }

  // Allocate space from the varlen pool
  size_t allocate_size = sizeof(RollbackSegmentHeader) + data_size;
  char *raw_rollback_seg = reinterpret_cast<char*>(pool_->Allocate(allocate_size));
  std::memset(raw_rollback_seg, 0, allocate_size);

  // Make a header
  new (raw_rollback_seg) RollbackSegmentHeader();
  RollbackSegmentHeader *rb_seg_header = reinterpret_cast<RollbackSegmentHeader *>(raw_rollback_seg);

  // Set the col map
  size_t offset = sizeof(RollbackSegmentHeader);
  for (auto target : target_list) {
    auto col_id = target.first;
    rb_seg_header->col_offset_map_[target.first] = offset;
    offset += table_->GetSchema()->GetLength(col_id);
  }

  return rb_seg_header;
}

void RollbackSegmentManager::SetSegmentValue(RollbackSegmentHeader *rb_header, const oid_t col_id,
                                             const Value &value, VarlenPool *data_pool) {
  assert(rb_header);
  assert(rb_header->HasColumn(col_id));

  auto schema = table_->GetSchema();
  const ValueType type  = schema->GetType(col_id);
  const bool is_inlined = schema->IsInlined(col_id);

  char *raw_rollback_seg = reinterpret_cast<char*>(rb_header);
  char *value_location = raw_rollback_seg + rb_header->col_offset_map_[col_id];
  int32_t column_length = schema->GetLength(col_id);

  if (is_inlined == false)
    column_length = schema->GetVariableLength(col_id);

  const bool is_in_bytes = false;

  // Allocate in heap or given data pool depending on whether a pool is provided
  if (data_pool == nullptr) {
    // Skip casting if type is same
    if (type == value.GetValueType()) {
      value.SerializeToTupleStorage(value_location, is_inlined, column_length,
                                    is_in_bytes);
    } else {
      Value casted_value = value.CastAs(type);
      casted_value.SerializeToTupleStorage(value_location, is_inlined,
                                           column_length, is_in_bytes);
      // Do not clean up immediately
      casted_value.SetCleanUp(false);
    }
  } else {
    // Skip casting if type is same
    if (type == value.GetValueType()) {
      value.SerializeToTupleStorageAllocateForObjects(
        value_location, is_inlined, column_length, is_in_bytes, data_pool);
    } else {
      value.CastAs(type).SerializeToTupleStorageAllocateForObjects(
        value_location, is_inlined, column_length, is_in_bytes, data_pool);
    }
  }
}

Value RollbackSegmentManager::GetSegmentValue(RollbackSegmentHeader *rb_header, const oid_t col_id) const {
  assert(rb_header);
  assert(rb_header->HasColumn(col_id));

  auto schema = table_->GetSchema();

  const ValueType type = schema->GetType(col_id);

  const char *raw_rollback_seg = reinterpret_cast<char*>(rb_header);
  const char *data_ptr = raw_rollback_seg + rb_header->col_offset_map_[col_id];
  const bool is_inlined = schema->IsInlined(col_id);

  return Value::InitFromTupleStorage(data_ptr, type, is_inlined);
}

}  // End storage namespace
}  // End peloton namespace
