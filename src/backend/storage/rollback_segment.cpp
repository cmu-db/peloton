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

RollbackSegment *RollbackSegmentManager::GetRollbackSegment(
  const catalog::Schema *schema,
  const peloton::planner::ProjectInfo::TargetList &target_list) {

  RollbackSegment *rb_seg = new RollbackSegment();
  assert(rb_seg);

  // Figure out the size of the data field
  size_t data_size = 0;
  for (auto target : target_list) {
    auto col_id = target.first;

    // set the col map
    rb_seg->col_offset_map_[col_id] = data_size;

    data_size += schema->GetLength(col_id);
  }

  // Allocate space for the data
  rb_seg->data_ = reinterpret_cast<char*>(::operator new(data_size));
  std::memset(rb_seg->data_, 0, data_size);

  return rb_seg;
}

void RollbackSegment::SetSegmentValue(const catalog::Schema *schema, const oid_t col_id,
                                             const Value &value, VarlenPool *data_pool) {
  assert(HasColumn(col_id));

  const ValueType type  = schema->GetType(col_id);
  const bool is_inlined = schema->IsInlined(col_id);

  char *value_location = data_ + col_offset_map_[col_id];
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

Value RollbackSegment::GetSegmentValue(const catalog::Schema *schema,
                                       const oid_t col_id) const {
  assert(HasColumn(col_id));

  const ValueType type = schema->GetType(col_id);

  const char *data_ptr = data_ + col_offset_map_[col_id];
  const bool is_inlined = schema->IsInlined(col_id);

  return Value::InitFromTupleStorage(data_ptr, type, is_inlined);
}

}  // End storage namespace
}  // End peloton namespace
