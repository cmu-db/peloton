//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_iterator.cpp
//
// Identification: src/codegen/util/index_scan_iterator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/index_scan_iterator.h"
#include "common/logger.h"
#include "type/value_factory.h"
#include "storage/tuple.h"
#include <string.h>

namespace peloton {
namespace codegen {
namespace util {
IndexScanIterator::IndexScanIterator(index::Index *index,
                                     storage::Tuple *point_key_p,
                                     storage::Tuple *low_key_p,
                                     storage::Tuple *high_key_p) {
  index_ = index;
  if (point_key_p != nullptr) {
    is_point_query_ = true;
    is_full_scan_ = false;
    point_key_p_ = point_key_p;
  } else if (low_key_p != nullptr && high_key_p != nullptr) {
    is_point_query_ = false;
    is_full_scan_ = false;
    low_key_p_ = low_key_p;
    high_key_p_ = high_key_p;
  } else {
    is_point_query_ = false;
    is_full_scan_ = true;
  }
}

bool SortByTileId(ItemPointer *left, ItemPointer *right) {
  return (left->block < right->block ||
          (left->block == right->block && left->offset < right->offset));
}
void IndexScanIterator::DoScan() {
  LOG_TRACE("do scan in iterator");
  if (is_point_query_) {
    index_->ScanKey(point_key_p_, result_);
  } else if (is_full_scan_) {
    index_->CodeGenFullScan(result_);
  } else {
    index_->CodeGenRangeScan(low_key_p_, high_key_p_, result_);
  }
  LOG_TRACE("result size = %lu\n", result_.size());

  // TODO:
  // currently the RowBatch produced in the index scan only contains one
  // tuple because 1.the tuples in RowBatch have to be in the same tile
  // group 2.the result order of index scan has to follow the key order
  // 3.in most cases, index is built on random data so the probability
  // that two continuous result tuples are in the same tile group is low
  // potential optimization: in the index scan results, find out the
  // continuous result tuples that are in the same tile group (and have
  // ascending tile group offset) and produce one RowBatch for these tuples
}

// binary search to check whether the target offset is in the results
bool IndexScanIterator::RowOffsetInResult(uint64_t distinct_tile_index,
                                          uint32_t row_offset) {
  uint32_t l = result_metadata_[distinct_tile_index * 3 + 1];
  uint32_t r = result_metadata_[distinct_tile_index * 3 + 2];
  // binary search
  while (l < r) {
    uint32_t mid = l + (r - l) / 2;
    if (result_[mid]->offset == row_offset) {
      return true;
    }
    if (result_[mid]->offset > row_offset) {
      // right boundary exclusive?
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return false;
}

void IndexScanIterator::UpdateTupleWithInteger(
    int value, UNUSED_ATTRIBUTE int attribute_id, char *attribute_name,
    bool is_lower_key) {
  if (is_full_scan_) return;

  storage::Tuple *update_tuple =
      (is_point_query_) ? point_key_p_
                        : ((is_lower_key) ? low_key_p_ : high_key_p_);
  const std::vector<catalog::Column> &columns =
      update_tuple->GetSchema()->GetColumns();
  for (unsigned int i = 0; i < columns.size(); i++) {
    if (strcmp(columns[i].GetName().c_str(), attribute_name) == 0) {
      update_tuple->SetValue(
          i, peloton::type::ValueFactory::GetIntegerValue(value));
      break;
    }
  }
}

void IndexScanIterator::UpdateTupleWithBigInteger(
    int64_t value, UNUSED_ATTRIBUTE int attribute_id, char *attribute_name,
    bool is_lower_key) {
  if (is_full_scan_) return;

  storage::Tuple *update_tuple =
      (is_point_query_) ? point_key_p_
                        : ((is_lower_key) ? low_key_p_ : high_key_p_);
  const std::vector<catalog::Column> &columns =
      update_tuple->GetSchema()->GetColumns();
  for (unsigned int i = 0; i < columns.size(); i++) {
    if (strcmp(columns[i].GetName().c_str(), attribute_name) == 0) {
      update_tuple->SetValue(
          i, peloton::type::ValueFactory::GetBigIntValue(value));
      break;
    }
  }
}

void IndexScanIterator::UpdateTupleWithDouble(double value,
                                              UNUSED_ATTRIBUTE int attribute_id,
                                              char *attribute_name,
                                              bool is_lower_key) {
  if (is_full_scan_) return;

  storage::Tuple *update_tuple =
      (is_point_query_) ? point_key_p_
                        : ((is_lower_key) ? low_key_p_ : high_key_p_);
  const std::vector<catalog::Column> &columns =
      update_tuple->GetSchema()->GetColumns();
  for (unsigned int i = 0; i < columns.size(); i++) {
    if (strcmp(columns[i].GetName().c_str(), attribute_name) == 0) {
      update_tuple->SetValue(
          i, peloton::type::ValueFactory::GetDecimalValue(value));
      break;
    }
  }
}

void IndexScanIterator::UpdateTupleWithVarchar(
    char *value, UNUSED_ATTRIBUTE int attribute_id, char *attribute_name,
    bool is_lower_key) {
  if (is_full_scan_) return;

  storage::Tuple *update_tuple =
      (is_point_query_) ? point_key_p_
                        : ((is_lower_key) ? low_key_p_ : high_key_p_);
  const std::vector<catalog::Column> &columns =
      update_tuple->GetSchema()->GetColumns();
  for (unsigned int i = 0; i < columns.size(); i++) {
    if (strcmp(columns[i].GetName().c_str(), attribute_name) == 0) {
      update_tuple->SetValue(i, peloton::type::ValueFactory::GetVarcharValue(
                                    value, false, index_->GetPool()),
                             index_->GetPool());
      break;
    }
  }
}

void IndexScanIterator::UpdateTupleWithBoolean(
    bool value, UNUSED_ATTRIBUTE int attribute_id, char *attribute_name,
    bool is_lower_key) {
  if (is_full_scan_) return;

  storage::Tuple *update_tuple =
      (is_point_query_) ? point_key_p_
                        : ((is_lower_key) ? low_key_p_ : high_key_p_);
  const std::vector<catalog::Column> &columns =
      update_tuple->GetSchema()->GetColumns();
  for (unsigned int i = 0; i < columns.size(); i++) {
    if (strcmp(columns[i].GetName().c_str(), attribute_name) == 0) {
      update_tuple->SetValue(
          i, peloton::type::ValueFactory::GetBooleanValue(value));
      break;
    }
  }
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton