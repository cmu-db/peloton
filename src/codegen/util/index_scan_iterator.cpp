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

namespace peloton {
namespace codegen {
namespace util {
IndexScanIterator::IndexScanIterator(index::Index *index, storage::Tuple *low_key_p, storage::Tuple *high_key_p)
  : index_(index), low_key_p_(low_key_p), high_key_p_(high_key_p) {}

bool SortByTileId(ItemPointer *left, ItemPointer *right) {
  return (left->block < right->block || (left->block == right->block && left->offset < right->offset));
}
void IndexScanIterator::DoScan() {
  LOG_INFO("do scan in iterator");
  index_->CodeGenRangeScan(low_key_p_, high_key_p_, result_);
  LOG_INFO("result size = %lu\n", result_.size());
  // debug
  printf("before sorting:\n");
  for (std::vector<ItemPointer *>::iterator it = result_.begin(); it != result_.end(); ++it) {
    printf("%u %u\n", (*it)->block, (*it)->offset);
  }
  // debug
  std::sort(result_.begin(), result_.end(), SortByTileId);
  // debug
  printf("after sorting:\n");
  for (std::vector<ItemPointer *>::iterator it = result_.begin(); it != result_.end(); ++it) {
    printf("%u %u\n", (*it)->block, (*it)->offset);
  }
  // debug

  // find all distinct tile group id and their start/end position in result vector
  distinct_tile_group_num_ = 0;
  if (result_.size() > 0) {
    uint32_t previous_tile_group_id = result_[0]->block;
    uint32_t begin = 0, iter = 0;
    while (iter < result_.size() && result_[iter]->block == previous_tile_group_id) {
      ++iter;
    }
    while (iter < result_.size()) {
      if (result_[iter]->block != previous_tile_group_id) {
        result_metadata_.push_back(previous_tile_group_id);
        result_metadata_.push_back(begin);
        result_metadata_.push_back(iter);
        distinct_tile_group_num_++;

        previous_tile_group_id = result_[iter]->block;
        begin = iter;
      }
      ++iter;
    }
    // don't forget the last one
    result_metadata_.push_back(previous_tile_group_id);
    result_metadata_.push_back(begin);
    result_metadata_.push_back(iter);
    distinct_tile_group_num_++;
  }
}

bool IndexScanIterator::RowOffsetInResult(uint64_t distinct_tile_index, uint32_t row_offset) {
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

}
}
}