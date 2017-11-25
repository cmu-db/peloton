//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_iterator.h
//
// Identification: src/include/codegen/util/index_scan_iterator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <vector>

#include "codegen/codegen.h"
#include "index/art_index.h"


namespace peloton {
namespace codegen {
namespace util {

class IndexScanIterator {
private:
  bool is_full_scan_;
  index::Index *index_;
  storage::Tuple *low_key_p_;
  storage::Tuple *high_key_p_;
  std::vector<ItemPointer *> result_;

  oid_t distinct_tile_group_num_;
  std::vector<uint32_t> result_metadata_;

public:
  IndexScanIterator(index::Index *index, storage::Tuple *low_key_p, storage::Tuple *high_key_p);
  void DoScan();
  uint64_t GetDistinctTileGroupNum() { return (uint64_t)distinct_tile_group_num_; }
  uint64_t GetTileGroupId(uint64_t distinct_tile_index) {
    return (uint64_t)(result_[distinct_tile_index]->block);
  }
  uint32_t GetTileGroupOffset(uint64_t result_iter) {
    return (uint32_t)(result_[result_iter]->offset);
  }
  bool RowOffsetInResult(uint64_t distinct_tile_index, uint32_t row_offset);
  uint64_t GetResultSize() { return (uint64_t)result_.size(); }
};

}
}
}
