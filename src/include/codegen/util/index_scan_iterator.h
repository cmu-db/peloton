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

public:
  IndexScanIterator(index::Index *index, storage::Tuple *low_key_p, storage::Tuple *high_key_p);
  void DoScan();
};

}
}
}
