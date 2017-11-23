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

void IndexScanIterator::DoScan() {
  LOG_INFO("do scan in iterator");
}
}
}
}