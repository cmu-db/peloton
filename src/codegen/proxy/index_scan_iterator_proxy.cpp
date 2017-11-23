//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_iterator_proxy.cpp
//
// Identification: src/codegen/proxy/index_scan_iterator_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/index_scan_iterator_proxy.h"

namespace peloton {
namespace codegen {
DEFINE_TYPE(IndexScanIterator, "peloton::codegen::util::IndexScanIterator", MEMBER(opaque));

DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, DoScan);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, GetDistinctTileGroupNum);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, GetTileGroupId);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, RowOffsetInResult);
}
}