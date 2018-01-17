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
DEFINE_TYPE(IndexScanIterator, "peloton::codegen::util::IndexScanIterator",
            MEMBER(opaque));

DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, DoScan);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator,
              GetDistinctTileGroupNum);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, GetTileGroupId);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, RowOffsetInResult);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, GetResultSize);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, GetTileGroupOffset);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, UpdateTupleWithInteger);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, UpdateTupleWithBigInteger);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, UpdateTupleWithDouble);
DEFINE_METHOD(peloton::codegen::util, IndexScanIterator, UpdateTupleWithVarchar);
}
}