//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_iterator_proxy.h
//
// Identification: src/include/codegen/proxy/index_scan_iterator_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "codegen/util/index_scan_iterator.h"

namespace peloton {
namespace codegen {

PROXY(IndexScanIterator) {
  DECLARE_MEMBER(0, char[sizeof(util::IndexScanIterator)], opaque);
  DECLARE_TYPE;

  DECLARE_METHOD(DoScan);
  DECLARE_METHOD(GetDistinctTileGroupNum);
  DECLARE_METHOD(GetTileGroupId);
  DECLARE_METHOD(RowOffsetInResult);
  DECLARE_METHOD(GetResultSize);
  DECLARE_METHOD(GetTileGroupOffset);
};

TYPE_BUILDER(IndexScanIterator, util::IndexScanIterator);
}
}