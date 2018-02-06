//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions_proxy.cpp
//
// Identification: src/codegen/proxy/runtime_functions_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/runtime_functions_proxy.h"

#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/tile_group_proxy.h"
#include "codegen/proxy/zone_map_proxy.h"
#include "codegen/proxy/index_proxy.h"
#include "codegen/proxy/index_scan_iterator_proxy.h"
#include "codegen/proxy/tuple_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(ColumnLayoutInfo, "peloton::ColumnLayoutInfo",
            MEMBER(col_start_ptr), MEMBER(stride), MEMBER(columnar));

DEFINE_TYPE(AbstractExpression, "peloton::expression::AbstractExpression",
            MEMBER(opaque));

DEFINE_METHOD(peloton::codegen, RuntimeFunctions, HashCrc64);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, GetTileGroup);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, GetTileGroupById);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, GetTileGroupLayout);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, FillPredicateArray);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, ThrowDivideByZeroException);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, ThrowOverflowException);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, GetIterator);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, DeleteIterator);
}  // namespace codegen
}  // namespace peloton