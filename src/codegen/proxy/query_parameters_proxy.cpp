//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_parameters_proxy.cpp
//
// Identification: src/codegen/proxy/query_parameters_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/query_parameters_proxy.h"

#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/transaction_context_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(QueryParameters, "codegen::QueryParameters", MEMBER(opaque));

DEFINE_METHOD(peloton::codegen, QueryParameters, GetBoolean);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetTinyInt);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetSmallInt);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetInteger);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetBigInt);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetDouble);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetDate);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetTimestamp);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetVarcharVal);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetVarcharLen);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetVarbinaryVal);
DEFINE_METHOD(peloton::codegen, QueryParameters, GetVarbinaryLen);
DEFINE_METHOD(peloton::codegen, QueryParameters, IsNull);

}  // namespace codegen
}  // namespace peloton
