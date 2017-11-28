//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_parameters_proxy.h
//
// Identification: src/include/codegen/proxy/query_parameters_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "codegen/query_parameters.h"

namespace peloton {
namespace codegen {

PROXY(QueryParameters) {
  /// We don't need access to internal fields, so use an opaque byte array
  DECLARE_MEMBER(0, char[sizeof(QueryParameters)], opaque);
  DECLARE_TYPE;

  /// Proxy Init() and Delete() in codegen::Deleter
  DECLARE_METHOD(GetBoolean);
  DECLARE_METHOD(GetTinyInt);
  DECLARE_METHOD(GetSmallInt);
  DECLARE_METHOD(GetInteger);
  DECLARE_METHOD(GetBigInt);
  DECLARE_METHOD(GetDouble);
  DECLARE_METHOD(GetDate);
  DECLARE_METHOD(GetTimestamp);
  DECLARE_METHOD(GetVarcharVal);
  DECLARE_METHOD(GetVarcharLen);
  DECLARE_METHOD(GetVarbinaryVal);
  DECLARE_METHOD(GetVarbinaryLen);
  DECLARE_METHOD(IsNull);
};

TYPE_BUILDER(QueryParameters, codegen::QueryParameters);

}  // namespace codegen
}  // namespace peloton
