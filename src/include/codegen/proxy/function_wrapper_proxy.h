//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_wrapper_proxy.h
//
// Identification: src/include/codegen/proxy/function_wrapper_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "codegen/function_wrapper.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"

namespace peloton {
namespace codegen {

PROXY(FunctionWrapper) {
  DECLARE_METHOD(TinyIntWrapper);
  DECLARE_METHOD(SmallIntWrapper);
  DECLARE_METHOD(IntegerWrapper);
  DECLARE_METHOD(BigIntWrapper);
  DECLARE_METHOD(DecimalWrapper);
  DECLARE_METHOD(DateWrapper);
  DECLARE_METHOD(TimestampWrapper);
  DECLARE_METHOD(VarcharWrapper);
};

}  // namespace codegen
}  // namespace peloton
