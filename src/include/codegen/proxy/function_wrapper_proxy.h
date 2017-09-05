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
