#include "codegen/proxy/function_wrapper_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_METHOD(peloton::codegen, FunctionWrapper, TinyIntWrapper);
DEFINE_METHOD(peloton::codegen, FunctionWrapper, SmallIntWrapper);
DEFINE_METHOD(peloton::codegen, FunctionWrapper, IntegerWrapper);
DEFINE_METHOD(peloton::codegen, FunctionWrapper, BigIntWrapper);
DEFINE_METHOD(peloton::codegen, FunctionWrapper, DecimalWrapper);
DEFINE_METHOD(peloton::codegen, FunctionWrapper, DateWrapper);
DEFINE_METHOD(peloton::codegen, FunctionWrapper, TimestampWrapper);
DEFINE_METHOD(peloton::codegen, FunctionWrapper, VarcharWrapper);

}  // namespace codegen
}  // namespace peloton
