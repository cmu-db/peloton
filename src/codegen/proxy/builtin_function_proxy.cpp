#include "include/codegen/proxy/builtin_function_proxy.h"
#include "codegen/proxy/executor_context_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_METHOD(peloton::codegen::function, StringFunctions, Ascii_);
DEFINE_METHOD(peloton::codegen::function, StringFunctions, Chr_);
DEFINE_METHOD(peloton::codegen::function, StringFunctions, Substr_);
DEFINE_METHOD(peloton::codegen::function, StringFunctions, CharLength_);
DEFINE_METHOD(peloton::codegen::function, StringFunctions, Concat_);
DEFINE_METHOD(peloton::codegen::function, StringFunctions, OctetLength_);
DEFINE_METHOD(peloton::codegen::function, StringFunctions, Repeat_);
DEFINE_METHOD(peloton::codegen::function, StringFunctions, Replace_);
DEFINE_METHOD(peloton::codegen::function, StringFunctions, LTrim_);
DEFINE_METHOD(peloton::codegen::function, StringFunctions, RTrim_);
DEFINE_METHOD(peloton::codegen::function, StringFunctions, BTrim_);

DEFINE_METHOD(peloton::codegen::function, DateFunctions, Extract_);

DEFINE_METHOD(peloton::codegen::function, DecimalFunctions, Sqrt_);

}  // namespace codegen
}  // namespace peloton
