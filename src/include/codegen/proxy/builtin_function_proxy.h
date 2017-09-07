#include "codegen/function/functions.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"

namespace peloton {
namespace codegen {

PROXY(StringFunctions) {
  DECLARE_METHOD(Ascii_);
  DECLARE_METHOD(Chr_);
  DECLARE_METHOD(Substr_);
  DECLARE_METHOD(CharLength_);
  DECLARE_METHOD(Concat_);
  DECLARE_METHOD(OctetLength_);
  DECLARE_METHOD(Repeat_);
  DECLARE_METHOD(Replace_);
  DECLARE_METHOD(LTrim_);
  DECLARE_METHOD(RTrim_);
  DECLARE_METHOD(BTrim_);
};

PROXY(DateFunctions) {
  DECLARE_METHOD(Extract_);
};

PROXY(DecimalFunctions) {
  DECLARE_METHOD(Sqrt_);
};

}  // namespace codegen
}  // namespace peloton
