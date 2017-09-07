#include <cmath>
#include "codegen/type/sql_type.h"
#include "codegen/function/decimal_functions.h"
#include "codegen/proxy/builtin_function_proxy.h"

namespace peloton {
namespace codegen {
namespace function {

codegen::Value DecimalFunctions::Sqrt(CodeGen &codegen, CompilationContext &ctx,
                                      const std::vector<codegen::Value> &args) {
  llvm::Function *func = DecimalFunctionsProxy::Sqrt_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::DECIMAL),
                        ret_val, nullptr);
}

double DecimalFunctions::Sqrt_(
    UNUSED_ATTRIBUTE executor::ExecutorContext *executor_context,
    double val) {
  if (val < 0) {
    return 0.0;
  }
  return sqrt(val);
}

}  // namespace function
}  // namespace expression
}  // namespace peloton
