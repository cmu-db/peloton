#pragma once

#include "codegen/function/string_functions.h"
#include "codegen/function/date_functions.h"
#include "codegen/function/decimal_functions.h"

#include <unordered_map>

namespace peloton {
namespace codegen {
namespace function {

typedef codegen::Value (*BuiltInFuncType)(CodeGen &codegen,
                                          CompilationContext &ctx,
                                          const std::vector<codegen::Value> &);

class BuiltInFunctions {
  static std::unordered_map<std::string, BuiltInFuncType> func_map;

 public:

  static void AddFunction(const std::string &func_name, BuiltInFuncType func);

  static BuiltInFuncType GetFuncByName(const std::string &func_name);

  static char* ReturnString(executor::ExecutorContext *executor_context,
                            const std::string& str) {
    auto pool = executor_context->GetPool();
    char* ret = (char*)pool->Allocate(str.length() + 1);
    strcpy(ret, str.c_str());
    return ret;
  }
  static llvm::Value* CallFunction(CodeGen &codegen, CompilationContext &ctx,
                                   llvm::Function *func,
                                   const std::vector<codegen::Value> &args) {
    std::vector<llvm::Value*> args_;
    args_.push_back(ctx.GetExecutorContextPtr());
    for (auto &arg : args) {
      args_.push_back(arg.GetValue());
    }
    return codegen.CallFunc(func, args_);
  }
};

}  // namespace function
}  // namespace codegen
}  // namespace peloton
