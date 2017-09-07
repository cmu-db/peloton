#pragma once

#include <vector>
#include "codegen/value.h"
#include "codegen/compilation_context.h"
#include "executor/executor_context.h"

namespace peloton {
namespace codegen {
namespace function {

class DecimalFunctions {
 public:
  static codegen::Value Sqrt(CodeGen &codegen, CompilationContext &ctx,
                              const std::vector<codegen::Value> &args);
  static double Sqrt_(executor::ExecutorContext *executor_context,
                     double val);
};

}  // namespace function
}  // namespace expression
}  // namespace peloton
