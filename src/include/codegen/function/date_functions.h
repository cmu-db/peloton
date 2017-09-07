#pragma once

#include <vector>
#include "codegen/value.h"
#include "codegen/compilation_context.h"
#include "executor/executor_context.h"

namespace peloton {
namespace codegen {
namespace function {

class DateFunctions {
 public:
  // The arguments are contained in the args vector
  // (1) The first argument is the part of the date to extract
  // (see DatePartType in type/types.h
  // (2) The second argument is the timestamp to extract the part from
  // @return The Value returned should be a type::DecimalValue that is
  // constructed using type::ValueFactory
  static codegen::Value Extract(CodeGen &codegen, CompilationContext &ctx,
                                const std::vector<codegen::Value> &args);
  static double Extract_(executor::ExecutorContext *executor_context,
                         int32_t date_part, uint64_t timestamp);
};

}  // namespace function
}  // namespace expression
}  // namespace peloton
