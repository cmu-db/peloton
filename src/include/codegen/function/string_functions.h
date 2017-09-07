#pragma once

#include <vector>
#include "codegen/value.h"
#include "codegen/compilation_context.h"
#include "executor/executor_context.h"

namespace peloton {
namespace codegen {
namespace function {

class StringFunctions {
 public:
  // ASCII code of the first character of the argument.
  static codegen::Value Ascii(CodeGen &codegen, CompilationContext &ctx,
                              const std::vector<codegen::Value> &args);
  static int32_t Ascii_(executor::ExecutorContext *executor_context,
                        const char* str);

  // Get Character from integer
  static codegen::Value Chr(CodeGen &codegen, CompilationContext &ctx,
                            const std::vector<codegen::Value> &args);
  static char* Chr_(executor::ExecutorContext *executor_context,
                    int32_t val);

  // substring
  static codegen::Value Substr(CodeGen &codegen, CompilationContext &ctx,
                               const std::vector<codegen::Value> &args);
  static char* Substr_(executor::ExecutorContext *executor_context,
                       const char* str, int32_t from, int32_t len);

  // Number of characters in string
  static codegen::Value CharLength(CodeGen &codegen, CompilationContext &ctx,
                                   const std::vector<codegen::Value> &args);
  static int32_t CharLength_(executor::ExecutorContext *executor_context,
                             const char* str);

  // Concatenate two strings
  static codegen::Value Concat(CodeGen &codegen, CompilationContext &ctx,
                               const std::vector<codegen::Value> &args);
  static char* Concat_(executor::ExecutorContext *executor_context,
                       const char* str1, const char* str2);

  // Number of bytes in string
  static codegen::Value OctetLength(CodeGen &codegen, CompilationContext &ctx,
                                    const std::vector<codegen::Value> &args);
  static int32_t OctetLength_(executor::ExecutorContext *executor_context,
                              const char* str);

  // Repeat string the specified number of times
  static codegen::Value Repeat(CodeGen &codegen, CompilationContext &ctx,
                               const std::vector<codegen::Value> &args);
  static char* Repeat_(executor::ExecutorContext *executor_context,
                       const char* str, int32_t num);

  // Replace all occurrences in string of substring from with substring to
  static codegen::Value Replace(CodeGen &codegen, CompilationContext &ctx,
                                const std::vector<codegen::Value> &args);
  static char* Replace_(executor::ExecutorContext *executor_context,
                        const char* str, const char* from, const char* to);

  // Remove the longest string containing only characters from characters
  // from the start of string
  static codegen::Value LTrim(CodeGen &codegen, CompilationContext &ctx,
                              const std::vector<codegen::Value> &args);
  static char* LTrim_(executor::ExecutorContext *executor_context,
                      const char* str, const char* from);

  // Remove the longest string containing only characters from characters
  // from the end of string
  static codegen::Value RTrim(CodeGen &codegen, CompilationContext &ctx,
                              const std::vector<codegen::Value> &args);
  static char* RTrim_(executor::ExecutorContext *executor_context,
                      const char* str, const char* from);

  // Remove the longest string consisting only of characters in characters
  // from the start and end of string
  static codegen::Value BTrim(CodeGen &codegen, CompilationContext &ctx,
                              const std::vector<codegen::Value> &args);
  static char* BTrim_(executor::ExecutorContext *executor_context,
                      const char* str, const char* from);
};

}  // namespace function
}  // namespace expression
}  // namespace peloton
