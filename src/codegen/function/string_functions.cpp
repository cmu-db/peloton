#include "codegen/type/sql_type.h"
#include "codegen/function/functions.h"
#include "codegen/proxy/builtin_function_proxy.h"

namespace peloton {
namespace codegen {
namespace function {

// ASCII code of the first character of the argument.
codegen::Value StringFunctions::Ascii(CodeGen &codegen, CompilationContext &ctx,
                                      const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::Ascii_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::INTEGER),
                        ret_val, nullptr);
}

int32_t StringFunctions::Ascii_(UNUSED_ATTRIBUTE executor::ExecutorContext *executor_context,
                                const char* str) {
  if (str == nullptr) {
    return 0;
  }
  int32_t val = str[0];
  return val;
}

// Get Character from integer
codegen::Value StringFunctions::Chr(CodeGen &codegen, CompilationContext &ctx,
                                    const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::Chr_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  llvm::Value *ret_len = codegen.CallStrlen(ret_val);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::VARCHAR),
                        ret_val, ret_len);
}

char* StringFunctions::Chr_(executor::ExecutorContext *executor_context,
                            int32_t val) {
  auto pool = reinterpret_cast<executor::ExecutorContext*>(executor_context)->GetPool();
  char* str = (char*)pool->Allocate(2);
  str[0] = val;
  str[1] = 0;
  return str;
}

// substring
codegen::Value StringFunctions::Substr(CodeGen &codegen, CompilationContext &ctx,
                                       const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::Substr_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  llvm::Value *ret_len = codegen.CallStrlen(ret_val);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::VARCHAR),
                        ret_val, ret_len);
}

char* StringFunctions::Substr_(executor::ExecutorContext *executor_context,
                               const char* str, int32_t from, int32_t len) {
  if (str == nullptr) {
    return nullptr;
  }
  std::string str_(str);
  int32_t from_(from - 1);
  int32_t len_(len);
  std::string sub(str_.substr(from_, len_));
  return BuiltInFunctions::ReturnString(executor_context, sub);
}

// Number of characters in string
codegen::Value StringFunctions::CharLength(CodeGen &codegen, CompilationContext &ctx,
                                           const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::CharLength_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::INTEGER),
                        ret_val, nullptr);
}

int32_t StringFunctions::CharLength_(UNUSED_ATTRIBUTE executor::ExecutorContext *executor_context, const char* str) {
  if (str == nullptr) {
    return 0;
  }
  return strlen(str);
}

// Concatenate two strings
codegen::Value StringFunctions::Concat(CodeGen &codegen, CompilationContext &ctx,
                                       const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::Concat_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  llvm::Value *ret_len = codegen.CallStrlen(ret_val);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::VARCHAR),
                        ret_val, ret_len);
}

char* StringFunctions::Concat_(executor::ExecutorContext *executor_context,
                               const char* str1, const char* str2) {
  if (str1 == nullptr || str2 == nullptr) {
    return nullptr;
  }
  std::string str = std::string(str1) + std::string(str2);
  return BuiltInFunctions::ReturnString(executor_context, str);
}

// Number of bytes in string
codegen::Value StringFunctions::OctetLength(CodeGen &codegen, CompilationContext &ctx,
                                            const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::OctetLength_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::INTEGER),
                        ret_val, nullptr);
}

int32_t StringFunctions::OctetLength_(UNUSED_ATTRIBUTE executor::ExecutorContext *executor_context,
                                      const char* str) {
  return strlen(str);
}

// Repeat string the specified number of times
codegen::Value StringFunctions::Repeat(CodeGen &codegen, CompilationContext &ctx,
                                       const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::Repeat_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  llvm::Value *ret_len = codegen.CallStrlen(ret_val);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::VARCHAR),
                        ret_val, ret_len);
}

char* StringFunctions::Repeat_(executor::ExecutorContext *executor_context,
                               const char* str, int32_t num) {
  if (str == nullptr) {
    return nullptr;
  }
  std::string str_(str);
  std::string ret = "";
  while (num--) {
    ret = ret + str_;
  }
  return BuiltInFunctions::ReturnString(executor_context, ret);
}

// Replace all occurrences in string of substring from with substring to
codegen::Value StringFunctions::Replace(CodeGen &codegen, CompilationContext &ctx,
                                        const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::Replace_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  llvm::Value *ret_len = codegen.CallStrlen(ret_val);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::VARCHAR),
                        ret_val, ret_len);
}

char* StringFunctions::Replace_(executor::ExecutorContext *executor_context,
                                const char* str, const char* from, const char* to) {
  if (str == nullptr || from == nullptr || to == nullptr) {
    return nullptr;
  }
  std::string str_(str);
  std::string from_(from);
  std::string to_(to);
  size_t pos = 0;
  while((pos = str_.find(from_, pos)) != std::string::npos) {
    str_.replace(pos, from_.length(), to_);
    pos += to_.length();
  }
  return BuiltInFunctions::ReturnString(executor_context, str_);
}

// Remove the longest string containing only characters from characters
// from the start of string
codegen::Value StringFunctions::LTrim(CodeGen &codegen, CompilationContext &ctx,
                                      const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::LTrim_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  llvm::Value *ret_len = codegen.CallStrlen(ret_val);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::VARCHAR),
                        ret_val, ret_len);
}

char* StringFunctions::LTrim_(executor::ExecutorContext *executor_context,
                              const char* str, const char* from) {
  if (str == nullptr || from == nullptr) {
    return nullptr;
  }
  std::string str_(str);
  std::string from_(from);
  size_t pos = 0;
  bool erase = 0;
  while (from_.find(str_[pos]) != std::string::npos) {
    erase = 1;
    pos++;
  }
  if (erase)
    str_.erase(0, pos);
  return BuiltInFunctions::ReturnString(executor_context, str_);
}

// Remove the longest string containing only characters from characters
// from the end of string
codegen::Value StringFunctions::RTrim(CodeGen &codegen, CompilationContext &ctx,
                                      const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::RTrim_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  llvm::Value *ret_len = codegen.CallStrlen(ret_val);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::VARCHAR),
                        ret_val, ret_len);
}

char* StringFunctions::RTrim_(executor::ExecutorContext *executor_context,
                              const char* str, const char* from) {
  if (str == nullptr || from == nullptr) {
    return nullptr;
  }
  std::string str_(str);
  std::string from_(from);
  if (str_.length() == 0)
    return BuiltInFunctions::ReturnString(executor_context, "");
  size_t pos = str_.length() - 1;
  bool erase = 0;
  while (from_.find(str_[pos]) != std::string::npos) {
    erase = 1;
    pos--;
  }
  if (erase)
    str_.erase(pos + 1, str_.length() - pos - 1);
  return BuiltInFunctions::ReturnString(executor_context, str_);
}

// Remove the longest string consisting only of characters in characters
// from the start and end of string
codegen::Value StringFunctions::BTrim(CodeGen &codegen, CompilationContext &ctx,
                                      const std::vector<codegen::Value> &args) {
  llvm::Function *func = StringFunctionsProxy::BTrim_.GetFunction(codegen);
  llvm::Value *ret_val = BuiltInFunctions::CallFunction(codegen, ctx, func, args);
  llvm::Value *ret_len = codegen.CallStrlen(ret_val);
  return codegen::Value(type::SqlType::LookupType(peloton::type::TypeId::VARCHAR),
                        ret_val, ret_len);
}

char* StringFunctions::BTrim_(executor::ExecutorContext *executor_context,
                              const char* str, const char* from) {
  if (str == nullptr || from == nullptr) {
    return nullptr;
  }
  std::string str_(str);
  std::string from_(from);
  if (str_.length() == 0)
    return BuiltInFunctions::ReturnString(executor_context, "");

  size_t pos = str_.length() - 1;
  bool erase = 0;
  while (from_.find(str_[pos]) != std::string::npos) {
    erase = 1;
    pos--;
  }
  if (erase)
    str_.erase(pos + 1, str_.length() - pos - 1);

  pos = 0;
  erase = 0;
  while (from_.find(str_[pos]) != std::string::npos) {
    erase = 1;
    pos++;
  }
  if (erase)
    str_.erase(0, pos);
  return BuiltInFunctions::ReturnString(executor_context, str_);
}

}  // namespace function
}  // namespace expression
}  // namespace peloton
