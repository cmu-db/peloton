#include "codegen/expression/function_translator.h"
#include "codegen/type/sql_type.h"
#include "expression/function_expression.h"
#include "codegen/proxy/function_wrapper_proxy.h"

namespace peloton {
namespace codegen {

// Constructor
FunctionTranslator::FunctionTranslator(
    const expression::FunctionExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {}

codegen::Value FunctionTranslator::DeriveValue(
    CodeGen &codegen, RowBatch::Row &row) const {
  const auto &func_expr = GetExpressionAs<expression::FunctionExpression>();

  // get the number of arguments
  size_t child_num = func_expr.GetChildrenSize();
  std::vector<llvm::Value*> args;

  // store function pointer
  args.push_back(codegen.Const64((int64_t)func_expr.func_ptr_));
  // store argument number
  args.push_back(codegen.Const32((int32_t) child_num));

  // store arguments
  for (size_t i = 0; i < child_num; ++i) {
    args.push_back(codegen.Const32(
        static_cast<int32_t>(func_expr.func_arg_types_[i])));
    args.push_back(row.DeriveValue(codegen, *func_expr.GetChild(i))
                       .GetValue());
  }

  return CallWrapperFunction(func_expr.GetValueType(), args, codegen);
}

codegen::Value FunctionTranslator::CallWrapperFunction(
    peloton::type::TypeId ret_type,
    std::vector<llvm::Value*> &args,
    CodeGen &codegen) const {
  llvm::Function *wrapper = nullptr;
  std::vector<llvm::Type *> arg_types{codegen.Int32Type()};

  // get the wrapper function with certain return type
  switch (ret_type) {
    case peloton::type::TypeId::TINYINT:
      wrapper = FunctionWrapperProxy::TinyIntWrapper.GetFunction(codegen);
      break;
    case peloton::type::TypeId::SMALLINT:
      wrapper = FunctionWrapperProxy::SmallIntWrapper.GetFunction(codegen);
      break;
    case peloton::type::TypeId::INTEGER:
      wrapper = FunctionWrapperProxy::IntegerWrapper.GetFunction(codegen);
      break;
    case peloton::type::TypeId::BIGINT:
      wrapper = FunctionWrapperProxy::BigIntWrapper.GetFunction(codegen);
      break;
    case peloton::type::TypeId::DECIMAL:
      wrapper = FunctionWrapperProxy::DecimalWrapper.GetFunction(codegen);
      break;
    case peloton::type::TypeId::DATE:
      wrapper = FunctionWrapperProxy::DateWrapper.GetFunction(codegen);
      break;
    case peloton::type::TypeId::TIMESTAMP:
      wrapper = FunctionWrapperProxy::TimestampWrapper.GetFunction(codegen);
      break;
    case peloton::type::TypeId::VARCHAR:
      wrapper = FunctionWrapperProxy::VarcharWrapper.GetFunction(codegen);
      break;
    default:
      break;
  }
  if (wrapper != nullptr) {
    // call the function
    llvm::Value *ret_val = codegen.CallFunc(wrapper, args);
    // call strlen to get the length of a varchar
    llvm::Value *ret_len = nullptr;
    if (ret_type == peloton::type::TypeId::VARCHAR) {
      ret_len = codegen.CallStrlen(ret_val);
    }
    return codegen::Value(type::SqlType::LookupType(ret_type), ret_val, ret_len);
  }
  else {
    return codegen::Value(type::SqlType::LookupType(ret_type));
  }
}

}  // namespace codegen
}  // namespace peloton
