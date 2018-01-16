//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_translator.cpp
//
// Identification: src/codegen/expression/function_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/function_translator.h"

#include "codegen/type/decimal_type.h"
#include "codegen/type/sql_type.h"
#include "codegen/type/type_system.h"
#include "expression/function_expression.h"
#include "udf/udf_handler.h"

namespace peloton {
namespace codegen {

FunctionTranslator::FunctionTranslator(
    const expression::FunctionExpression &func_expr,
    CompilationContext &context)
    : ExpressionTranslator(func_expr, context) {
  if (!func_expr.IsUDF()) {
    PL_ASSERT(func_expr.GetFunc().op_id != OperatorId::Invalid);
    PL_ASSERT(func_expr.GetFunc().impl != nullptr);

    // Prepare each of the child expressions
    for (uint32_t i = 0; i < func_expr.GetChildrenSize(); i++) {
      context.Prepare(*func_expr.GetChild(i));
    }
  }
}

codegen::Value FunctionTranslator::DeriveValue(CodeGen &codegen,
                                               RowBatch::Row &row) const {
  // The function expression
  const auto &func_expr = GetExpressionAs<expression::FunctionExpression>();

  // Collect the arguments for the function
  std::vector<codegen::Value> args;
  for (uint32_t i = 0; i < func_expr.GetChildrenSize(); i++) {
    args.push_back(row.DeriveValue(codegen, *func_expr.GetChild(i)));
  }

  // The context for the function invocation
  type::TypeSystem::InvocationContext ctx{
      .on_error = OnError::Exception,
      .executor_context = context_.GetExecutorContextPtr()};

  if (!func_expr.IsUDF()) {
    // The ID of the operator we're calling
    OperatorId operator_id = func_expr.GetFunc().op_id;

    if (args.size() == 1) {
      // Lookup unary operation
      auto *unary_op =
          type::TypeSystem::GetUnaryOperator(operator_id, args[0].GetType());
      PL_ASSERT(unary_op != nullptr);

      // Invoke
      return unary_op->Eval(codegen, args[0], ctx);
    } else if (args.size() == 2) {
      // Lookup the function
      type::Type left_type = args[0].GetType(), right_type = args[1].GetType();
      auto *binary_op = type::TypeSystem::GetBinaryOperator(
          operator_id, left_type, left_type, right_type, right_type);
      PL_ASSERT(binary_op);

      // Invoke
      return binary_op->Eval(codegen, args[0].CastTo(codegen, left_type),
                             args[1].CastTo(codegen, right_type), ctx);
    } else {
      // It's an N-Ary function
      // Collect argument types for lookup
      std::vector<type::Type> arg_types;
      for (const auto &arg_val : args) {
        arg_types.push_back(arg_val.GetType());
      }

      // Lookup the function
      auto *nary_op = type::TypeSystem::GetNaryOperator(operator_id, arg_types);
      PL_ASSERT(nary_op != nullptr);

      // Invoke
      return nary_op->Eval(codegen, args, ctx);
    }
  } else {
    // It's a UDF
    std::vector<llvm::Value *> raw_args;
    for (uint32_t i = 0; i < args.size(); i++) {
      raw_args.push_back(args[i].GetValue());
    }

    std::unique_ptr<peloton::udf::UDFHandler> udf_handler(
        new peloton::udf::UDFHandler());

    // Register function prototype in current context
    auto *func_ptr = udf_handler->RegisterExternalFunction(codegen, func_expr);

    auto call_ret = codegen.CallFunc(func_ptr, raw_args);

    // TODO(PP): Should be changed to accomodate any return type
    return codegen::Value{type::Decimal::Instance(), call_ret, nullptr,
                          nullptr};
  }
}

}  // namespace codegen
}  // namespace peloton
