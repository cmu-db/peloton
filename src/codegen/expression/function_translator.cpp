//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_translator.cpp
//
// Identification: src/codegen/expression/function_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "codegen/expression/function_translator.h"
#include "codegen/type/sql_type.h"
#include "expression/function_expression.h"

namespace peloton {
namespace codegen {

// Constructor
FunctionTranslator::FunctionTranslator(
    const expression::FunctionExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx), context_(ctx) {}


codegen::Value FunctionTranslator::DeriveValue(
    CodeGen &codegen, RowBatch::Row &row) const {
  const auto &func_expr = GetExpressionAs<expression::FunctionExpression>();

  // get the number of arguments
  size_t child_num = func_expr.GetChildrenSize();
  std::vector<codegen::Value> args;

  // store arguments
  for (size_t i = 0; i < child_num; ++i) {
    args.push_back(row.DeriveValue(codegen, *func_expr.GetChild(i)));
  }

  return func_expr.codegen_func_ptr_(codegen, context_, args);
}

}  // namespace codegen
}  // namespace peloton
