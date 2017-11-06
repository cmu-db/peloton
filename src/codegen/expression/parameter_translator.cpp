//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_translator.cpp
//
// Identification: src/codegen/expression/parameter_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/compilation_context.h"
#include "codegen/expression/parameter_translator.h"
#include "expression/parameter_value_expression.h"

namespace peloton {
namespace codegen {

// Constructor
ParameterTranslator::ParameterTranslator(
    const expression::ParameterValueExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {
  index_ = ctx.GetParameterIdx(&exp);
}

// Return an LLVM value for the constant: run-time value
codegen::Value ParameterTranslator::DeriveValue(
    CodeGen &codegen, UNUSED_ATTRIBUTE RowBatch::Row &row) const {
  auto parameter_storage = context_.GetParameterStorage();

  return parameter_storage.GetValue(codegen, index_);
}

}  // namespace codegen
}  // namespace peloton
