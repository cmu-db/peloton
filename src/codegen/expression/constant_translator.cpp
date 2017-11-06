//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_translator.cpp
//
// Identification: src/codegen/expression/constant_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/compilation_context.h"
#include "codegen/expression/constant_translator.h"
#include "expression/constant_value_expression.h"

namespace peloton {
namespace codegen {

// Constructor
ConstantTranslator::ConstantTranslator(
    const expression::ConstantValueExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {
  index_ = ctx.GetParameterIdx(&exp);
}

// Return an LLVM value for our constant: values passed over at run time
codegen::Value ConstantTranslator::DeriveValue(
    CodeGen &codegen, UNUSED_ATTRIBUTE RowBatch::Row &row) const {
  auto parameter_storage = context_.GetParameterStorage();

  return parameter_storage.GetValue(codegen, index_);
}

}  // namespace codegen
}  // namespace peloton
