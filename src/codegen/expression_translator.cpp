//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_translator.cpp
//
// Identification: src/codegen/expression_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression_translator.h"

#include "codegen/compilation_context.h"
#include "expression/abstract_expression.h"

namespace peloton {
namespace codegen {

ExpressionTranslator::ExpressionTranslator(
    const expression::AbstractExpression &expression, CompilationContext &ctx)
    : context_(ctx), expression_(expression) {
  for (uint32_t i = 0; i < expression_.GetChildrenSize(); i++) {
    context_.Prepare(*expression_.GetChild(i));
  }
}

CodeGen &ExpressionTranslator::GetCodeGen() const {
  return context_.GetCodeGen();
}

llvm::Value *ExpressionTranslator::GetValuesPtr() const {
    const auto ret = context_.GetValuesPtr();
    return ret;
}
}  // namespace codegen
}  // namespace peloton