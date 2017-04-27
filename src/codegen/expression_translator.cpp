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

llvm::Value *ExpressionTranslator::GetCharPtrParamPtr() const {
    const auto char_ptr_params = context_.GetCharPtrParamPtr();
    return char_ptr_params;
};

llvm::Value *ExpressionTranslator::GetCharLenParamPtr() const {
    const auto char_len_params = context_.GetCharLenParamPtr();
    return char_len_params;
};
}  // namespace codegen
}  // namespace peloton