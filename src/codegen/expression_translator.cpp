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

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Constructor
//===----------------------------------------------------------------------===//
ExpressionTranslator::ExpressionTranslator(CompilationContext &ctx)
    : ctx_(ctx) {}

//===----------------------------------------------------------------------===//
// Get the code generator
//===----------------------------------------------------------------------===//
CodeGen &ExpressionTranslator::GetCodeGen() const { return ctx_.GetCodeGen(); }

}  // namespace codegen
}  // namespace peloton