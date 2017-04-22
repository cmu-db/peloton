//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_translator.h
//
// Identification: src/include/codegen/expression_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/row_batch.h"
#include "codegen/value.h"
#include "expression/abstract_expression.h"
#include "codegen/compilation_context.h"

namespace peloton {
namespace codegen {

// Forward declare
class CompilationContext;

//===----------------------------------------------------------------------===//
// Base class for all expression translators. At a high level, an expression
// translator converts an expression tree into a single Value. In this sense,
// an expression is a function from a row to a single value (i.e., Row -> Value)
//===----------------------------------------------------------------------===//
class ExpressionTranslator {
 public:
  // Constructor
  ExpressionTranslator(const expression::AbstractExpression &expression,
                       CompilationContext &ctx);

  // Destructor
  virtual ~ExpressionTranslator() {}

  // Compute this expression
  virtual codegen::Value DeriveValue(CodeGen &codegen,
                                     RowBatch::Row &row) const = 0;

  template <typename T>
  const T &GetExpressionAs() const {
    return static_cast<const T &>(expression_);
  }

  type::Type::TypeId GetValueType() const { return expression_.GetValueType(); }

 protected:
  // Return the compilation context
  CompilationContext &GetCompilationContext() const { return context_; }

  // Return the code generator
  CodeGen &GetCodeGen() const;

  llvm::Value *GetValuesPtr() const;

 private:
  // The compilation state context
  CompilationContext &context_;

  // The expression
  const expression::AbstractExpression &expression_;
};

}  // namespace codegen
}  // namespace peloton