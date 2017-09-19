//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_translator.h
//
// Identification: src/include/codegen/expression/expression_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/row_batch.h"
#include "codegen/value.h"

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

 protected:
  // The compilation state context
  CompilationContext &context_;

 private:
  // The expression
  const expression::AbstractExpression &expression_;
};

}  // namespace codegen
}  // namespace peloton
