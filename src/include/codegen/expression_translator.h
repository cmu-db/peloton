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

namespace peloton {
namespace codegen {

// Forward declare
class CompilationContext;
class ConsumerContext;

//===----------------------------------------------------------------------===//
// Base class for all expression translators. At a high level, an expression
// translator converts an expression tree into code. For example, the following
// expression:
//     *
//    | |
//   +   b
//  | |
// a   2
//
// will be converted/translated into the following psuedo-code:
//
// tmp1 = a + 2
// tmp2 = tmp1 * b
//
// Expressions are not always purely arithmetic as in the example since they
// can include *any* type of expression we currently support in Peloton. The
// result of the translation (expressions always produce a single value) will be
// put into the context object (hence why it isn't marked const).
//
// Of course, it is a little more complicated. It is the obligation of the
// translator to generate LLVM IR that is functionality equivalent to the
// psuedo-code presented earlier.
//===----------------------------------------------------------------------===//
class ExpressionTranslator {
 public:
  // Constructor
  ExpressionTranslator(CompilationContext &ctx);

  // Destructor
  virtual ~ExpressionTranslator() {}

  // Compute this expression
  virtual codegen::Value DeriveValue(ConsumerContext &context,
                                     RowBatch::Row &row) const = 0;

  // Get the code generator
  CodeGen &GetCodeGen() const;

 private:
  // The compilation context state
  CompilationContext &ctx_;
};

}  // namespace codegen
}  // namespace peloton