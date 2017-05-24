//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_value_translator.h
//
// Identification: src/include/codegen/tuple_value_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression_translator.h"
#include "type/type.h"

namespace peloton {

namespace expression {
class TupleValueExpression;
}  // namespace expression

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for expressions that access a specific attribute in a tuple
//===----------------------------------------------------------------------===//
class TupleValueTranslator : public ExpressionTranslator {
 public:
  // Constructor
  TupleValueTranslator(const expression::TupleValueExpression &tve_expr,
                       CompilationContext &context);

  // Return the attribute from the row
  codegen::Value DeriveValue(CodeGen &codegen, RowBatch::Row &row) const override;

  codegen::Value DerivePrimitiveValue(CodeGen &codegen, RowBatch::Row &row) const;
  codegen::Value DeriveTypeValue(CodeGen &codegen, RowBatch::Row &row) const;

 private:
  uint32_t offset_;
  CompilationContext &ctx_;
};

}  // namespace codegen
}  // namespace peloton