//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_value_translator.cpp
//
// Identification: src/codegen/tuple_value_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/tuple_value_translator.h"

#include "expression/tuple_value_expression.h"
#include "codegen/parameter.h"
#include "codegen/compilation_context.h"
#include "codegen/value_factory_proxy.h"
#include "codegen/value_proxy.h"

namespace peloton {
namespace codegen {

// Constructor
TupleValueTranslator::TupleValueTranslator(
    const expression::TupleValueExpression &tve_expr,
    CompilationContext &context)
    : ExpressionTranslator(tve_expr, context), ctx_(context){
  PL_ASSERT(tve_expr.GetAttributeRef() != nullptr);
}

codegen::Value TupleValueTranslator::DeriveValue(CodeGen &codegen,
                                                 RowBatch::Row &row) const {
  const auto &tve_expr = GetExpressionAs<expression::TupleValueExpression>();
  return row.DeriveValue(codegen, tve_expr.GetAttributeRef());
}

}  // namespace codegen
}  // namespace peloton