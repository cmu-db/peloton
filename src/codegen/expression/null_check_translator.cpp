//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// null_check_translator.cpp
//
// Identification: src/codegen/expression/null_check_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/null_check_translator.h"

#include "codegen/type/boolean_type.h"
#include "codegen/type/type_system.h"
#include "expression/operator_expression.h"

namespace peloton {
namespace codegen {

// Constructor
NullCheckTranslator::NullCheckTranslator(
    const expression::OperatorExpression &null_check, CompilationContext &ctx)
    : ExpressionTranslator(null_check, ctx) {
  PELOTON_ASSERT(null_check.GetChildrenSize() == 1);
}

Value NullCheckTranslator::DeriveValue(CodeGen &codegen,
                                       RowBatch::Row &row) const {
  const auto &null_check = GetExpressionAs<expression::OperatorExpression>();
  Value val = row.DeriveValue(codegen, *null_check.GetChild(0));
  switch (null_check.GetExpressionType()) {
    case ExpressionType::OPERATOR_IS_NULL:
      return Value{type::Boolean::Instance(), val.IsNull(codegen)};
    case ExpressionType::OPERATOR_IS_NOT_NULL:
      return Value{type::Boolean::Instance(), val.IsNotNull(codegen)};
    default: {
      throw Exception(
          "NullCheck expression has invalid ExpressionType: " +
          ExpressionTypeToString(null_check.GetExpressionType()));
    }
  }
}

}  // namespace codegen
}  // namespace peloton
