//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_translator.cpp
//
// Identification: src/codegen/expression/constant_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/constant_translator.h"

#include "expression/constant_value_expression.h"
#include "type/value_peeker.h"

namespace peloton {
namespace codegen {

// Constructor
ConstantTranslator::ConstantTranslator(
    const expression::ConstantValueExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {}

// Return an LLVM value for our constant (i.e., a compile-time constant)
codegen::Value ConstantTranslator::DeriveValue(CodeGen &codegen,
                                               RowBatch::Row &) const {
  // Pull out the constant from the expression
  const type::Value &constant =
      GetExpressionAs<expression::ConstantValueExpression>().GetValue();

  // Convert the value into an LLVM compile-time constant
  llvm::Value *val = nullptr;
  llvm::Value *len = nullptr;
  switch (constant.GetTypeId()) {
    case type::Type::TypeId::TINYINT: {
      val = codegen.Const8(type::ValuePeeker::PeekTinyInt(constant));
      break;
    }
    case type::Type::TypeId::SMALLINT: {
      val = codegen.Const16(type::ValuePeeker::PeekSmallInt(constant));
      break;
    }
    case type::Type::TypeId::INTEGER: {
      val = codegen.Const32(type::ValuePeeker::PeekInteger(constant));
      break;
    }
    case type::Type::TypeId::BIGINT: {
      val = codegen.Const64(type::ValuePeeker::PeekBigInt(constant));
      break;
    }
    case type::Type::TypeId::DECIMAL: {
      val = codegen.ConstDouble(type::ValuePeeker::PeekDouble(constant));
      break;
    }
    case type::Type::TypeId::DATE: {
      val = codegen.Const32(type::ValuePeeker::PeekDate(constant));
      break;
    }
    case type::Type::TypeId::TIMESTAMP: {
      val = codegen.Const64(type::ValuePeeker::PeekTimestamp(constant));
      break;
    }
    case type::Type::TypeId::VARCHAR: {
      std::string str = type::ValuePeeker::PeekVarchar(constant);
      // val should be a pointer type to be used in comparisions inside a PHI
      val = codegen.ConstStringPtr(str);
      len = codegen.Const32(str.length());
      break;
    }
    default: {
      throw Exception{"Unknown constant value type " +
                      TypeIdToString(constant.GetTypeId())};
    }
  }
  return codegen::Value{constant.GetTypeId(), val, len};
}

}  // namespace codegen
}  // namespace peloton
