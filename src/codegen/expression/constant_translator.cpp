//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_translator.cpp
//
// Identification: src/codegen/expression/constant_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/constant_translator.h"

#include "codegen/type/sql_type.h"
#include "expression/constant_value_expression.h"
#include "type/value_peeker.h"

namespace peloton {
namespace codegen {

// Constructor
ConstantTranslator::ConstantTranslator(
    const expression::ConstantValueExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {}

// Return an LLVM value for our constant (i.e., a compile-time constant)
codegen::Value ConstantTranslator::DeriveValue(
    CodeGen &codegen, UNUSED_ATTRIBUTE RowBatch::Row &row) const {
  // Pull out the constant from the expression
  const peloton::type::Value &constant =
      GetExpressionAs<expression::ConstantValueExpression>().GetValue();

  // Convert the value into an LLVM compile-time constant
  llvm::Value *val = nullptr;
  llvm::Value *len = nullptr;
  switch (constant.GetTypeId()) {
    case peloton::type::TypeId::TINYINT: {
      val = codegen.Const8(peloton::type::ValuePeeker::PeekTinyInt(constant));
      break;
    }
    case peloton::type::TypeId::SMALLINT: {
      val = codegen.Const16(peloton::type::ValuePeeker::PeekSmallInt(constant));
      break;
    }
    case peloton::type::TypeId::INTEGER: {
      val = codegen.Const32(peloton::type::ValuePeeker::PeekInteger(constant));
      break;
    }
    case peloton::type::TypeId::BIGINT: {
      val = codegen.Const64(peloton::type::ValuePeeker::PeekBigInt(constant));
      break;
    }
    case peloton::type::TypeId::DECIMAL: {
      val =
          codegen.ConstDouble(peloton::type::ValuePeeker::PeekDouble(constant));
      break;
    }
    case peloton::type::TypeId::DATE: {
      val = codegen.Const32(peloton::type::ValuePeeker::PeekDate(constant));
      break;
    }
    case peloton::type::TypeId::TIMESTAMP: {
      val =
          codegen.Const64(peloton::type::ValuePeeker::PeekTimestamp(constant));
      break;
    }
    case peloton::type::TypeId::VARCHAR: {
      std::string str = peloton::type::ValuePeeker::PeekVarchar(constant);
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
  return codegen::Value{type::SqlType::LookupType(constant.GetTypeId()), val,
                        len, nullptr};
}

}  // namespace codegen
}  // namespace peloton
