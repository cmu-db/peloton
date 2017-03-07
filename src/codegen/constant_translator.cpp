//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_translator.cpp
//
// Identification: src/codegen/constant_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/constant_translator.h"

#include "codegen/consumer_context.h"
#include "type/value_peeker.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Constructor
//===----------------------------------------------------------------------===//
ConstantTranslator::ConstantTranslator(
    const expression::ConstantValueExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(ctx), exp_(exp) {}

//===----------------------------------------------------------------------===//
// Produce the value that is the result of codegen-ing the expression
//===----------------------------------------------------------------------===//
codegen::Value ConstantTranslator::DeriveValue(ConsumerContext &context,
                                               RowBatch::Row &) const {
  auto &codegen = context.GetCodeGen();
  const type::Value &constant = exp_.GetValue();
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
    case type::Type::TypeId::VARCHAR: {
      std::string str = type::ValuePeeker::PeekVarchar(constant);
      val = codegen.ConstString(str);
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