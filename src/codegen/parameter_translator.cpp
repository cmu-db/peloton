//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_translator.cpp
//
// Identification: src/codegen/parameter_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/parameter_translator.h"

#include "expression/constant_value_expression.h"
#include "expression/parameter_value_expression.h"
#include "type/value_peeker.h"
#include "codegen/value_peeker_proxy.h"
#include "codegen/value_proxy.h"
#include "codegen/parameter.h"

namespace peloton {

namespace expression {
  class ParameterValueExpression;
}  // namespace planner

namespace codegen {

// Constructor
ParameterTranslator::ParameterTranslator(
    const expression::AbstractExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx), ctx_(ctx) {
  switch (exp.GetExpressionType()) {
    case ExpressionType::VALUE_PARAMETER: {
      int param_idx =
              GetExpressionAs<expression::ParameterValueExpression>().GetValueIdx();
      Parameter param = Parameter::GetParamValParamInstance(param_idx);
      offset_ = ctx_.StoreParam(param);
      break;
    }
    case ExpressionType::VALUE_CONSTANT: {
      const type::Value &constant =
            GetExpressionAs<expression::ConstantValueExpression>().GetValue();
      Parameter param = Parameter::GetConstValParamInstance(constant);
      offset_ = ctx_.StoreParam(param);
      break;
    }
    default: {
      throw Exception{"We don't have a translator for expression type: " +
                      ExpressionTypeToString(exp.GetExpressionType())};
    }
  }
}

// Return an LLVM value for our constant (i.e., a compile-time constant)
codegen::Value ParameterTranslator::DeriveValue(CodeGen &codegen,
                                               RowBatch::Row &) const {
  std::vector<llvm::Value *> args =
          {ctx_.GetValuesPtr(), codegen.Const64(offset_)};

  llvm::Value *value = codegen.CallFunc(
          ValueProxy::_GetValue::GetFunction(codegen),
          args);

  return codegen::Value{type::Type::INVALID, value, nullptr};
}

codegen::Value ParameterTranslator::DeriveTypeValue(CodeGen &codegen,
                                                    RowBatch::Row &row) const {
  return DeriveValue(codegen, row);
}

}  // namespace codegen
}  // namespace peloton