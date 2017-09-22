//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_parameters.h
//
// Identification: src/include/codegen/query_parameters.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/parameter.h"
#include "planner/abstract_plan.h"
#include "type/value_peeker.h"

namespace peloton {

namespace type {
class Value;
}

namespace codegen {
class QueryParameters {
 public:
  QueryParameters(const planner::AbstractPlan &plan,
                  const std::vector<peloton::type::Value> &parameter_values) {
    // Extract Parameters information from the papers
    plan.ExtractParameters(parameters_, parameters_index_);

    // Set the values from the user's query parameters
    // The constant expressions should have already been applied by this time
    SetParameterExprValues(parameter_values);
  }

  int8_t GetTinyInt(int32_t index) {
    return peloton::type::ValuePeeker::PeekTinyInt(
        parameters_[index].GetValue());
  }

  int16_t GetSmallInt(int32_t index) {
    return peloton::type::ValuePeeker::PeekSmallInt(
        parameters_[index].GetValue());
  }

  int32_t GetInteger(int32_t index) {
    return peloton::type::ValuePeeker::PeekInteger(
        parameters_[index].GetValue());
  }

  int64_t GetBigInt(int32_t index) {
    return peloton::type::ValuePeeker::PeekBigInt(
        parameters_[index].GetValue());
  }

  double GetDouble(int32_t index) {
    return peloton::type::ValuePeeker::PeekDouble(
        parameters_[index].GetValue());
  }

  int32_t GetDate(int32_t index) {
    return peloton::type::ValuePeeker::PeekDate(parameters_[index].GetValue());
  }

  uint64_t GetTimestamp(int32_t index) {
    return peloton::type::ValuePeeker::PeekTimestamp(
        parameters_[index].GetValue());
  }

  const char *GetVarcharVal(int32_t index) {
    return peloton::type::ValuePeeker::PeekVarchar(
        parameters_[index].GetValue());
  }
  uint32_t GetVarcharLen(int32_t index) {
    return parameters_[index].GetValue().GetLength();
  }

  size_t GetParameterIdx(const expression::AbstractExpression *expression)
      const {
    auto param = parameters_index_.find(expression);
    PL_ASSERT(param != parameters_index_.end());
    return param->second;
  }

 private:
  // Set values from parameters into member variables
  void SetParameterExprValues(const std::vector<peloton::type::Value> &values) {
    for (uint32_t i = 0; i < parameters_.size(); i++) {
      auto &param = parameters_[i];
      // Retrieve a value for each parameter and store it
      if (param.GetType() == expression::Parameter::Type::PARAMETER) {
        auto param_value = values[param.GetParamIdx()].CastAs(
            param.GetValueType());
        parameters_[i] = expression::Parameter::CreateConstParameter(param_value);
      }
    }
  }

 private:
  // Parameter information vector and expression index to the vector
  std::vector<expression::Parameter> parameters_;
  std::unordered_map<const expression::AbstractExpression *, size_t>
      parameters_index_;
};

}  // namespace codegen
}  // namespace peloton 
