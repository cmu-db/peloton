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
#include "type/type_id.h"
#include "type/value_peeker.h"

namespace peloton {

namespace type {
class Value;
}

namespace codegen {
class QueryParameters {
 public:
  // Constructor
  QueryParameters(planner::AbstractPlan &plan,
                  const std::vector<peloton::type::Value> &parameter_values) {
    // Extract Parameters information and set value type for all the PVE
    plan.VisitParameters(parameters_, parameters_index_, parameter_values);

    // Set the values from the user's query parameters
    SetParameterExpressionValues(parameter_values);
  }

  // Get the index of the expresson in the parameter storage
  size_t GetParameterIdx(const expression::AbstractExpression *expression)
      const {
    auto param = parameters_index_.find(expression);
    PL_ASSERT(param != parameters_index_.end());
    return param->second;
  }

  // Get the parameter value's type at the specified index
  peloton::type::TypeId GetValueType(uint32_t index) const {
    return parameters_[index].GetValue().GetTypeId();
  }

  // Get the parameter object vector
  const std::vector<expression::Parameter> &GetParameters() const {
    return parameters_;
  }

  // Get the boolean value for the index
  bool GetBoolean(uint32_t index) const {
    return peloton::type::ValuePeeker::PeekBoolean(
        parameters_[index].GetValue());
  }

  // Get the tinyint value for the index
  int8_t GetTinyInt(uint32_t index) const {
    return peloton::type::ValuePeeker::PeekTinyInt(
        parameters_[index].GetValue());
  }

  // Get the smallint value for the index
  int16_t GetSmallInt(uint32_t index) const {
    return peloton::type::ValuePeeker::PeekSmallInt(
        parameters_[index].GetValue());
  }

  // Get the integer value for the index
  int32_t GetInteger(uint32_t index) const {
    return peloton::type::ValuePeeker::PeekInteger(
        parameters_[index].GetValue());
  }

  // Get the bigint value for the index
  int64_t GetBigInt(uint32_t index) const {
    return peloton::type::ValuePeeker::PeekBigInt(
        parameters_[index].GetValue());
  }

  // Get the double value for the index
  double GetDouble(uint32_t index) const {
    return peloton::type::ValuePeeker::PeekDouble(
        parameters_[index].GetValue());
  }

  // Get the date value for the index
  int32_t GetDate(uint32_t index) const {
    return peloton::type::ValuePeeker::PeekDate(parameters_[index].GetValue());
  }

  // Get the timestamp value for the index
  uint64_t GetTimestamp(uint32_t index) const {
    return peloton::type::ValuePeeker::PeekTimestamp(
        parameters_[index].GetValue());
  }

  // Get the valrchar value for the index
  const char *GetVarcharVal(uint32_t index) const {
    return peloton::type::ValuePeeker::PeekVarchar(
        parameters_[index].GetValue());
  }

  // Get the valrchar length for the index
  uint32_t GetVarcharLen(uint32_t index) const {
    return parameters_[index].GetValue().GetLength();
  }

  // Get the valrbinary value for the index
  const char *GetVarbinaryVal(uint32_t index) const {
    return peloton::type::ValuePeeker::PeekVarbinary(
        parameters_[index].GetValue());
  }

  // Get the valrbinary length for the index
  uint32_t GetVarbinaryLen(uint32_t index) const {
    return parameters_[index].GetValue().GetLength();
  }

  // Get the nullability for the value at the index
  bool IsNull(uint32_t index) const {
    return parameters_[index].GetValue().IsNull();
  }

 private:
  // Set values from parameters into member variables
  void SetParameterExpressionValues(
      const std::vector<peloton::type::Value> &values) {
    for (size_t i = 0; i < parameters_.size(); i++) {
      auto &param = parameters_[i];
      if (param.GetType() == expression::Parameter::Type::PARAMETER) {
        auto value = values[param.GetParamIdx()];
        param = expression::Parameter::CreateConstParameter(value,
                                                            value.IsNull());
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
