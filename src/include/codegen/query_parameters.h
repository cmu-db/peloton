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

#include "codegen/query.h"
#include "type/value_peeker.h"

namespace peloton {

namespace planner {
class Parameter;
}

namespace type {
class Value;
}

namespace codegen {
class QueryParameters {
 public:
  QueryParameters(std::vector<planner::Parameter> &parameters,
                  std::unordered_map<const expression::AbstractExpression *,
                                     size_t> &index, 
                  const std::vector<peloton::type::Value> &parameter_values)
    : parameters_(parameters), parameter_index_(index) {
    // Set the values from the user's query parameters
    // The constant values should have already been applied by this time
    SetParameterValues(parameter_values);

    // Serialize Parameters into an array so that the codegen runtime accesses
    // the parameter values with indexes
    // SerializeStorage();
  }

  int8_t GetTinyInt(int32_t index) {
    auto value = parameters_[index].GetValue();
    return peloton::type::ValuePeeker::PeekTinyInt(value);
  }
  int16_t GetSmallInt(int32_t index) {
    auto value = parameters_[index].GetValue();
    return peloton::type::ValuePeeker::PeekSmallInt(value);
  }
  int32_t GetInteger(int32_t index) {
    auto value = parameters_[index].GetValue();
    return peloton::type::ValuePeeker::PeekInteger(value);
  }
  int64_t GetBigInt(int32_t index) {
    auto value = parameters_[index].GetValue();
    return peloton::type::ValuePeeker::PeekBigInt(value);
  }
  double GetDouble(int32_t index) {
    auto value = parameters_[index].GetValue();
    return peloton::type::ValuePeeker::PeekDouble(value);
  }
  int32_t GetDate(int32_t index) {
    auto value = parameters_[index].GetValue();
    return peloton::type::ValuePeeker::PeekDate(value);
  }
  uint64_t GetTimestamp(int32_t index) {
    auto value = parameters_[index].GetValue();
    return peloton::type::ValuePeeker::PeekTimestamp(value);
  }
  char *GetVarcharVal(int32_t index) {
    auto value = parameters_[index].GetValue();
    return const_cast<char *>(peloton::type::ValuePeeker::PeekVarchar(value));
  }
  uint32_t GetVarcharLen(int32_t index) {
    auto value = parameters_[index].GetValue();
    return value.GetLength();
  }

 private:
  // Set values from parameters into member variables
  void SetParameterValues(const std::vector<peloton::type::Value> &values) {
    for (uint32_t i = 0; i < parameters_.size(); i++) {
      auto &param = parameters_[i];
      // Retrieve a value for each parameter and store it
      if (param.GetType() == planner::Parameter::Type::PARAMETER) {
        auto param_value = values[param.GetParamIdx()].CastAs(
            param.GetValueType());
        parameters_[i] = planner::Parameter::CreateConstant(param_value);
      }
    }
  }

#if 0
  void SerializeStorage() {

    // Serialize the parameter values into values_
    for (uint32_t i = 0; i < parameters_.size(); i++) {
      type::Value value = parameters_[i].GetValue();
      case peloton::type::TypeId::TINYINT: {
        values_.emplace_back(std::unique_ptr<char[]>{new char[sizeof(int8_t)]});
        *reinterpret_cast<int8_t *>(values_[i].get()) =
            type::ValuePeeker::PeekTinyInt(value);
        break;
      }
      case peloton::type::TypeId::SMALLINT: {
        values_.emplace_back(
            std::unique_ptr<char[]>{new char[sizeof(int16_t)]});
        *reinterpret_cast<int16_t *>(values_[i].get()) =
            type::ValuePeeker::PeekSmallInt(value);
        break;
      }
      case peloton::type::TypeId::INTEGER: {
        values_.emplace_back(
            std::unique_ptr<char[]>{new char[sizeof(int32_t)]});
        *reinterpret_cast<int32_t *>(values_[i].get()) =
            type::ValuePeeker::PeekInteger(value);
        break;
      }
      case peloton::type::TypeId::BIGINT: {
        values_.emplace_back(
            std::unique_ptr<char[]>{new char[sizeof(int64_t)]});
        *reinterpret_cast<int64_t *>(values_[i].get()) =
            type::ValuePeeker::PeekBigInt(value);
        break;
      }
      case peloton::type::TypeId::DECIMAL: {
        values_.emplace_back(std::unique_ptr<char[]>{new char[sizeof(double)]});
        *reinterpret_cast<double *>(values_[i].get()) =
            type::ValuePeeker::PeekDouble(value);
        break;
      }
      case peloton::type::TypeId::DATE: {
        values_.emplace_back(
            std::unique_ptr<char[]>{new char[sizeof(int32_t)]});
        *reinterpret_cast<int32_t *>(values_[i].get()) =
            type::ValuePeeker::PeekDate(value);
        break;
      }
      case peloton::type::TypeId::TIMESTAMP: {
        values_.emplace_back(
            std::unique_ptr<char[]>{new char[sizeof(uint64_t)]});
        *reinterpret_cast<uint64_t *>(values_[i].get()) =
            type::ValuePeeker::PeekTimestamp(value);
        break;
      }
      case peloton::type::TypeId::VARCHAR: {
        const char *str = type::ValuePeeker::PeekVarchar(value);
        values_.emplace_back(
            std::unique_ptr<char[]>{new char[value.GetLength()]});
        PL_MEMCPY(values_[i].get(), str, value.GetLength());

        lengths_[i] = value.GetLength();
        break;
      }
      case peloton::type::TypeId::VARBINARY: {
        const char *bin = type::ValuePeeker::PeekVarbinary(value);
        values_.emplace_back(
            std::unique_ptr<char[]>{new char[value.GetLength()]});
        PL_MEMCPY(values_[i].get(), bin, value.GetLength());

        lengths_[i] = value.GetLength();
        break;
      }
    }
  }
#endif

 private:
  // Parameter information
  std::vector<planner::Parameter> parameters_;
  std::unordered_map<const expression::AbstractExpression *, size_t>
    parameter_index_;

  // Query Parameter value storage
  std::vector<std::unique_ptr<char[]>> values_;
  std::vector<int32_t> lengths_;
};

}  // namespace codegen
}  // namespace peloton 
