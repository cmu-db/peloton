//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_parameters_map.h
//
// Identification: src/include/codegen/query_parameters_map.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/parameter.h"

namespace peloton {

namespace type {
class Value;
}

namespace codegen {
class QueryParametersMap {
 public:
  QueryParametersMap() = default;

  DISALLOW_COPY(QueryParametersMap);

  QueryParametersMap(QueryParametersMap &&other)
      : map_(std::move(other.map_)),
        parameters_(std::move(other.parameters_)) {}

  QueryParametersMap &operator=(QueryParametersMap &&other) noexcept {
    map_ = std::move(other.map_);
    parameters_ = std::move(other.parameters_);
    return *this;
  }

  void Insert(expression::Parameter parameter,
              expression::AbstractExpression *expression) {
    parameters_.push_back(parameter);
    map_[expression] = parameters_.size() - 1;
  }

  uint32_t GetIndex(const expression::AbstractExpression *expression) const {
    auto param = map_.find(expression);
    PL_ASSERT(param != map_.end());
    return param->second;
  }

  const std::vector<expression::Parameter> &GetParameters() const {
    return parameters_;
  }

 private:
  // Parameter map
  std::unordered_map<const expression::AbstractExpression *, uint32_t> map_;

  // Parameter meta information
  std::vector<expression::Parameter> parameters_;
};

}  // namespace codegen
}  // namespace peloton
