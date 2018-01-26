//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// portal.h
//
// Identification: src/include/common/portal.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>
#include "type/value.h"
#include "statistics/query_metric.h"

namespace peloton {

class Statement;

class Portal {
 public:
  Portal() = delete;
  Portal(const Portal &) = delete;
  Portal &operator=(const Portal &) = delete;
  Portal(Portal &&) = delete;
  Portal &operator=(Portal &&) = delete;

  Portal(const std::string &portal_name, std::shared_ptr<Statement> statement,
         std::vector<type::Value> bind_parameters,
         std::shared_ptr<stats::QueryMetric::QueryParams> param_stat);

  ~Portal();

  std::shared_ptr<Statement> GetStatement() const;

  const std::vector<type::Value> &GetParameters() const;

  inline std::shared_ptr<stats::QueryMetric::QueryParams> GetParamStat() const {
    return param_stat_;
  }

  // Portal name
  std::string portal_name_;

  // Prepared statement
  std::shared_ptr<Statement> statement_;

  // Values bound to the statement of this portal
  std::vector<type::Value> bind_parameters_;

  // The serialized params for stats collection
  std::shared_ptr<stats::QueryMetric::QueryParams> param_stat_;
};

}  // namespace peloton
