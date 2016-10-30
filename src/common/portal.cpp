//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// portal.cpp
//
// Identification: src/common/portal.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/portal.h"
#include "common/logger.h"
#include "common/statement.h"

namespace peloton {

Portal::Portal(const std::string& portal_name,
               std::shared_ptr<Statement> statement,
               std::vector<common::Value> bind_parameters,
               std::shared_ptr<stats::QueryMetric::QueryParams> param_stat)
    : portal_name(portal_name),
      statement(statement),
      bind_parameters(std::move(bind_parameters)),
      param_stat_(param_stat) {}

Portal::~Portal() { statement.reset(); }

std::shared_ptr<Statement> Portal::GetStatement() const { return statement; }

std::shared_ptr<Statement> Portal::GetStatement() const { return statement; }
}  // namespace peloton
