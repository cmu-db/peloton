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
               std::vector<type::Value> bind_parameters)
    : portal_name_(portal_name),
      statement_(statement),
      bind_parameters_(std::move(bind_parameters)) {}

Portal::~Portal() { statement_.reset(); }

std::shared_ptr<Statement> Portal::GetStatement() const { return statement_; }

const std::vector<type::Value>& Portal::GetParameters() const {
  return bind_parameters_;
}

}  // namespace peloton
