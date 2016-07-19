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
               const std::vector<std::pair<int, std::string>>& bind_parameters)
: portal_name(portal_name),
  statement(statement),
  bind_parameters(bind_parameters) {

  LOG_INFO("Portal created : %s", portal_name.c_str());

}

Portal::~Portal() {

  statement.reset();

  LOG_INFO("Portal destroyed : %s", portal_name.c_str());

}

std::shared_ptr<Statement> Portal::GetStatement() const {
  return statement;
}


}  // namespace peloton
