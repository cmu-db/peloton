//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// portal.h
//
// Identification: src/include/common/portal.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>
#include "type/value.h"

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
         std::vector<type::Value> bind_parameters);

  ~Portal();

  std::shared_ptr<Statement> GetStatement() const;

  const std::vector<type::Value> &GetParameters() const;

  // Portal name
  std::string portal_name_;

  // Prepared statement
  std::shared_ptr<Statement> statement_;

  // Values bound to the statement of this portal
  std::vector<type::Value> bind_parameters_;

};

}  // namespace peloton
