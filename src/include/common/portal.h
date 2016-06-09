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

#include <string>
#include <vector>
#include <memory>

namespace peloton {

class Statement;

class Portal {

 public:

  Portal() = delete;
  Portal(const Portal &) = delete;
  Portal &operator=(const Portal &) = delete;
  Portal(Portal &&) = delete;
  Portal &operator=(Portal &&) = delete;

  Portal(const std::string& portal_name,
         std::shared_ptr<Statement> statement,
         const std::vector<std::pair<int, std::string>>& bind_parameters);

  ~Portal();

  std::shared_ptr<Statement> GetStatement() const;

 private:

  // Portal name
  std::string portal_name;

  // Prepared statement
  std::shared_ptr<Statement> statement;

  // Group the parameter types and the parameters in this vector
  std::vector<std::pair<int, std::string>> bind_parameters;

};

}  // namespace peloton
