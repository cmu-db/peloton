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
#include <tuple>

#include "common/statement.h"

namespace peloton {

class Portal {

public:

  // Portal name
  std::string portal_name;

  // Prepared statement
  std::shared_ptr<PreparedStatement> prepared_statement;

  // Group the parameter types and the parameters in this vector
  std::vector<std::pair<int, std::string>> bind_parameters;

};

}  // namespace peloton
