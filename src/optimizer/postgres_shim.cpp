//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_shim.cpp
//
// Identification: src/optimizer/postgres_shim.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/postgres_shim.h"
#include "optimizer/query_operators.h"
#include "optimizer/query_node_printer.h"

namespace peloton {
namespace optimizer {

namespace {}  // anonymous namespace

//===--------------------------------------------------------------------===//
// Compatibility with Postgres
//===--------------------------------------------------------------------===//
bool ShouldPelotonOptimize(std::string parse) {
  std::cout << "Just a placeholder for prase string " << parse << std::endl;
  return false;
}

std::shared_ptr<Select> PostgresQueryToPelotonQuery(std::string parse) {
  std::cout << "Just a placeholder for prase string " << parse << std::endl;
  return NULL;
}
}
}
