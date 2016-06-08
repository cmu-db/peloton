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

#include "common/logger.h"
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
  return false;
}

std::shared_ptr<Select> PostgresQueryToPelotonQuery(std::string parse) {
  return NULL;
}

}
}
