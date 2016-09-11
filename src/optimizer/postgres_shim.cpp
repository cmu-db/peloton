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
#include "common/logger.h"
#include "optimizer/query_operators.h"

namespace peloton {
namespace optimizer {

namespace {}  // anonymous namespace

//===--------------------------------------------------------------------===//
// Compatibility with Postgres
//===--------------------------------------------------------------------===//
bool ShouldPelotonOptimize(std::string) { return false; }

std::shared_ptr<Select> PostgresQueryToPelotonQuery(std::string) {
  return NULL;
}
}
}
