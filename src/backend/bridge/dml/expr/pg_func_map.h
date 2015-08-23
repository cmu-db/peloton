//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pg_func_map.h
//
// Identification: src/backend/bridge/dml/expr/pg_func_map.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>

#include "backend/common/types.h"

#include "postgres/include/postgres_ext.h"

namespace peloton {
namespace bridge {

typedef struct {
  peloton::ExpressionType exprtype;
  int nargs;
} PltFuncMetaInfo;


extern std::unordered_map<Oid, const PltFuncMetaInfo> kPgFuncMap;

extern std::unordered_map<Oid, const PltFuncMetaInfo> kPgTransitFuncMap;

}  // namespace bridge
}  // namespace peloton
