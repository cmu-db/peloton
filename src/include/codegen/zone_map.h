//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map.h
//
// Identification: src/include/codegen/zone_map.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>

#include "codegen/codegen.h"
#include "codegen/value.h"
#include "type/types.h"
#include "type/value.h"
#include "expression/abstract_expression.h"
#include "codegen/proxy/runtime_functions_proxy.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Various common functions that are called from compiled query plans
//===----------------------------------------------------------------------===//
class ZoneMap {
 public:
  // // Get the column configuration for every column in the tile group
  // llvm::Value *ComparePredicateWithZoneMap(CodeGen &codegen , const storage::PredicateInfo *arr, size_t num_predicates, llvm::Value *zone_map) const;
};

}  // namespace codegen
}  // namespace peloton