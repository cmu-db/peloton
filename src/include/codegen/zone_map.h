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
  // Get the column configuration for every column in the tile group
  bool ComparePredicateWithZoneMap(CodeGen &codegen , llvm::Value *predicate_array, size_t num_predicates, llvm::Value *zone_map) const;
  llvm::Value *GetMinValue(CodeGen &codegen, llvm::Value *zone_map, uint32_t col_num) const;
  llvm::Value *GetMaxValue(CodeGen &codegen, llvm::Value *zone_map, uint32_t col_num) const;
 private:
 	  //vector of parsed_predicates
  	std::vector<std::unique_ptr<const expression::AbstractExpression>> parsed_predicates;
 	llvm::Value *is_zone_mappable;
};

}  // namespace codegen
}  // namespace peloton