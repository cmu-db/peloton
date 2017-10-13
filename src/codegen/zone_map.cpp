//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map.cpp
//
// Identification: src/codegen/zone_map.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/zone_map.h"

#include "catalog/schema.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/scan_callback.h"
#include "codegen/proxy/tile_group_proxy.h"
#include "codegen/proxy/zone_map_proxy.h"
#include "codegen/varlen.h"
#include "codegen/vector.h"
#include "codegen/lang/vectorized_loop.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/bigint_type.h"
#include "expression/abstract_expression.h"

namespace peloton {
namespace codegen {

  bool ZoneMap::ComparePredicateWithZoneMap(CodeGen &codegen, llvm::Value* predicate_array , size_t num_predicates, llvm::Value *zone_map) const {
    codegen.Call(ZoneMapProxy::ComparePredicate, {zone_map, predicate_array ,codegen.Const32(num_predicates)});
    return true;
  }

}  // namespace codegen
}  // namespace peloton
