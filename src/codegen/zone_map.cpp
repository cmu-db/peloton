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

  llvm::Value *ZoneMap::ComparePredicateWithZoneMap(CodeGen &codegen, const storage::PredicateInfo *arr, size_t num_predicates, llvm::Value *zone_map) const {
  	llvm::Value *predicate_array = codegen->CreateIntToPtr(codegen.Const64((int64_t)arr),
                                    PredicateInfoProxy::GetType(codegen)->getPointerTo());
    codegen.CallPrintf("Call to ComparePredicate\n",{});
    llvm::Value *skip_tile_group = codegen.Call(ZoneMapProxy::ComparePredicate, {zone_map, predicate_array ,codegen.Const32(num_predicates)});
    codegen.CallPrintf("Call ended ComparePredicate\n",{});
    codegen.CallPrintf("Scan Tile: [%lu]\n", {skip_tile_group});
    return skip_tile_group;
  }

}  // namespace codegen
}  // namespace peloton
