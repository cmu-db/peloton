//=----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_proxy.cpp
//
// Identification: src/codegen/proxy/zone_map_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/zone_map_proxy.h"
#include "codegen/zone_map.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(PredicateInfo, "peloton::storage::PredicateInfo", MEMBER(col_id), MEMBER(comparison_operator), MEMBER(predicate_value));
DEFINE_TYPE(ZoneMap, "peloton::storage::ZoneMap", MEMBER(opaque));
DEFINE_TYPE(ZoneMapManager, "peloton::storage::ZoneMapManager", MEMBER(opaque));

DEFINE_METHOD(peloton::storage, ZoneMapManager, ComparePredicateAgainstZoneMap);

}  // namespace codegen
}  // namespace peloton