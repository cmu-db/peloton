//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_proxy.cpp
//
// Identification: src/codegen/proxy/zone_map_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/zone_map_proxy.h"

#include "codegen/proxy/value_proxy.h"
#include "codegen/proxy/data_table_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(PredicateInfo, "peloton::storage::PredicateInfo", col_id,
            comparison_operator, predicate_value);
DEFINE_TYPE(ZoneMapManager, "peloton::storage::ZoneMapManager", opaque);

DEFINE_METHOD(peloton::storage, ZoneMapManager, ShouldScanTileGroup);
DEFINE_METHOD(peloton::storage, ZoneMapManager, GetInstance);

}  // namespace codegen
}  // namespace peloton