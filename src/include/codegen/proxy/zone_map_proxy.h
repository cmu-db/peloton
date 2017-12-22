//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_proxy.h
//
// Identification: src/include/codegen/proxy/zone_map_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "storage/zone_map_manager.h"
#include "concurrency/transaction_context.h"
#include "codegen/proxy/value_proxy.h"
#include "codegen/proxy/transaction_context_proxy.h"
#include "codegen/proxy/data_table_proxy.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

PROXY(PredicateInfo) {
  DECLARE_MEMBER(0, int, col_id);
  DECLARE_MEMBER(1, int, comparison_operator);
  DECLARE_MEMBER(2, peloton::type::Value, predicate_value);
  DECLARE_TYPE;
};

PROXY(ZoneMapManager) {
  DECLARE_MEMBER(0, char[sizeof(storage::ZoneMapManager)], opaque);
  DECLARE_TYPE;
  DECLARE_METHOD(ShouldScanTileGroup);
  DECLARE_METHOD(GetInstance);
};

TYPE_BUILDER(PredicateInfo, storage::PredicateInfo);
TYPE_BUILDER(ZoneMapManager, storage::ZoneMapManager);

}  // namespace codegen
}  // namespace peloton