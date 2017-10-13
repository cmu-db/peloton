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
#include "storage/zone_map.h"
#include "codegen/zone_map.h"
#include "codegen/proxy/value_proxy.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

	// PROXY(Value) {
	
	// 	DECLARE_MEMBER(0, char[sizeof(peloton::type::Value)], opaque);
	// 	DECLARE_TYPE;
	// };

	PROXY(PredicateInfo) {
	
		DECLARE_MEMBER(0, int, col_id);
		DECLARE_MEMBER(1, int, comparison_operator);
		DECLARE_MEMBER(2, peloton::type::Value, predicate_value);
	
		DECLARE_TYPE;
	};

	PROXY(ZoneMap) {
	
		DECLARE_MEMBER(0, char[sizeof(storage::ZoneMap)], opaque);
		DECLARE_TYPE;
	
		DECLARE_METHOD(GetMinValue);
		DECLARE_METHOD(GetMaxValue);
		DECLARE_METHOD(TestCall);
		DECLARE_METHOD(ComparePredicate);
	};

// TYPE_BUILDER(Value, peloton::type::Value);
TYPE_BUILDER(PredicateInfo, storage::PredicateInfo);
TYPE_BUILDER(ZoneMap, storage::ZoneMap);


}  // namespace codegen
}  // namespace peloton