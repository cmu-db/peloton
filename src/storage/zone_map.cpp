 //===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map.cpp
//
// Identification: src/storage/zone_map.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/zone_map.h"

#include <numeric>

#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "common/logger.h"
#include "common/platform.h"
#include "type/types.h"
#include "storage/abstract_table.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include "expression/tuple_value_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/abstract_expression.h"
#include "type/value.h"
#include "type/types.h"

namespace peloton {
namespace storage {

ZoneMap::~ZoneMap() {
  //delete[] zone_map;
}

void ZoneMap::UpdateZoneMap(oid_t tile_column_itr, type::Value val)  {
	std::map<oid_t, statistics>::iterator it;
	it = stats_map.find(tile_column_itr);
	if (it!= stats_map.end()) {
		if (val.CompareLessThan(stats_map[tile_column_itr].min)) {
			stats_map[tile_column_itr].min = val;
			return;
		}
		if (val.CompareGreaterThan(stats_map[tile_column_itr].max)) {
			stats_map[tile_column_itr].max = val;
			return;
		}
	} else {
		statistics stats = {val, val};
		stats_map[tile_column_itr] = stats;
	}
}

void ZoneMap::TestCall() {
	std::cout << "This is a test call \n";
	LOG_DEBUG("Column Max : [%s]", stats_map[0].max.ToString().c_str());
}

bool ZoneMap::ComparePredicate(storage::PredicateInfo *parsed_predicates , int32_t num_predicates) {

	LOG_DEBUG("The number of predicates is [%d]", num_predicates);
	for (int32_t i = 0; i < num_predicates; i++) {
		// Extract the col_id, operator and predicate_value
		int col_id = parsed_predicates[i].col_id;
		int comparison_operator	= parsed_predicates[i].comparison_operator;
		type::Value predicate_value = parsed_predicates[i].predicate_value;
		// Get Min and Max value for this column
		type::Value max_val = GetMaxValue_(col_id);
		type::Value min_val = GetMinValue_(col_id);

		switch(comparison_operator) {
			case (int)ExpressionType::COMPARE_EQUAL:
				if(!checkEqual(col_id, predicate_value)) {
					return false;
				}
				break;
		    case (int)ExpressionType::COMPARE_LESSTHAN:
				if(!checkLessThan(col_id, predicate_value)) {
					return false;
				}
				break;
		    case (int)ExpressionType::COMPARE_LESSTHANOREQUALTO:
				if(!checkLessThanEquals(col_id, predicate_value)) {
					return false;
				}
				break;
		    case (int)ExpressionType::COMPARE_GREATERTHAN:
				LOG_DEBUG("Called");
				LOG_DEBUG("predicateVal : [%s]", predicate_value.ToString().c_str());
				LOG_DEBUG("Column Max : [%s]", max_val.ToString().c_str());
				if(!checkGreaterThan(col_id, predicate_value)) {
					return false;
				}
				break;
		    case (int)ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				if(!checkGreaterThanEquals(col_id, predicate_value)) {
					return false;
				}
				break;
		    default: {
		      throw Exception{"Invalid expression type for translation "};
		    }
		}
	}
	return true;
}



const std::string ZoneMap::GetInfo() const {
  std::ostringstream os;

  os << "ZoneMap Testing" << std::endl;
  return os.str();
}


}  // namespace storage
}  // namespace peloton