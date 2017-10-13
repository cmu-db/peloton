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
		std::cout << (parsed_predicates[i].col_id) << "\n";
		// auto temp_predicate = parsed_predicates[i].get();
		// auto right_exp = (const expression::ConstantValueExpression *)(temp_predicate->GetChild(1));
		// auto left_exp = (const expression::TupleValueExpression *)(temp_predicate->GetChild(0));
		// int colId = left_exp->GetColumnId();
		// LOG_DEBUG("The column is : [%d]", colId);
		// type::Value predicateVal = right_exp->GetValue();
		// LOG_DEBUG("Called");
		// type::Value maxVal = GetMaxValue_(colId);
		// type::Value minVal = GetMinValue_(colId);
		// LOG_DEBUG("Column Max : [%s]", maxVal.ToString().c_str());
		// LOG_DEBUG("Column Min : [%s]", minVal.ToString().c_str());
		// switch(temp_predicate->GetExpressionType()) {
		// 	LOG_DEBUG("Called");
		// 	case ExpressionType::COMPARE_EQUAL:
		// 		LOG_DEBUG("Called");
		// 		if(!checkEqual(colId, predicateVal)) {
		// 			return false;
		// 		}
		// 		break;
		//     case ExpressionType::COMPARE_LESSTHAN:
		//     	LOG_DEBUG("Called");
		// 		if(!checkLessThan(colId, predicateVal)) {
		// 			return false;
		// 		}
		// 		break;
		//     case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// 		LOG_DEBUG("Called");
		// 		if(!checkLessThanEquals(colId, predicateVal)) {
		// 			return false;
		// 		}
		// 		break;
		//     case ExpressionType::COMPARE_GREATERTHAN:
		// 		LOG_DEBUG("Called");
		// 		LOG_DEBUG("predicateVal : [%s]", predicateVal.ToString().c_str());
		// 		LOG_DEBUG("Column Max : [%s]", maxVal.ToString().c_str());
		// 		if(!checkGreaterThan(colId, predicateVal)) {
		// 			return false;
		// 		}
		// 		break;
		//     case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// 		LOG_DEBUG("Called");
		// 		if(!checkGreaterThanEquals(colId, predicateVal)) {
		// 			return false;
		// 		}
		// 		break;
		//     default: {
		//       throw Exception{"Invalid expression type for translation " +
		//                       ExpressionTypeToString(temp_predicate->GetExpressionType())};
		//     }
		// }
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