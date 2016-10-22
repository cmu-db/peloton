//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_util.h
//
// Identification: src/include/index/index_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <map>
#include "common/value.h"

namespace peloton {
namespace index {

class IndexMetadata;

void ConstructIntervals(oid_t leading_column_id,
                        const std::vector<common::Value> &values,
                        const std::vector<oid_t> &key_column_ids,
                        const std::vector<ExpressionType> &expr_types,
                        std::vector<std::pair<common::Value, common::Value>> &intervals);

void FindMaxMinInColumns(oid_t leading_column_id,
                         const std::vector<common::Value> &values,
                         const std::vector<oid_t> &key_column_ids,
                         const std::vector<ExpressionType> &expr_types,
                         std::map<oid_t, std::pair<common::Value, common::Value>> &non_leading_columns);
                         
bool HasNonOptimizablePredicate(const std::vector<ExpressionType> &expr_types);

bool FindValueIndex(const IndexMetadata *metadata_p,
                    const std::vector<oid_t> &tuple_column_id_list,
                    const std::vector<ExpressionType> &expr_list,
                    std::vector<std::pair<oid_t, oid_t>> &value_index_list);

}  // End index namespace
}  // End peloton namespace
