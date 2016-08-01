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

namespace peloton {
namespace index {

void ConstructIntervals(oid_t leading_column_id,
                        const std::vector<Value> &values,
                        const std::vector<oid_t> &key_column_ids,
                        const std::vector<ExpressionType> &expr_types,
                        std::vector<std::pair<Value, Value>> &intervals);

void FindMaxMinInColumns(oid_t leading_column_id,
                         const std::vector<Value> &values,
                         const std::vector<oid_t> &key_column_ids,
                         const std::vector<ExpressionType> &expr_types,
                         std::map<oid_t, std::pair<Value, Value>> &non_leading_columns);

}  // End index namespace
}  // End peloton namespace
