//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// util.h
//
// Identification: src/include/optimizer/util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdlib>
#include <algorithm>
#include <string>

#include "expression/abstract_expression.h"

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {
class DataTable;
}

namespace optimizer {
namespace util {

inline void to_lower_string(std::string &str) {
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
}

// get the column IDs evaluated in a predicate
void GetPredicateColumns(const catalog::Schema *schema,
                                expression::AbstractExpression *expression,
                                std::vector<oid_t> &column_ids,
                                std::vector<ExpressionType> &expr_types,
                                std::vector<type::Value> &values,
                                bool &index_searchable);

bool CheckIndexSearchable(storage::DataTable *target_table,
                                 expression::AbstractExpression *expression,
                                 std::vector<oid_t> &key_column_ids,
                                 std::vector<ExpressionType> &expr_types,
                                 std::vector<type::Value> &values,
                                 oid_t &index_id);
} /* namespace util */
} /* namespace optimizer */
} /* namespace peloton */
