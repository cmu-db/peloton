//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_value_expression.cpp
//
// Identification: /peloton/src/expression/tuple_value_expression.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include "common/abstract_tuple.h"
#include "expression/tuple_value_expression.h"
#include "util/hash_util.h"

namespace peloton {
namespace expression {
type::Value TupleValueExpression::Evaluate(
    const AbstractTuple *tuple1, const AbstractTuple *tuple2,
    UNUSED_ATTRIBUTE executor::ExecutorContext *context) const {
  if (tuple_idx_ == 0) {
    PL_ASSERT(tuple1 != nullptr);
    return (tuple1->GetValue(value_idx_));
  } else {
    PL_ASSERT(tuple2 != nullptr);
    return (tuple2->GetValue(value_idx_));
  }
}

hash_t TupleValueExpression::Hash() const {
  hash_t hash = HashUtil::Hash(&exp_type_);
  if (!table_name_.empty())
    hash = HashUtil::CombineHashes(
        hash, HashUtil::HashBytes(table_name_.c_str(), table_name_.length()));
  hash = HashUtil::CombineHashes(hash,
                                 HashUtil::Hash(&(std::get<0>(bound_obj_id_))));
  hash = HashUtil::CombineHashes(hash,
                                 HashUtil::Hash(&(std::get<1>(bound_obj_id_))));
  hash = HashUtil::CombineHashes(hash,
                                 HashUtil::Hash(&(std::get<2>(bound_obj_id_))));
  return hash;
}

}  // namespace expression
}  // namespace peloton