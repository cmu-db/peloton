//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_query_to_op.h
//
// Identification: src/optimizer/convert_query_to_op.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/op_expression.h"
#include "optimizer/column_manager.h"
#include "optimizer/query_operators.h"

#include <memory>

namespace peloton {
namespace optimizer {

std::shared_ptr<OpExpression> ConvertQueryToOpExpression(
  ColumnManager &manager,
  std::shared_ptr<Select> op);

} /* namespace optimizer */
} /* namespace peloton */
