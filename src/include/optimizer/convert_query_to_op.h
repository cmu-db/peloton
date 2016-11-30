//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_query_to_op.h
//
// Identification: src/include/optimizer/convert_query_to_op.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/column_manager.h"
#include "optimizer/query_operators.h"

#include <memory>
#include "operator_expression.h"

namespace peloton {

namespace parser {
class SQLStatement;
}

namespace optimizer {

std::shared_ptr<OperatorExpression> ConvertQueryToOpExpression(
    ColumnManager &manager, parser::SQLStatement *tree);

} /* namespace optimizer */
} /* namespace peloton */
