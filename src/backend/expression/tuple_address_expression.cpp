//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tuple_address_expression.cpp
//
// Identification: src/backend/expression/tuple_address_expression.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/expression/tuple_address_expression.h"

#include "backend/common/logger.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {
namespace expression {

TupleAddressExpression::TupleAddressExpression()
  : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS)
{
}
TupleAddressExpression::~TupleAddressExpression()
{
}

}  // End expression namespace
}  // End peloton namespace


