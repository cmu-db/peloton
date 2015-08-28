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

#include "tuple_address_expression.h"
#include "common/debuglog.h"
#include "expressions/abstractexpression.h"

namespace voltdb {

TupleAddressExpression::TupleAddressExpression()
  : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS)
{
}
TupleAddressExpression::~TupleAddressExpression()
{
}

}

