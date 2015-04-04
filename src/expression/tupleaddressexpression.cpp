
#include "tupleaddressexpression.h"

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

