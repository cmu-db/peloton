//===----------------------------------------------------------------------===//
//
//							PelotonDB
//
// tuple_address_expression.h
//
// Identification: src/backend/expression/tuple_address_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <sstream>

#include "backend/common/value_factory.h"
#include "backend/common/serializer.h"
#include "backend/common/value_vector.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Tuple Address Expression
//===--------------------------------------------------------------------===//

class TupleAddressExpression : public AbstractExpression {
public:
  TupleAddressExpression()
      : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS) {}

  inline Value Evaluate(const AbstractTuple *tuple1,
                        __attribute__((unused)) const AbstractTuple *tuple2,
                        __attribute__((unused))
                        executor::ExecutorContext *) const {
    return ValueFactory::GetAddressValue(tuple1->GetData());
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "TupleAddressExpression\n";
  }
};

} // End expression namespace
} // End peloton namespace
