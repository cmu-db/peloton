//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_address_expression.h
//
// Identification: src/expression/tuple_address_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/value_factory.h"
#include "common/serializer.h"
#include "storage/tuple.h"

#include "expression/abstract_expression.h"

#include <string>
#include <sstream>

namespace peloton {
namespace expression {

class TupleAddressExpression : public AbstractExpression {
 public:
  TupleAddressExpression()
      : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS,
                           VALUE_TYPE_ADDRESS) {}

  inline Value Evaluate(const AbstractTuple *tuple1,
                        UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
                        UNUSED_ATTRIBUTE
                        executor::ExecutorContext *context) const {
    return ValueFactory::GetAddressValue(tuple1->GetData());
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "TupleAddressExpression\n";
  }

  AbstractExpression *Copy() const { return new TupleAddressExpression(); }
};

}  // End expression namespace
}  // End peloton namespace
