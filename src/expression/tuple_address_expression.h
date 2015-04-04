#pragma once

#include "common/value_factory.h"
#include "common/serializer.h"
#include "common/value_vector.h"
#include "storage/tuple.h"

#include "expression/abstract_expression.h"

#include <string>
#include <sstream>

namespace nstore {
namespace expression {

class TupleAddressExpression : public AbstractExpression {
 public:

  TupleAddressExpression()
 : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS) {
  }

  ~TupleAddressExpression() {
  }

  inline Value eval(const storage::Tuple *tuple1, __attribute__((unused)) const storage::Tuple *tuple2)  const {
    return ValueFactory::GetAddressValue(tuple1->GetData());
  }

  std::string debugInfo(const std::string &spacer) const {
    return spacer + "TupleAddressExpression\n";
  }
};

} // End expression namespace
} // End nstore namespace

