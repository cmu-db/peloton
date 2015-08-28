//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tuple_address_expression.h
//
// Identification: src/backend/expression/tuple_address_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/value_factory.h"
#include "backend/common/serializer.h"
#include "backend/common/value_vector.h"
#include "backend/storage/tuple.h"

#include "backend/expression/abstract_expression.h"

#include <string>
#include <sstream>

namespace peloton {
namespace expression {

class TupleAddressExpression : public AbstractExpression {
  public:
    TupleAddressExpression();
    ~TupleAddressExpression();


    inline Value eval(const AbstractTuple *tuple1, const TableTuple *tuple2)  const {
        return ValueFactory::getAddressValue(tuple1->address());
    }

    std::string debugInfo(const std::string &spacer) const {
        return spacer + "TupleAddressExpression\n";
    }
};

}  // End expression namespace
}  // End peloton namespace
