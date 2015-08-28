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

#ifndef HSTORETUPLEADDRESSEXPRESSION_H
#define HSTORETUPLEADDRESSEXPRESSION_H

#include "common/common.h"
#include "common/ValueFactory.hpp"
#include "common/serializeio.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"

#include "expressions/abstractexpression.h"

#include <string>
#include <sstream>

namespace voltdb {

class TupleAddressExpression : public AbstractExpression {
  public:
    TupleAddressExpression();
    ~TupleAddressExpression();


    inline NValue eval(const TableTuple *tuple1, const TableTuple *tuple2)  const {
        return ValueFactory::getAddressValue(tuple1->address());
    }

    std::string debugInfo(const std::string &spacer) const {
        return spacer + "TupleAddressExpression\n";
    }
};

}
#endif // HSTORETUPLEADDRESSEXPRESSION_H
