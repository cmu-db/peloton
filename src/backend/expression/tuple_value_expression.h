//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tuple_value_expression.h
//
// Identification: src/backend/expression/tuple_value_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"
#include "backend/storage/tuple.h"

#include <string>
#include <sstream>

namespace peloton {
namespace expression {

class TupleValueExpression : public AbstractExpression {
  public:
    TupleValueExpression(const int tableIdx, const int valueIdx)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE), tuple_idx(tableIdx), value_idx(valueIdx)
    {
        VOLT_TRACE("OptimizedTupleValueExpression %d using tupleIdx %d valueIdx %d", m_type, tableIdx, valueIdx);
    };

    virtual Value eval(const AbstractTuple *tuple1, const TableTuple *tuple2) const {
        if (tuple_idx == 0) {
            assert(tuple1);
            if ( ! tuple1 ) {
                throw Exception("TupleValueExpression::"
                                              "eval:"
                                              " Couldn't find tuple 1 (possible index scan planning error)");
            }
            return tuple1->getValue(value_idx);
        }
        else {
            assert(tuple2);
            if ( ! tuple2 ) {
                throw Exception("TupleValueExpression::"
                                              "eval:"
                                              " Couldn't find tuple 2 (possible index scan planning error)");
            }
            return tuple2->getValue(value_idx);
        }
    }

    std::string debugInfo(const std::string &spacer) const {
        std::ostringstream buffer;
        buffer << spacer << "Optimized Column Reference[" << tuple_idx << ", " << value_idx << "]\n";
        return (buffer.str());
    }

    int getColumnId() const {return this->value_idx;}

  protected:

    const int tuple_idx;           // which tuple. defaults to tuple1
    const int value_idx;           // which (offset) column of the tuple
};

}  // End expression namespace
}  // End peloton namespace
