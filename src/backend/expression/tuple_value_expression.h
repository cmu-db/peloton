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
        LOG_INFO("OptimizedTupleValueExpression %d using tupleIdx %d valueIdx %d", m_type, tableIdx, valueIdx);
    };

    virtual Value Evaluate(const AbstractTuple *tuple1,
                           const AbstractTuple *tuple2,
                           __attribute__((unused)) executor::ExecutorContext *context) const {
        if (tuple_idx == 0) {
            assert(tuple1);
            if ( ! tuple1 ) {
                throw Exception("TupleValueExpression::"
                                              "Evaluate:"
                                              " Couldn't find tuple 1 (possible index scan planning error)");
            }
            return tuple1->GetValue(value_idx);
        }
        else {
            assert(tuple2);
            if ( ! tuple2 ) {
                throw Exception("TupleValueExpression::"
                                              "Evaluate:"
                                              " Couldn't find tuple 2 (possible index scan planning error)");
            }
            return tuple2->GetValue(value_idx);
        }
    }

    std::string DebugInfo(const std::string &spacer) const {
        std::ostringstream buffer;
        buffer << spacer << "Optimized Column Reference[" << tuple_idx << ", " << value_idx << "]\n";
        return (buffer.str());
    }

    int GetColumnId() const {return this->value_idx;}

    int GetTupleIdx() const {return this->tuple_idx;}

  protected:

    const int tuple_idx;           // which tuple. defaults to tuple1
    const int value_idx;           // which (offset) column of the tuple
};

}  // End expression namespace
}  // End peloton namespace
