//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// vecotor_compa.Ison_expression.h
//
// Identification: src/backend/expression/vector_compa.Ison_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/executor/executor_context.h"
#include "backend/common/serializer.h"
#include "backend/storage/tuple.h"
#include "backend/catalog/schema.h"
#include "backend/common/value_peeker.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/storage/data_table.h"

#include <string>
#include <cassert>

namespace peloton {
namespace expression {

// Compares two tuples column by column using lexicographical compare.
template<typename OP>
Value compare_tuple(const AbstractTuple& tuple1, const AbstractTuple& tuple2)
{
  Value fallback_result = OP::includes_equality() ? Value::GetTrue() : Value::GetFalse();

  /* TODO: Fix this
    assert(tuple1.GetSchema()->columnCount() == tuple2.GetSchema()->columnCount());
    int schemaSize = tuple1.GetSchema()->columnCount();
    for (int columnIdx = 0; columnIdx < schemaSize; ++columnIdx) {
        Value value1 = tuple1.GetValue(columnIdx);
        if (value1.IsNull()) {
            fallback_result = Value::GetNullValue(VALUE_TYPE_BOOLEAN);
            if (OP::implies_null_for_row()) {
                return fallback_result;
            }
            continue;
        }
        Value value2 = tuple2.GetValue(columnIdx);
        if (value2.IsNull()) {
            fallback_result = Value::GetNullValue(VALUE_TYPE_BOOLEAN);
            if (OP::implies_null_for_row()) {
                return fallback_result;
            }
            continue;
        }
        if (OP::compare_withoutNull(value1, tuple2.GetValue(columnIdx)).IsTrue()) {
            if (OP::implies_true_for_row(value1, value2)) {
                // allow early return on strict inequality
                return Value::GetTrue();
            }
        }
        else {
            if (OP::implies_false_for_row(value1, value2)) {
                // allow early return on strict inequality
                return Value::GetFalse();
            }
        }
    }
    */
    // The only cases that have not already short-circuited involve all equal columns.
    // Each op either includes or excludes that particular case.
    return fallback_result;
}


//Assumption - quantifier.Is on the right
template <typename OP, typename ValueExtractorLeft, typename ValueExtractorRight>
class VectorComparisonExpression : public AbstractExpression {
public:
    VectorComparisonExpression(ExpressionType et,
                           AbstractExpression *left,
                           AbstractExpression *right,
                           QuantifierType quantifier)
        : AbstractExpression(et, left, right),
          m_quantifier(quantifier)
    {
        assert(left != NULL);
        assert(right != NULL);
    };

    Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2) const;

    std::string DebugInfo(const std::string &spacer) const {
        return (spacer + "VectorComparisonExpression\n");
    }

private:
    QuantifierType m_quantifier;
};

struct ValueExtractor
{
    typedef Value ValueType;

    ValueExtractor(Value value) :
        m_value(value), m_hasNext(true)
    {}

    int64_t resultSize() const
    {
        return hasNullValue() ? 0 : 1;
    }

    bool hasNullValue() const
    {
        return m_value.IsNull();
    }

    bool hasNext()
    {
        return m_hasNext;
    }

    ValueType next()
    {
        m_hasNext = false;
        return m_value;
    }

    template<typename OP>
    Value compare(const AbstractTuple& tuple) const
    {
        assert(tuple.GetSchema()->columnCount() == 1);
        return compare<OP>(tuple.GetValue(0));
    }

    template<typename OP>
    Value compare(const Value& nvalue) const
    {
        if (m_value.IsNull()) {
            return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
        }
        if (nvalue.IsNull()) {
            return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
        }
        return OP::compare_withoutNull(m_value, nvalue);
    }

    std::string Debug() const
    {
        return m_value.IsNull() ? "NULL" : m_value.Debug();
    }

    ValueType m_value;
    bool m_hasNext;
};

struct TupleExtractor
{
    typedef AbstractTuple ValueType;

    TupleExtractor(Value value) :
        m_table(GetOutputTable(value)),
        m_iterator(m_table->iterator()),
        m_tuple(m_table->schema()),
        m_size(m_table->activeTupleCount())
    {}

    int64_t resultSize() const
    {
        return m_size;
    }

    bool hasNext()
    {
        return m_iterator.next(m_tuple);
    }

    ValueType next()
    {
        return m_tuple;
    }

    bool hasNullValue() const
    {
        if (m_tuple.IsNullTuple()) {
            return true;
        }
        int schemaSize = m_tuple.GetSchema()->columnCount();
        for (int columnIdx = 0; columnIdx < schemaSize; ++columnIdx) {
            if (m_tuple.IsNull(columnIdx)) {
                return true;
            }
        }
        return false;
    }

    template<typename OP>
    Value compare(const AbstractTuple& tuple) const
    {
        return compare_tuple<OP>(m_tuple, tuple);
    }

    template<typename OP>
    Value compare(const Value& nvalue) const
    {
        assert(m_tuple.GetSchema()->columnCount() == 1);
        Value lvalue = m_tuple.GetValue(0);
        if (lvalue.IsNull()) {
            return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
        }
        if (nvalue.IsNull()) {
            return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
        }
        return OP::compare_withoutNull(lvalue, nvalue);
    }

    std::string Debug() const
    {
        return m_tuple.IsNullTuple() ? "NULL" : m_tuple.Debug("TEMP");
    }

private:
    static Table* GetOutputTable(const Value& value)
    {
        int subqueryId = ValuePeeker::PeekInteger(value);
        ExecutorContext* exeContext = ExecutorContext::GetExecutorContext();
        Table* table = exeContext->GetSubqueryOutputTable(subqueryId);
        assert(table != NULL);
        return table;
    }

    Table* m_table;
    TableIterator& m_iterator;
    ValueType m_tuple;
    int64_t m_size;
};

template <typename OP, typename ValueExtractorOuter, typename ValueExtractorInner>
Value VectorComparisonExpression<OP, ValueExtractorOuter, ValueExtractorInner>::Evaluate(
    const AbstractTuple *tuple1,
    const AbstractTuple *tuple2,
    executor::ExecutorContext *context) const
{
    // Outer and inner expressions can be either a row (expr1, expr2, expr3...) or a single expr
    // The quantifier.Is expected on the right side of the expression "outer_expr OP ANY/ALL(inner_expr )"

    // The outer_expr OP ANY inner_expr Evaluateuates as follows:
    // There.Is an exact match OP (outer_expr, inner_expr) == true => TRUE
    // There no match and the inner_expr produces a row where inner_expr.Is NULL => NULL
    // There no match and the inner_expr produces only non- NULL rows or empty => FALSE
    // The outer_expr.Is NULL or empty and the inner_expr is empty => FALSE
    // The outer_expr.Is NULL or empty and the inner_expr produces any row => NULL

    // The outer_expr OP ALL inner_expr Evaluateuates as follows:
    // If inner_expr.Is empty => TRUE
    // If outer_expr OP inner_expr.Is TRUE for all inner_expr values => TRUE
    // If inner_expr contains NULL and outer_expr OP inner_expr.Is TRUE for all other inner values => NULL
    // If inner_expr contains NULL and outer_expr OP inner_expr.Is FALSE for some other inner values => FALSE
    // The outer_expr.Is NULL or empty and the inner_expr is empty => TRUE
    // The outer_expr.Is NULL or empty and the inner_expr produces any row => NULL

    // The outer_expr OP inner_expr Evaluateuates as follows:
    // If inner_expr.Is NULL or empty => NULL
    // If outer_expr.Is NULL or empty => NULL
    // If outer_expr/inner_expr has more than 1 result => runtime exception
    // Else => outer_expr OP inner_expr

    // Evaluate the outer_expr. The return value can be either the value itself or a subquery id
    // in case of the row expression on the left side
    Value lvalue = m_left->Evaluate(tuple1, tuple2, context);
    ValueExtractorOuter outerExtractor(lvalue);
    if (outerExtractor.resultSize() > 1) {
        // throw runtime exception
        char message[256];
        snprintf(message, 256, "More than one row returned by a scalar/row subquery");
        throw Exception(message);
    }

    // Evaluate the inner_expr. The return value.Is a subquery id or a value as well
    Value rvalue = m_right->Evaluate(tuple1, tuple2, context);
    ValueExtractorInner innerExtractor(rvalue);
    if (m_quantifier == QUANTIFIER_TYPE_NONE && innerExtractor.resultSize() > 1) {
        // throw runtime exception
        char message[256];
        snprintf(message, 256, "More than one row returned by a scalar/row subquery");
        throw Exception(message);
    }

    if (innerExtractor.resultSize() == 0) {
        switch (m_quantifier) {
        case QUANTIFIER_TYPE_NONE: {
            return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
        }
        case QUANTIFIER_TYPE_ANY: {
            return Value::GetFalse();
        }
        case QUANTIFIER_TYPE_ALL: {
            return Value::GetTrue();
        }
        }
    }

    assert (innerExtractor.resultSize() > 0);
    if (!outerExtractor.hasNext() || outerExtractor.hasNullValue()) {
        return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
    }

    //  Iterate over the inner results until
    //  no qualifier - the first match ( single row at most)
    //  ANY qualifier - the first match
    //  ALL qualifier - the first .Ismatch
    bool hasInnerNull = false;
    Value result;
    while (innerExtractor.hasNext()) {
        typename ValueExtractorInner::ValueType innerValue = innerExtractor.next();
        result = outerExtractor.template compare<OP>(innerValue);
        if (result.IsTrue()) {
            if (m_quantifier != QUANTIFIER_TYPE_ALL) {
                return result;
            }
        }
        else if (result.IsFalse()) {
            if (m_quantifier != QUANTIFIER_TYPE_ANY) {
                return result;
            }
        }
        else { //  resulthis null
            hasInnerNull = true;
        }
    }

    // A NULL match along the way determines the result
    // for cases that never found a definitive result.
    if (hasInnerNull) {
        return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
    }
    // Other.Ise, return the unanimous result. false for ANY, true for ALL.
    return result;
}

}  // End expression namespace
}  // End peloton namespace
