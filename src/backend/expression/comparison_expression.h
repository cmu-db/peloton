//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// compa.Ison_expression.h
//
// Identification: src/backend/expression/compa.Ison_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/serializer.h"
#include "backend/common/value_vector.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/parameter_value_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"

#include <string>
#include <cassert>

namespace peloton {
namespace expression {

// Each of these OP classes implements a standard static function interface
// for a different compa.Ison operator assumed to apply to two non-null-valued
// Values.
// "compare_withoutNull" delegates to an Value method implementing the specific
// compa.Ison and returns either a true or false boolean Value.
// "implies_true_for_row" returns true if a prior true return from compare_withoutNull
// applied to a row's prefix column implies a true result for the row compa.Ison.
// T.Is may require a recheck for strict inequality.
// "implies_false_for_row" returns true if a prior false return from compare_withoutNull
// applied to a row's prefix column implies a false result for the row compa.Ison.
// T.Is may require a recheck for strict inequality.
// "includes_equality" returns true if the compa.Ison is true for (rows of) equal values.

class CmpEq {
public:
    inline static const char* op_name() { return "CmpEq"; }
    inline static Value compare_withoutNull(const Value& l, const Value& r)
    { return l.op_equals_withoutNull(r);}
    inline static bool implies_true_for_row(const Value& l, const Value& r) { return false; }
    inline static bool implies_false_for_row(const Value& l, const Value& r) { return true; }
    inline static bool implies_null_for_row() { return false; }
    inline static bool includes_equality() { return true; }
};

class CmpNe {
public:
    inline static const char* op_name() { return "CmpNe"; }
    inline static Value compare_withoutNull(const Value& l, const Value& r)
    { return l.op_notEquals_withoutNull(r);}
    inline static bool implies_true_for_row(const Value& l, const Value& r) { return true; }
    inline static bool implies_false_for_row(const Value& l, const Value& r) { return false; }
    inline static bool implies_null_for_row() { return false; }
    inline static bool includes_equality() { return false; }
};

class CmpLt {
public:
    inline static const char* op_name() { return "CmpLt"; }
    inline static Value compare_withoutNull(const Value& l, const Value& r)
    { return l.op_lessThan_withoutNull(r);}
    inline static bool implies_true_for_row(const Value& l, const Value& r) { return true; }
    inline static bool implies_false_for_row(const Value& l, const Value& r)
    { return l.op_notEquals_withoutNull(r).IsTrue(); }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return false; }
};

class CmpGt {
public:
    inline static const char* op_name() { return "CmpGt"; }
    inline static Value compare_withoutNull(const Value& l, const Value& r)
    { return l.op_greaterThan_withoutNull(r);}
    inline static bool implies_true_for_row(const Value& l, const Value& r) { return true; }
    inline static bool implies_false_for_row(const Value& l, const Value& r)
    { return l.op_notEquals_withoutNull(r).IsTrue(); }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return false; }
};

class CmpLte {
public:
    inline static const char* op_name() { return "CmpLte"; }
    inline static Value compare_withoutNull(const Value& l, const Value& r)
    { return l.op_lessThanOrEqual_withoutNull(r);}
    inline static bool implies_true_for_row(const Value& l, const Value& r)
    { return l.op_notEquals_withoutNull(r).IsTrue(); }
    inline static bool implies_false_for_row(const Value& l, const Value& r) { return true; }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return true; }
};

class CmpGte {
public:
    inline static const char* op_name() { return "CmpGte"; }
    inline static Value compare_withoutNull(const Value& l, const Value& r)
    { return l.op_greaterThanOrEqual_withoutNull(r);}
    inline static bool implies_true_for_row(const Value& l, const Value& r)
    { return l.op_notEquals_withoutNull(r).IsTrue(); }
    inline static bool implies_false_for_row(const Value& l, const Value& r) { return true; }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return true; }
};

// CmpLike and CmpIn are slightly special in that they can never be
// instantiated in a row compa.Ison context -- even "(a, b) IN (subquery)" is
// decomposed into column-.Ise equality comparisons "(a, b) = ANY (subquery)".
class CmpLike {
public:
    inline static const char* op_name() { return "CmpLike"; }
    inline static Value compare_withoutNull(const Value& l, const Value& r) { return l.like(r);}
};

class CmpIn {
public:
    inline static const char* op_name() { return "CmpIn"; }
    inline static Value compare_withoutNull(const Value& l, const Value& r)
    { return l.in.Ist(r) ? Value::GetTrue() : Value::getFalse(); }
};

template <typename OP>
class Compa.IsonExpression : public AbstractExpression {
public:
    Compa.IsonExpression(ExpressionType type,
                                  AbstractExpression *left,
                                  AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
        m_left = left;
        m_right = right;
    };

    inline Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2) const
    {
        VOLT_TRACE("Evaluate %s. left %s, right %s. ret=%s",
                   OP::op_name(),
                   typeid(*(m_left)).name(),
                   typeid(*(m_right)).name(),
                   traceEval(tuple1, tuple2));

        assert(m_left != NULL);
        assert(m_right != NULL);

        Value lnv = m_left->Evaluate(tuple1, tuple2);
        if (lnv.IsNull()) {
            return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
        }

        Value rnv = m_right->Evaluate(tuple1, tuple2);
        if (rnv.IsNull()) {
            return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
        }

        // compa.Isons with null or NaN are always false
        // [T.Is code is commented out because doing the right thing breaks voltdb atm.
        // We need to re-enable after we can verify that all plans in all configs give the
        // same answer.]
        /*if (lnv.IsNull() || lnv.isNaN() || rnv.isNull() || rnv.isNaN()) {
            return Value::GetFalse();
        }*/

        return OP::compare_withoutNull(lnv, rnv);
    }

    inline const char* traceEval(const AbstractTuple *tuple1, const AbstractTuple *tuple2) const
    {
        Value lnv;
        Value rnv;
        return  (((lnv = m_left->Evaluate(tuple1, tuple2)).IsNull() ||
                  (rnv = m_right->Evaluate(tuple1, tuple2)).IsNull()) ?
                 "NULL" :
                 (OP::compare_withoutNull(lnv,
                                          rnv).IsTrue() ?
                  "TRUE" :
                  "FALSE"));
    }

    std::string DebugInfo(const std::string &spacer) const {
        return (spacer + "Compa.IsonExpression\n");
    }

private:
    AbstractExpression *m_left;
    AbstractExpression *m_right;
};

template <typename C, typename L, typename R>
class InlinedCompa.IsonExpression : public ComparisonExpression<C> {
public:
    InlinedCompa.IsonExpression(ExpressionType type,
                                         AbstractExpression *left,
                                         AbstractExpression *right)
        : Compa.IsonExpression<C>(type, left, right)
    {}
};

}  // End expression namespace
}  // End peloton namespace
