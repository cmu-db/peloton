//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// comparison_expression.h
//
// Identification: src/backend/expression/comparison_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef HSTORECOMPARISONEXPRESSION_H
#define HSTORECOMPARISONEXPRESSION_H

#include "common/common.h"
#include "common/serializeio.h"
#include "common/valuevector.h"

#include "expressions/abstractexpression.h"
#include "expressions/parametervalueexpression.h"
#include "expressions/constantvalueexpression.h"
#include "expressions/tuplevalueexpression.h"

#include <string>
#include <cassert>

namespace voltdb {

// Each of these OP classes implements a standard static function interface
// for a different comparison operator assumed to apply to two non-null-valued
// NValues.
// "compare_withoutNull" delegates to an NValue method implementing the specific
// comparison and returns either a true or false boolean NValue.
// "implies_true_for_row" returns true if a prior true return from compare_withoutNull
// applied to a row's prefix column implies a true result for the row comparison.
// This may require a recheck for strict inequality.
// "implies_false_for_row" returns true if a prior false return from compare_withoutNull
// applied to a row's prefix column implies a false result for the row comparison.
// This may require a recheck for strict inequality.
// "includes_equality" returns true if the comparison is true for (rows of) equal values.

class CmpEq {
public:
    inline static const char* op_name() { return "CmpEq"; }
    inline static NValue compare_withoutNull(const NValue& l, const NValue& r)
    { return l.op_equals_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r) { return false; }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_null_for_row() { return false; }
    inline static bool includes_equality() { return true; }
};

class CmpNe {
public:
    inline static const char* op_name() { return "CmpNe"; }
    inline static NValue compare_withoutNull(const NValue& l, const NValue& r)
    { return l.op_notEquals_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r) { return false; }
    inline static bool implies_null_for_row() { return false; }
    inline static bool includes_equality() { return false; }
};

class CmpLt {
public:
    inline static const char* op_name() { return "CmpLt"; }
    inline static NValue compare_withoutNull(const NValue& l, const NValue& r)
    { return l.op_lessThan_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r)
    { return l.op_notEquals_withoutNull(r).isTrue(); }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return false; }
};

class CmpGt {
public:
    inline static const char* op_name() { return "CmpGt"; }
    inline static NValue compare_withoutNull(const NValue& l, const NValue& r)
    { return l.op_greaterThan_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r)
    { return l.op_notEquals_withoutNull(r).isTrue(); }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return false; }
};

class CmpLte {
public:
    inline static const char* op_name() { return "CmpLte"; }
    inline static NValue compare_withoutNull(const NValue& l, const NValue& r)
    { return l.op_lessThanOrEqual_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r)
    { return l.op_notEquals_withoutNull(r).isTrue(); }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return true; }
};

class CmpGte {
public:
    inline static const char* op_name() { return "CmpGte"; }
    inline static NValue compare_withoutNull(const NValue& l, const NValue& r)
    { return l.op_greaterThanOrEqual_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r)
    { return l.op_notEquals_withoutNull(r).isTrue(); }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return true; }
};

// CmpLike and CmpIn are slightly special in that they can never be
// instantiated in a row comparison context -- even "(a, b) IN (subquery)" is
// decomposed into column-wise equality comparisons "(a, b) = ANY (subquery)".
class CmpLike {
public:
    inline static const char* op_name() { return "CmpLike"; }
    inline static NValue compare_withoutNull(const NValue& l, const NValue& r) { return l.like(r);}
};

class CmpIn {
public:
    inline static const char* op_name() { return "CmpIn"; }
    inline static NValue compare_withoutNull(const NValue& l, const NValue& r)
    { return l.inList(r) ? NValue::getTrue() : NValue::getFalse(); }
};

template <typename OP>
class ComparisonExpression : public AbstractExpression {
public:
    ComparisonExpression(ExpressionType type,
                                  AbstractExpression *left,
                                  AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
        m_left = left;
        m_right = right;
    };

    inline NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const
    {
        VOLT_TRACE("eval %s. left %s, right %s. ret=%s",
                   OP::op_name(),
                   typeid(*(m_left)).name(),
                   typeid(*(m_right)).name(),
                   traceEval(tuple1, tuple2));

        assert(m_left != NULL);
        assert(m_right != NULL);

        NValue lnv = m_left->eval(tuple1, tuple2);
        if (lnv.isNull()) {
            return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
        }

        NValue rnv = m_right->eval(tuple1, tuple2);
        if (rnv.isNull()) {
            return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
        }

        // comparisons with null or NaN are always false
        // [This code is commented out because doing the right thing breaks voltdb atm.
        // We need to re-enable after we can verify that all plans in all configs give the
        // same answer.]
        /*if (lnv.isNull() || lnv.isNaN() || rnv.isNull() || rnv.isNaN()) {
            return NValue::getFalse();
        }*/

        return OP::compare_withoutNull(lnv, rnv);
    }

    inline const char* traceEval(const TableTuple *tuple1, const TableTuple *tuple2) const
    {
        NValue lnv;
        NValue rnv;
        return  (((lnv = m_left->eval(tuple1, tuple2)).isNull() ||
                  (rnv = m_right->eval(tuple1, tuple2)).isNull()) ?
                 "NULL" :
                 (OP::compare_withoutNull(lnv,
                                          rnv).isTrue() ?
                  "TRUE" :
                  "FALSE"));
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "ComparisonExpression\n");
    }

private:
    AbstractExpression *m_left;
    AbstractExpression *m_right;
};

template <typename C, typename L, typename R>
class InlinedComparisonExpression : public ComparisonExpression<C> {
public:
    InlinedComparisonExpression(ExpressionType type,
                                         AbstractExpression *left,
                                         AbstractExpression *right)
        : ComparisonExpression<C>(type, left, right)
    {}
};

}
#endif
