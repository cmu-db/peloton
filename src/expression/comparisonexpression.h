
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

namespace voltdb {

class CmpEq {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_equals(r);}
};
class CmpNe {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_notEquals(r);}
};
class CmpLt {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_lessThan(r);}
};
class CmpGt {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_greaterThan(r);}
};
class CmpLte {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_lessThanOrEqual(r);}
};
class CmpGte {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_greaterThanOrEqual(r);}
};

template <typename C>
class ComparisonExpression : public AbstractExpression {
public:
    ComparisonExpression(ExpressionType type,
                                  AbstractExpression *left,
                                  AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
        this->m_left = left;
        this->m_right = right;
    };

    inline NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        VOLT_TRACE("eval %s. left %s, right %s. ret=%s",
                   typeid(this->compare).name(), typeid(*(this->m_left)).name(),
                   typeid(*(this->m_right)).name(),
                   this->compare.cmp(this->m_left->eval(tuple1, tuple2),
                                     this->m_right->eval(tuple1, tuple2)).isTrue()
                   ? "TRUE" : "FALSE");

        assert(m_left != NULL);
        assert(m_right != NULL);

        return this->compare.cmp(
            this->m_left->eval(tuple1, tuple2),
            this->m_right->eval(tuple1, tuple2));
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "ComparisonExpression\n");
    }

private:
    AbstractExpression *m_left;
    AbstractExpression *m_right;
    C compare;
};

template <typename C, typename L, typename R>
class InlinedComparisonExpression : public AbstractExpression {
public:
    InlinedComparisonExpression(ExpressionType type,
                                         AbstractExpression *left,
                                         AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
        this->m_left = left;
        this->m_leftTyped = dynamic_cast<L*>(left);
        this->m_right = right;
        this->m_rightTyped = dynamic_cast<R*>(right);

        assert (m_leftTyped != NULL);
        assert (m_rightTyped != NULL);
    };

    inline NValue eval(const TableTuple *tuple1, const TableTuple *tuple2 ) const {
        return this->compare.cmp(
            this->m_left->eval(tuple1, tuple2),
            this->m_right->eval(tuple1, tuple2));
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OptimizedInlinedComparisonExpression\n");
    }

  private:
    L *m_leftTyped;
    R *m_rightTyped;
    C compare;
};

}
#endif
